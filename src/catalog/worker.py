"""
Catalog background worker thread.

This module runs a background thread that:
1. Maintains its own database connection (separate from MCP server)
2. Runs its own asyncio event loop for async operations
3. Runs every 5 minutes to collect schema information
4. Stores schema data in a global Store for enrichment processing

This is a background thread component, separate from the MCP server's event loop.
"""

import asyncio
import logging
import threading
import time
from typing import Any

from acouchbase.bucket import AsyncBucket
from acouchbase.cluster import AsyncCluster

from catalog.schema import SchemaCollection, parse_infer_output
from catalog.store.store import (
    compute_catalog_schema_hash,
    get_all_bucket_database_info,
    get_catalog_store,
)
from utils.config import get_settings
from utils.connection import connect_to_bucket_async, connect_to_couchbase_cluster_async
from utils.constants import DEFAULT_WORKER_BUCKET_CONCURRENCY, MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.catalog")


# Catalog refresh interval (5 minutes)
CATALOG_REFRESH_INTERVAL = 300  # seconds


def _is_retryable_infer_error(error: Exception) -> bool:
    """Return True when schema probe/INFER error is transient and worth retrying."""
    error_text = str(error)
    retryable_tokens = (
        "Scan continuation failed",
        "16054",
        "Scope not found in CB datastore",
        "12021",
        "Index not ready for serving queries",
        "5000",
    )
    return any(token in error_text for token in retryable_tokens)


def _should_exclude_scope(scope_name: str) -> bool:
    """Return True when scope is internal and should not affect catalog hashing."""
    return scope_name == "_system"


def _get_worker_bucket_concurrency() -> int:
    """Read worker bucket concurrency from settings with a safe fallback."""
    raw_value = get_settings().get(
        "worker_bucket_concurrency", DEFAULT_WORKER_BUCKET_CONCURRENCY
    )
    try:
        parsed_value = int(raw_value)
    except (TypeError, ValueError):
        return DEFAULT_WORKER_BUCKET_CONCURRENCY
    return max(1, parsed_value)


async def _get_index_definitions(
    cluster: AsyncCluster, bucket_name: str, scope_name: str, collection_name: str
) -> list[dict[str, Any]]:
    """Get index definitions for a collection."""
    try:
        query = (
            "SELECT meta().id, i.name, i.index_key, i.metadata.definition "
            "FROM system:indexes as i "
            f"WHERE i.bucket_id = '{bucket_name}' "
            f"AND i.scope_id = '{scope_name}' "
            f"AND i.keyspace_id = '{collection_name}'"
        )
        result = cluster.query(query)
        indexes = []
        async for row in result:
            indexes.append(row)
        # Sort indexes by name to maintain consistent order
        indexes_sorted = sorted(
            indexes,
            key=lambda idx: idx.get("indexes", {}).get("name", idx.get("name", "")),
        )
        return indexes_sorted
    except Exception as e:
        logger.warning(
            f"Error fetching indexes for {bucket_name}.{scope_name}.{collection_name}: {e}"
        )
        return []


async def _collect_single_bucket_schema(
    cluster: AsyncCluster,
    bucket_name: str,
    existing_database_info: dict[str, Any],
) -> dict[str, Any]:
    """Collect schema and index metadata for one bucket."""
    bucket_start = time.perf_counter()
    bucket = connect_to_bucket_async(cluster, bucket_name)
    scopes_data = {}

    collection_manager = bucket.collections()
    scopes = await collection_manager.get_all_scopes()
    scopes_sorted = sorted(scopes, key=lambda s: s.name)

    for scope in scopes_sorted:
        scope_name = scope.name
        if _should_exclude_scope(scope_name):
            logger.debug(
                "Skipping internal scope from catalog state: %s.%s",
                bucket_name,
                scope_name,
            )
            continue

        collections_data = {}
        collections_sorted = sorted(scope.collections, key=lambda c: c.name)
        for collection in collections_sorted:
            collection_name = collection.name
            logger.debug(
                "Processing collection: %s.%s.%s",
                bucket_name,
                scope_name,
                collection_name,
            )

            raw_schema = await _infer_collection_schema(bucket, scope_name, collection_name)
            new_schema_collection = parse_infer_output(raw_schema)
            schema = new_schema_collection.to_dict()

            try:
                existing_buckets = existing_database_info.get("buckets", {})
                existing_bucket = existing_buckets.get(bucket_name, {})
                existing_scopes = existing_bucket.get("scopes", {})
                existing_scope = existing_scopes.get(scope_name, {})
                existing_collections = existing_scope.get("collections", {})
                existing_collection = existing_collections.get(collection_name, {})
                existing_schema_list = existing_collection.get("schema", [])
                if existing_schema_list and isinstance(existing_schema_list, list):
                    existing_schema_collection = SchemaCollection.from_dict(
                        existing_schema_list
                    )
                    existing_schema_collection.merge(new_schema_collection)
                    schema = existing_schema_collection.to_dict()
                    logger.debug(
                        "Merged schema for %s.%s.%s (%s variants)",
                        bucket_name,
                        scope_name,
                        collection_name,
                        len(existing_schema_collection),
                    )
            except Exception as merge_error:
                logger.warning(
                    "Error merging schema for %s.%s.%s: %s",
                    bucket_name,
                    scope_name,
                    collection_name,
                    merge_error,
                )

            indexes = await _get_index_definitions(
                cluster, bucket_name, scope_name, collection_name
            )
            collections_data[collection_name] = {
                "name": collection_name,
                "schema": schema,
                "indexes": indexes,
            }

        scopes_data[scope_name] = {
            "name": scope_name,
            "collections": collections_data,
        }

    elapsed = time.perf_counter() - bucket_start
    logger.debug("Collected schema for bucket=%s in %.2fs", bucket_name, elapsed)
    return {"name": bucket_name, "scopes": scopes_data}


async def _run_bucket_collection_task(
    cluster: AsyncCluster,
    bucket_name: str,
    existing_database_info: dict[str, Any],
    semaphore: asyncio.Semaphore,
) -> tuple[str, dict[str, Any] | None]:
    """Run one bucket collection task under semaphore and isolate failures."""
    async with semaphore:
        try:
            logger.debug("Processing bucket: %s", bucket_name)
            bucket_payload = await _collect_single_bucket_schema(
                cluster, bucket_name, existing_database_info
            )
            return bucket_name, bucket_payload
        except Exception as bucket_error:
            logger.warning(
                "Failed collecting schema for bucket=%s: %s",
                bucket_name,
                bucket_error,
            )
            return bucket_name, None


async def _collect_buckets_scopes_collections(
    cluster: AsyncCluster, existing_database_info: dict[str, Any]
) -> dict[str, Any]:
    """
    Collect all buckets, scopes, and collections from the cluster.
    Merges new schema data with existing schema data.

    Args:
        cluster: AsyncCluster connection
        existing_database_info: Previously collected database info to merge with

    Returns:
        Updated database_info with merged schema data
    """
    database_info = {"buckets": {}}

    try:
        bucket_manager = cluster.buckets()
        all_buckets = await bucket_manager.get_all_buckets()
        all_buckets_sorted = sorted(all_buckets, key=lambda b: b.name)
        semaphore = asyncio.Semaphore(_get_worker_bucket_concurrency())
        bucket_tasks = [
            asyncio.create_task(
                _run_bucket_collection_task(
                    cluster, bucket_info.name, existing_database_info, semaphore
                )
            )
            for bucket_info in all_buckets_sorted
        ]
        for bucket_name, bucket_payload in await asyncio.gather(*bucket_tasks):
            if not bucket_payload:
                logger.warning("Skipping failed bucket in this cycle: %s", bucket_name)
                continue
            database_info["buckets"][bucket_name] = bucket_payload
    except Exception as e:
        logger.error(f"Error collecting database schema: {e}", exc_info=True)
        return database_info

    logger.info("Collected schema for %s buckets", len(database_info["buckets"]))
    return database_info


async def _infer_collection_schema(
    bucket: AsyncBucket, scope_name: str, collection_name: str
) -> list[dict[str, Any]]:
    """Run INFER query on a collection to get its schema, only if it has documents."""
    scope = bucket.scope(name=scope_name)
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            # First check if the collection has any documents using an existence probe.
            has_docs = False
            exists_query = f"SELECT RAW 1 FROM `{collection_name}` LIMIT 1"
            exists_result = scope.query(exists_query)
            async for _ in exists_result:
                has_docs = True
                break

            # Only run INFER if there are documents.
            if not has_docs:
                logger.debug(
                    f"Skipping schema inference for {scope_name}.{collection_name} (no documents)"
                )
                return []

            query = f"INFER `{collection_name}`"
            result = scope.query(query)
            schema_list = []
            async for row in result:
                schema_list.append(row)

            # INFER returns a list, we flatten it
            return schema_list[0] if schema_list else []
        except Exception as e:
            if attempt < max_attempts and _is_retryable_infer_error(e):
                backoff_seconds = float(2 ** (attempt - 1))
                logger.warning(
                    "Transient schema inference failure for %s.%s (attempt %s/%s); "
                    "retrying in %.1fs: %s",
                    scope_name,
                    collection_name,
                    attempt,
                    max_attempts,
                    backoff_seconds,
                    e,
                )
                await asyncio.sleep(backoff_seconds)
                continue

            logger.error(f"Error inferring schema for {scope_name}.{collection_name}: {e}")
            return []

    return []


def _persist_bucket_database_info(database_info: dict[str, Any]) -> int:
    """Persist each bucket payload into its corresponding bucket store."""
    updated_buckets = 0
    buckets = database_info.get("buckets", {})
    for bucket_name, bucket_data in buckets.items():
        bucket_store = get_catalog_store(bucket_name=bucket_name)
        old_bucket_info = bucket_store.get_database_info()
        new_bucket_info = {"buckets": {bucket_name: bucket_data}}
        scopes = bucket_data.get("scopes", {})
        scope_names = sorted(scopes.keys())
        collection_names: list[str] = []
        for scope_data in scopes.values():
            collections = scope_data.get("collections", {})
            collection_names.extend(collections.keys())
        collection_names = sorted(set(collection_names))
        summary_line = (
            f"{bucket_name}: scopes={len(scope_names)}"
            f" ({', '.join(scope_names[:3]) if scope_names else 'none'}), "
            f"collections={len(collection_names)}"
            f" ({', '.join(collection_names[:5]) if collection_names else 'none'})"
        )

        old_hash = (
            compute_catalog_schema_hash(old_bucket_info) if old_bucket_info else None
        )
        new_hash = compute_catalog_schema_hash(new_bucket_info)
        if old_hash != new_hash:
            # Save the latest bucket schema when there is any structural change.
            bucket_store.add_database_info(new_bucket_info)
            updated_buckets += 1
        if bucket_store.get_bucket_summary_line() != summary_line:
            # Persist a deterministic one-line summary used by bucket routing prompts.
            bucket_store.set_bucket_summary_line(summary_line)
    return updated_buckets


async def _catalog_worker_async(stop_event: threading.Event) -> None:
    """Async worker loop that performs catalog refresh cycles."""
    logger.info("Catalog background worker async loop started")

    # Create separate database connection for this thread
    cluster = None

    try:
        # Get settings
        settings = get_settings()
        connection_string = settings.get("connection_string")
        username = settings.get("username")
        password = settings.get("password")
        ca_cert_path = settings.get("ca_cert_path")
        client_cert_path = settings.get("client_cert_path")
        client_key_path = settings.get("client_key_path")

        # Validate required settings
        if not connection_string or not username:
            logger.warning(
                "Background thread: Missing connection settings, will retry later"
            )
        else:
            # Connect to cluster
            cluster = await connect_to_couchbase_cluster_async(
                connection_string,
                username,
                password,
                ca_cert_path,
                client_cert_path,
                client_key_path,
            )
            logger.info("Catalog worker connected to Couchbase cluster")

    except Exception as e:
        logger.error(f"Catalog worker failed to connect to cluster: {e}", exc_info=True)

    # Main loop
    while not stop_event.is_set():
        try:
            if cluster:
                logger.info("Starting catalog refresh cycle")
                cycle_start = time.perf_counter()

                # Get existing aggregated database info across bucket stores
                old_database_info = get_all_bucket_database_info()

                # Collect schema information (with merging)
                database_info = await _collect_buckets_scopes_collections(
                    cluster, old_database_info
                )

                # Persist each bucket schema to its own store
                updated_buckets = _persist_bucket_database_info(database_info)

                if updated_buckets:
                    logger.info(
                        "Schema change detected, updated %s bucket store(s)",
                        updated_buckets,
                    )
                else:
                    logger.debug("No bucket schema changes detected, skipping updates")

                cycle_duration = time.perf_counter() - cycle_start
                logger.info("Catalog refresh cycle completed in %.2fs", cycle_duration)
            else:
                logger.debug("No cluster connection, skipping refresh cycle")

        except Exception as e:
            logger.error(f"Error in catalog worker loop: {e}", exc_info=True)

        # Wait for the next interval or until stop event is set (async sleep)
        await asyncio.sleep(CATALOG_REFRESH_INTERVAL)

    # Cleanup
    if cluster:
        try:
            await cluster.close()
            logger.info("Catalog worker closed cluster connection")
        except Exception as e:
            logger.error(f"Error closing cluster connection: {e}")

    logger.info("Catalog background worker async loop stopped")


def catalog_worker_loop(stop_event: threading.Event) -> None:
    """
    Main worker loop entry point that runs in the background thread.

    This function creates a new asyncio event loop for the background thread
    and runs the async worker loop.
    """
    logger.info("Catalog background worker thread started")

    # Create a new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        # Run the async worker loop
        loop.run_until_complete(_catalog_worker_async(stop_event))
    except Exception as e:
        logger.error(f"Error in catalog worker event loop: {e}", exc_info=True)
    finally:
        # Clean up the event loop
        try:
            # Cancel all pending tasks
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()

            # Wait for all tasks to complete
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )

            # Close the loop
            loop.close()
            logger.info("Catalog worker event loop closed")
        except Exception as e:
            logger.error(f"Error closing event loop: {e}", exc_info=True)

    logger.info("Catalog background worker thread stopped")
