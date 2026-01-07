"""
Catalog background worker thread.

This module runs a background thread that:
1. Maintains its own database connection (separate from MCP server)
2. Runs its own asyncio event loop for async operations
3. Runs every 5 minutes to collect schema information
4. Stores schema data in a global Store for enrichment processing
5. Uses incremental updates to only refresh changed collections

This is a background thread component, separate from the MCP server's event loop.
"""

import asyncio
import hashlib
import json
import logging
import threading
from datetime import datetime
from typing import Any

from acouchbase.bucket import AsyncBucket
from acouchbase.cluster import AsyncCluster

from catalog.events.bridge import get_enrichment_bridge
from catalog.jobs.executor import InferenceTask, ParallelInferenceExecutor
from catalog.schema import SchemaCollection, parse_infer_output
from catalog.store.store import CollectionMetadata, get_catalog_store
from utils.config import get_settings
from utils.connection import connect_to_bucket_async, connect_to_couchbase_cluster_async
from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.catalog")


# Catalog refresh interval (5 minutes)
CATALOG_REFRESH_INTERVAL = 300  # seconds

# Force refresh after this many seconds even if no changes detected (1 hour)
FORCE_REFRESH_INTERVAL = 3600  # seconds

# Minimum document count change to trigger refresh (percentage)
DOCUMENT_COUNT_CHANGE_THRESHOLD = 0.1  # 10% change

# Maximum concurrent INFER queries
PARALLEL_INFER_CONCURRENCY = 5


def _compute_schema_hash(schema_data: dict[str, Any]) -> str:
    """Compute a hash of the schema data to detect changes."""
    schema_json = json.dumps(schema_data, sort_keys=True)
    return hashlib.sha256(schema_json.encode()).hexdigest()


def _compute_collection_schema_hash(schema: list[dict[str, Any]]) -> str:
    """Compute a hash of a single collection's schema."""
    schema_json = json.dumps(schema, sort_keys=True)
    return hashlib.sha256(schema_json.encode()).hexdigest()


def _needs_refresh(
    stored_metadata: CollectionMetadata | None,
    current_doc_count: int | None,
) -> bool:
    """
    Determine if a collection needs INFER refresh based on stored metadata.

    Args:
        stored_metadata: Previously stored metadata for the collection, or None if new
        current_doc_count: Current document count, or None if unknown

    Returns:
        True if the collection should be refreshed
    """
    # New collection - always refresh
    if stored_metadata is None:
        return True

    # Parse the last infer time
    try:
        last_infer = datetime.fromisoformat(stored_metadata.last_infer_time)
        age_seconds = (datetime.utcnow() - last_infer).total_seconds()
    except (ValueError, TypeError):
        # Invalid timestamp - force refresh
        return True

    # Force refresh if too old
    if age_seconds > FORCE_REFRESH_INTERVAL:
        logger.debug(f"Collection {stored_metadata.path} forced refresh (age: {age_seconds:.0f}s)")
        return True

    # Check document count change if we have counts
    if current_doc_count is not None and stored_metadata.document_count is not None:
        if stored_metadata.document_count == 0:
            # Was empty, now has documents
            if current_doc_count > 0:
                logger.debug(f"Collection {stored_metadata.path} refresh: was empty, now has {current_doc_count} docs")
                return True
        else:
            # Calculate percentage change
            change_ratio = abs(current_doc_count - stored_metadata.document_count) / stored_metadata.document_count
            if change_ratio >= DOCUMENT_COUNT_CHANGE_THRESHOLD:
                logger.debug(
                    f"Collection {stored_metadata.path} refresh: doc count changed from "
                    f"{stored_metadata.document_count} to {current_doc_count} ({change_ratio:.1%})"
                )
                return True

    # No refresh needed
    return False


async def _get_document_count(bucket: AsyncBucket, scope_name: str, collection_name: str) -> int:
    """Get the document count for a collection."""
    try:
        scope = bucket.scope(name=scope_name)
        count_query = f"SELECT RAW COUNT(*) FROM `{collection_name}`"
        count_result = scope.query(count_query)
        async for row in count_result:
            return row
        return 0
    except Exception as e:
        logger.warning(f"Error getting document count for {scope_name}.{collection_name}: {e}")
        return 0


async def _get_index_definitions(cluster: AsyncCluster, bucket_name: str, scope_name: str, collection_name: str) -> list[dict[str, Any]]:
    """Get index definitions for a collection."""
    try:
        query = f"SELECT meta().id, i.name, i.index_key, i.metadata.definition FROM system:indexes as i"
        f"WHERE i.bucket_id = '{bucket_name}' AND i.scope_id = '{scope_name}' AND i.keyspace_id = '{collection_name}'"
        result = await cluster.query(query)
        indexes = []
        async for row in result:
            indexes.append(row)
        # Sort indexes by name to maintain consistent order
        indexes_sorted = sorted(indexes, key=lambda idx: idx.get('indexes', {}).get('name', idx.get('name', '')))
        return indexes_sorted
    except Exception as e:
        logger.warning(f"Error fetching indexes for {bucket_name}.{scope_name}.{collection_name}: {e}")
        return []


async def _collect_buckets_scopes_collections(
    cluster: AsyncCluster,
    existing_database_info: dict[str, Any],
    incremental: bool = True,
) -> tuple[dict[str, Any], int, int]:
    """
    Collect all buckets, scopes, and collections from the cluster.
    Uses incremental updates and parallel execution for efficiency.

    Args:
        cluster: AsyncCluster connection
        existing_database_info: Previously collected database info to merge with
        incremental: If True, only refresh collections that have changed

    Returns:
        Tuple of (database_info, collections_refreshed, collections_skipped)
    """
    store = get_catalog_store()
    collections_to_refresh: list[InferenceTask] = []
    collections_skipped_data: dict[str, dict[str, Any]] = {}  # path -> existing data

    # Phase 1: Discover all collections and determine which need refresh
    try:
        bucket_manager = cluster.buckets()
        all_buckets = await bucket_manager.get_all_buckets()
        all_buckets_sorted = sorted(all_buckets, key=lambda b: b.name)

        for bucket_info in all_buckets_sorted:
            bucket_name = bucket_info.name
            bucket = connect_to_bucket_async(cluster, bucket_name)

            collection_manager = bucket.collections()
            scopes = await collection_manager.get_all_scopes()
            scopes_sorted = sorted(scopes, key=lambda s: s.name)

            for scope in scopes_sorted:
                scope_name = scope.name
                collections_sorted = sorted(scope.collections, key=lambda c: c.name)

                for collection in collections_sorted:
                    collection_name = collection.name
                    collection_path = f"{bucket_name}/{scope_name}/{collection_name}"

                    # Get current document count for change detection
                    current_doc_count = await _get_document_count(bucket, scope_name, collection_name)
                    stored_metadata = store.get_collection_metadata(collection_path)

                    if incremental and not _needs_refresh(stored_metadata, current_doc_count):
                        # Use existing data
                        existing_collection = (
                            existing_database_info.get("buckets", {})
                            .get(bucket_name, {})
                            .get("scopes", {})
                            .get(scope_name, {})
                            .get("collections", {})
                            .get(collection_name, {})
                        )

                        if existing_collection:
                            collections_skipped_data[collection_path] = existing_collection
                            logger.debug(f"Skipping (no changes): {collection_path}")
                        else:
                            # No cached data - need to refresh
                            collections_to_refresh.append(
                                InferenceTask(bucket=bucket_name, scope=scope_name, collection=collection_name)
                            )
                    else:
                        # Need refresh
                        collections_to_refresh.append(
                            InferenceTask(bucket=bucket_name, scope=scope_name, collection=collection_name)
                        )

        logger.info(
            f"Discovery complete: {len(collections_to_refresh)} to refresh, "
            f"{len(collections_skipped_data)} skipped"
        )

    except Exception as e:
        logger.error(f"Error discovering collections: {e}", exc_info=True)
        return {"buckets": {}}, 0, 0

    # Phase 2: Execute parallel INFER for collections needing refresh
    inference_results = {}
    if collections_to_refresh:
        executor = ParallelInferenceExecutor(cluster, concurrency=PARALLEL_INFER_CONCURRENCY)
        results = await executor.execute_batch(collections_to_refresh)

        for result in results:
            if result.success and result.schema is not None:
                # Merge with existing schema if present
                existing_schema_list = (
                    existing_database_info.get("buckets", {})
                    .get(result.bucket, {})
                    .get("scopes", {})
                    .get(result.scope, {})
                    .get("collections", {})
                    .get(result.collection, {})
                    .get("schema", [])
                )

                if existing_schema_list and isinstance(existing_schema_list, list):
                    try:
                        existing_schema_collection = SchemaCollection.from_dict(existing_schema_list)
                        existing_schema_collection.merge(result.schema)
                        schema = existing_schema_collection.to_dict()
                    except Exception as e:
                        logger.warning(f"Error merging schema for {result.path}: {e}")
                        schema = result.schema.to_dict()
                else:
                    schema = result.schema.to_dict()

                inference_results[result.path] = {
                    "name": result.collection,
                    "schema": schema,
                    "indexes": result.indexes,
                }

                # Update collection metadata
                new_metadata = CollectionMetadata(
                    bucket=result.bucket,
                    scope=result.scope,
                    collection=result.collection,
                    schema_hash=_compute_collection_schema_hash(schema),
                    last_infer_time=datetime.utcnow().isoformat(),
                    document_count=result.document_count,
                )
                store.set_collection_metadata(new_metadata)
            else:
                logger.warning(f"Inference failed for {result.path}: {result.error}")

    # Phase 3: Build the database_info structure
    database_info: dict[str, Any] = {"buckets": {}}

    try:
        bucket_manager = cluster.buckets()
        all_buckets = await bucket_manager.get_all_buckets()
        all_buckets_sorted = sorted(all_buckets, key=lambda b: b.name)

        for bucket_info in all_buckets_sorted:
            bucket_name = bucket_info.name
            bucket = connect_to_bucket_async(cluster, bucket_name)

            collection_manager = bucket.collections()
            scopes = await collection_manager.get_all_scopes()
            scopes_sorted = sorted(scopes, key=lambda s: s.name)

            scopes_data: dict[str, Any] = {}

            for scope in scopes_sorted:
                scope_name = scope.name
                collections_sorted = sorted(scope.collections, key=lambda c: c.name)

                collections_data: dict[str, Any] = {}

                for collection in collections_sorted:
                    collection_name = collection.name
                    collection_path = f"{bucket_name}/{scope_name}/{collection_name}"

                    # Check if we have refreshed data
                    if collection_path in inference_results:
                        collections_data[collection_name] = inference_results[collection_path]
                    # Check if we have skipped data
                    elif collection_path in collections_skipped_data:
                        collections_data[collection_name] = collections_skipped_data[collection_path]

                scopes_data[scope_name] = {
                    "name": scope_name,
                    "collections": collections_data,
                }

            database_info["buckets"][bucket_name] = {
                "name": bucket_name,
                "scopes": scopes_data,
            }

    except Exception as e:
        logger.error(f"Error building database_info: {e}", exc_info=True)

    collections_refreshed = len(inference_results)
    collections_skipped = len(collections_skipped_data)

    logger.info(
        f"Collected schema for {len(database_info['buckets'])} buckets "
        f"(refreshed: {collections_refreshed}, skipped: {collections_skipped})"
    )

    return database_info, collections_refreshed, collections_skipped


async def _infer_collection_schema(bucket: AsyncBucket, scope_name: str, collection_name: str) -> list[dict[str, Any]]:
    """Run INFER query on a collection to get its schema, only if it has documents."""
    try:
        scope = bucket.scope(name=scope_name)
        
        # First check if the collection has any documents
        count_query = f"SELECT RAW COUNT(*) FROM `{collection_name}` LIMIT 1"
        count_result = scope.query(count_query)
        doc_count = 0
        async for row in count_result:
            doc_count = row
        
        # Only run INFER if there are documents
        if doc_count == 0:
            logger.debug(f"Skipping schema inference for {scope_name}.{collection_name} (no documents)")
            return []
        
        query = f"INFER `{collection_name}`"
        result = scope.query(query)
        schema_list = []
        async for row in result:
            schema_list.append(row)
        
        # INFER returns a list, we flatten it
        return schema_list[0] if schema_list else []
    except Exception as e:
        logger.error(f"Error inferring schema for {scope_name}.{collection_name}: {e}")
        return []


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
            logger.warning("Background thread: Missing connection settings, will retry later")
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

                # Get existing database info from store
                store = get_catalog_store()
                old_database_info = store.get_database_info()
                old_hash = _compute_schema_hash(old_database_info) if old_database_info else None

                # Collect schema information with incremental updates
                database_info, refreshed, skipped = await _collect_buckets_scopes_collections(
                    cluster, old_database_info, incremental=True
                )

                # Compute hash of the new schema
                new_hash = _compute_schema_hash(database_info)

                # Only update store if schema changed
                if old_hash != new_hash:
                    logger.info(
                        f"Schema change detected, updating store and setting enrichment flag "
                        f"(refreshed: {refreshed}, skipped: {skipped})"
                    )
                    store.add_database_info(database_info)
                    store.set_schema_hash(new_hash)
                    store.set_needs_enrichment(True)
                    store.set_last_full_refresh(datetime.utcnow().isoformat())

                    # Signal the enrichment system via the bridge (event-driven)
                    bridge = get_enrichment_bridge()
                    bridge.signal_from_thread()
                    logger.debug("Signaled enrichment bridge")
                else:
                    logger.debug(f"No schema changes detected (refreshed: {refreshed}, skipped: {skipped})")

                logger.info("Catalog refresh cycle completed")
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
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            
            # Close the loop
            loop.close()
            logger.info("Catalog worker event loop closed")
        except Exception as e:
            logger.error(f"Error closing event loop: {e}", exc_info=True)
    
    logger.info("Catalog background worker thread stopped")

