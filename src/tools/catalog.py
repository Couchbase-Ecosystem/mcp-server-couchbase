"""
Tools for catalog operations.

This module contains tools for interacting with the catalog system,
including getting catalog status, retrieving cached schemas, and
triggering on-demand refresh.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from catalog.jobs.queue import InferenceJob, InferenceJobQueue, JobPriority
from catalog.store.store import get_catalog_store
from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.catalog")

# Global job queue for on-demand refresh requests
_catalog_job_queue: InferenceJobQueue | None = None


def _get_job_queue() -> InferenceJobQueue:
    """Get or create the global job queue."""
    global _catalog_job_queue
    if _catalog_job_queue is None:
        _catalog_job_queue = InferenceJobQueue()
    return _catalog_job_queue


def get_catalog_status(ctx: Context) -> dict[str, Any]:
    """
    Get the current status of the catalog system.

    Returns information about:
    - Number of collections indexed
    - Last refresh time
    - Enrichment status
    - Job queue statistics (if available)
    """
    try:
        store = get_catalog_store()
        database_info = store.get_database_info()

        # Count collections
        collection_count = 0
        bucket_count = 0
        if database_info and "buckets" in database_info:
            bucket_count = len(database_info["buckets"])
            for bucket_data in database_info["buckets"].values():
                for scope_data in bucket_data.get("scopes", {}).values():
                    collection_count += len(scope_data.get("collections", {}))

        # Get collection metadata count
        all_metadata = store.get_all_collection_metadata()

        # Get enrichment status
        needs_enrichment = store.get_needs_enrichment()
        has_enriched_prompt = bool(store.get_prompt())

        # Get job queue status if available
        job_queue_status = None
        try:
            queue = _get_job_queue()
            job_queue_status = queue.get_status()
        except Exception:
            pass

        return {
            "status": "active",
            "statistics": {
                "buckets_indexed": bucket_count,
                "collections_indexed": collection_count,
                "collections_tracked": len(all_metadata),
            },
            "enrichment": {
                "needs_enrichment": needs_enrichment,
                "has_enriched_prompt": has_enriched_prompt,
            },
            "last_full_refresh": store.get_last_full_refresh(),
            "schema_hash": store.get_schema_hash()[:16] + "..." if store.get_schema_hash() else None,
            "job_queue": job_queue_status,
        }

    except Exception as e:
        logger.error(f"Error getting catalog status: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
        }


def get_collection_schema_from_catalog(
    ctx: Context,
    bucket_name: str,
    scope_name: str,
    collection_name: str,
) -> dict[str, Any]:
    """
    Get cached schema for a collection from the catalog.

    This returns the schema variants without running INFER,
    using cached data from the catalog. This is faster than
    running INFER directly but may not reflect the latest changes.

    Args:
        bucket_name: Name of the bucket
        scope_name: Name of the scope
        collection_name: Name of the collection

    Returns:
        Schema information including variants and indexes, or error if not found
    """
    try:
        store = get_catalog_store()
        database_info = store.get_database_info()

        # Navigate to the collection
        bucket_data = database_info.get("buckets", {}).get(bucket_name)
        if not bucket_data:
            return {
                "status": "not_found",
                "error": f"Bucket '{bucket_name}' not found in catalog",
            }

        scope_data = bucket_data.get("scopes", {}).get(scope_name)
        if not scope_data:
            return {
                "status": "not_found",
                "error": f"Scope '{scope_name}' not found in bucket '{bucket_name}'",
            }

        collection_data = scope_data.get("collections", {}).get(collection_name)
        if not collection_data:
            return {
                "status": "not_found",
                "error": f"Collection '{collection_name}' not found in scope '{scope_name}'",
            }

        # Get collection metadata for additional info
        collection_path = f"{bucket_name}/{scope_name}/{collection_name}"
        metadata = store.get_collection_metadata(collection_path)

        return {
            "status": "success",
            "bucket": bucket_name,
            "scope": scope_name,
            "collection": collection_name,
            "schema": collection_data.get("schema", []),
            "indexes": collection_data.get("indexes", []),
            "metadata": {
                "last_infer_time": metadata.last_infer_time if metadata else None,
                "document_count": metadata.document_count if metadata else None,
                "schema_hash": metadata.schema_hash[:16] + "..." if metadata and metadata.schema_hash else None,
            },
        }

    except Exception as e:
        logger.error(f"Error getting collection schema from catalog: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
        }


def refresh_collection_schema(
    ctx: Context,
    bucket_name: str,
    scope_name: str,
    collection_name: str,
) -> dict[str, Any]:
    """
    Request immediate schema refresh for a collection.

    This queues a high-priority inference job for the specified collection.
    The refresh will happen asynchronously in the background.

    Args:
        bucket_name: Name of the bucket
        scope_name: Name of the scope
        collection_name: Name of the collection

    Returns:
        Status indicating whether the refresh was queued
    """
    try:
        import asyncio

        collection_path = f"{bucket_name}/{scope_name}/{collection_name}"

        # Create high-priority job
        job = InferenceJob(
            priority=JobPriority.HIGH,
            bucket=bucket_name,
            scope=scope_name,
            collection=collection_name,
        )

        # Try to enqueue (this is async, but we need to handle it)
        queue = _get_job_queue()

        # Use asyncio to run the enqueue
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, create a task
            asyncio.create_task(queue.enqueue(job))
            queued = True
        except RuntimeError:
            # Not in async context - run in new loop
            queued = asyncio.run(queue.enqueue(job))

        if queued:
            logger.info(f"Queued high-priority refresh for {collection_path}")
            return {
                "status": "queued",
                "message": f"Schema refresh queued for {collection_path}",
                "collection_path": collection_path,
                "priority": "HIGH",
            }
        else:
            return {
                "status": "already_pending",
                "message": f"Refresh already pending for {collection_path}",
                "collection_path": collection_path,
            }

    except Exception as e:
        logger.error(f"Error queuing collection refresh: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
        }


def get_enriched_database_context(ctx: Context) -> dict[str, Any]:
    """
    Get the LLM-enriched database context prompt.

    Returns the natural language description of the database
    generated by the enrichment system. This provides context
    about collections, relationships, and query patterns.

    Returns:
        The enriched prompt or status indicating it's not available
    """
    try:
        store = get_catalog_store()
        enriched_prompt = store.get_prompt()

        if enriched_prompt:
            return {
                "status": "available",
                "enriched_prompt": enriched_prompt,
                "prompt_length": len(enriched_prompt),
            }
        else:
            needs_enrichment = store.get_needs_enrichment()
            return {
                "status": "not_available",
                "message": "Enriched context is not yet available",
                "needs_enrichment": needs_enrichment,
                "hint": "The catalog worker will generate enriched context after schema collection",
            }

    except Exception as e:
        logger.error(f"Error getting enriched database context: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
        }
