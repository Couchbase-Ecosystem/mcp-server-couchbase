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
import hashlib
import json
import logging
import threading
from typing import Any

from acouchbase.cluster import AsyncCluster
from acouchbase.bucket import AsyncBucket

from utils.config import get_settings
from utils.connection import connect_to_bucket_async, connect_to_couchbase_cluster_async
from utils.constants import MCP_SERVER_NAME
from catalog.store.store import get_catalog_store
from catalog.schema import parse_infer_output, SchemaCollection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.catalog")


# Catalog refresh interval (5 minutes)
CATALOG_REFRESH_INTERVAL = 300  # seconds


def _compute_schema_hash(schema_data: dict[str, Any]) -> str:
    """Compute a hash of the schema data to detect changes."""
    schema_json = json.dumps(schema_data, sort_keys=True)
    return hashlib.sha256(schema_json.encode()).hexdigest()


async def _get_index_definitions(cluster: AsyncCluster, bucket_name: str, scope_name: str, collection_name: str) -> list[dict[str, Any]]:
    """Get index definitions for a collection."""
    try:
        query = f"SELECT * FROM system:indexes WHERE bucket_id = '{bucket_name}' AND scope_id = '{scope_name}' AND keyspace_id = '{collection_name}'"
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


async def _collect_buckets_scopes_collections(cluster: AsyncCluster, existing_database_info: dict[str, Any]) -> dict[str, Any]:
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
        # Get all buckets
        bucket_manager = cluster.buckets()
        all_buckets = await bucket_manager.get_all_buckets()
        
        # Sort buckets by name to maintain consistent order
        all_buckets_sorted = sorted(all_buckets, key=lambda b: b.name)
        
        for bucket_info in all_buckets_sorted:
            bucket_name = bucket_info.name
            logger.debug(f"Processing bucket: {bucket_name}")
            
            bucket = connect_to_bucket_async(cluster, bucket_name)
            scopes_data = {}
            
            # Get all scopes in the bucket
            collection_manager = bucket.collections()
            scopes = await collection_manager.get_all_scopes()
            
            # Sort scopes by name to maintain consistent order
            scopes_sorted = sorted(scopes, key=lambda s: s.name)
            
            for scope in scopes_sorted:
                scope_name = scope.name
                collections_data = {}
                
                # Get all collections in the scope
                # Sort collections by name to maintain consistent order
                collections_sorted = sorted(scope.collections, key=lambda c: c.name)
                
                for collection in collections_sorted:
                    collection_name = collection.name
                    logger.debug(f"Processing collection: {bucket_name}.{scope_name}.{collection_name}")
                    
                    # Run INFER query to get schema
                    raw_schema = await _infer_collection_schema(
                        bucket, scope_name, collection_name
                    )
                    
                    # Parse schema into SchemaCollection (multiple variants)
                    new_schema_collection = parse_infer_output(raw_schema)
                    
                    # Merge with existing schema if present
                    try:
                        existing_buckets = existing_database_info.get("buckets", {})
                        existing_bucket = existing_buckets.get(bucket_name, {})
                        existing_scopes = existing_bucket.get("scopes", {})
                        existing_scope = existing_scopes.get(scope_name, {})
                        existing_collections = existing_scope.get("collections", {})
                        existing_collection = existing_collections.get(collection_name, {})
                        existing_schema_list = existing_collection.get("schema", [])
                        
                        if existing_schema_list and isinstance(existing_schema_list, list):
                            # Load existing schema collection and merge with new
                            # Uses 70% similarity 1:1 matching
                            existing_schema_collection = SchemaCollection.from_dict(existing_schema_list)
                            existing_schema_collection.merge(new_schema_collection)
                            schema = existing_schema_collection.to_dict()
                            logger.debug(f"Merged schema for {bucket_name}.{scope_name}.{collection_name} ({len(existing_schema_collection)} variants)")
                        else:
                            # No existing schema, use new schema collection
                            schema = new_schema_collection.to_dict()
                    except Exception as e:
                        logger.warning(f"Error merging schema for {bucket_name}.{scope_name}.{collection_name}: {e}")
                        schema = new_schema_collection.to_dict()
                    
                    # Get indexes
                    indexes = await _get_index_definitions(cluster, bucket_name, scope_name, collection_name)
                            
                    collections_data[collection_name] = {
                        "name": collection_name,
                        "schema": schema,
                        "indexes": indexes,
                    }
                
                scopes_data[scope_name] = {
                    "name": scope_name,
                    "collections": collections_data
                }
            
            database_info["buckets"][bucket_name] = {
                "name": bucket_name,
                "scopes": scopes_data
            }
        
        logger.info(f"Collected schema for {len(database_info['buckets'])} buckets")
        return database_info
        
    except Exception as e:
        logger.error(f"Error collecting database schema: {e}", exc_info=True)
        return database_info


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
                
                # Collect schema information (with merging)
                database_info = await _collect_buckets_scopes_collections(cluster, old_database_info)
                
                # Compute hash of the new schema
                new_hash = _compute_schema_hash(database_info)
                
                # Only update store if schema changed
                if old_hash != new_hash:
                    logger.info("Schema change detected, updating store and setting enrichment flag")
                    store.add_database_info(database_info)
                    store.set_schema_hash(new_hash)
                    store.set_needs_enrichment(True)
                else:
                    logger.debug("No schema changes detected, skipping store update")
                
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

