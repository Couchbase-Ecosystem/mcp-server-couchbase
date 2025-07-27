"""
Tools for server operations.

This module contains tools for getting the server status, testing the connection, and getting the scopes and collections in the bucket.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.config import get_settings
from utils.constants import MCP_SERVER_NAME
from utils.context import ensure_cluster_connection, ensure_bucket_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.server")


def get_server_configuration_status(ctx: Context) -> dict[str, Any]:
    """Get the server status and configuration without establishing connections.
    This tool can be used to verify the server is running and check configuration.
    """
    settings = get_settings()

    # Don't expose sensitive information like passwords
    configuration = {
        "connection_string": settings.get("connection_string", "Not set"),
        "username": settings.get("username", "Not set"),
        "bucket_name": settings.get("bucket_name", "Not set"),
        "read_only_query_mode": settings.get("read_only_query_mode", True),
        "password_configured": bool(settings.get("password")),
    }

    app_context = ctx.request_context.lifespan_context
    connection_status = {
        "cluster_connected": app_context.cluster is not None,
        "bucket_connected": app_context.bucket is not None,
    }

    return {
        "server_name": MCP_SERVER_NAME,
        "status": "running",
        "configuration": configuration,
        "connections": connection_status,
    }


def test_connection(ctx: Context, bucket_name: str = None) -> dict[str, Any]:
    """Test the connection to Couchbase cluster and optionally a specified bucket.
    Returns connection status and basic cluster information.
    """
    cluster_connected = False
    bucket_connected = False
    try:
        cluster = ensure_cluster_connection(ctx)
        cluster_connected = True
        if bucket_name is not None:
            try:
                bucket = ensure_bucket_connection(ctx, bucket_name)
            except Exception as e:
                return {
                    "status": "error",
                    "cluster_connected": cluster_connected,
                    "bucket_connected": bucket_connected,
                    "error": str(e),
                    "message": f"Failed to connect to bucket named {bucket_name}",
                }
        else:
            return {
                "status": "success",
                "cluster_connected": cluster_connected,
                "message": "Successfully connected to Couchbase cluster",
            }
    except Exception as e:
        return {
            "status": "error",
            "cluster_connected": cluster_connected,
            "error": str(e),
            "message": "Failed to connect to Couchbase",
        }


def get_list_of_buckets_with_settings(
    ctx: Context
) -> list[dict[str, Any]]:
    """Get the list of buckets from the Couchbase cluster, including their bucket settings and additional statistics.
    Returns a list of comprehensive bucket information objects including settings.
    """
    
    result = []
    
    try:
        cluster = ensure_cluster_connection(ctx)
        bucket_manager = cluster.buckets()
        buckets = bucket_manager.get_all_buckets()
        
        for bucket_settings in buckets:
            # Convert BucketSettings object to dictionary using available attributes
            bucket_dict = {"bucket_name": bucket_settings.name}
            
            # Add basic bucket settings with safe access
            for attr in ["bucket_type", "ram_quota", "num_replicas", "replica_indexes", 
                        "flush_enabled", "max_expiry", "compression_mode", 
                        "minimum_durability_level", "storage_backend", "eviction_policy", 
                        "conflict_resolution", "history_retention_collection_default",
                        "history_retention_bytes", "history_retention_duration"]:
                if hasattr(bucket_settings, attr):
                    value = getattr(bucket_settings, attr)
                    # If the value has a .value attribute (enum), use that
                    if hasattr(value, 'value'):
                        bucket_dict[attr] = value.value
                    else:
                        bucket_dict[attr] = value
            
            result.append(bucket_dict)
        
        return result
    except Exception as e:
        logger.error(f"Error getting bucket information: {e}")
        raise 
    
def get_scopes_and_collections_in_bucket(ctx: Context, bucket_name: str) -> dict[str, list[str]]:
    """Get the names of all scopes and collections for a specified bucket.
    Returns a dictionary with scope names as keys and lists of collection names as values.
    """
    try:
        bucket = ensure_bucket_connection(ctx, bucket_name)
    except Exception as e:
        logger.error(f"Error accessing bucket: {e}")
        raise ValueError("Tool does not have access to bucket, or bucket does not exist.") from e
    try:
        scopes_collections = {}
        collection_manager = bucket.collections()
        scopes = collection_manager.get_all_scopes()
        for scope in scopes:
            collection_names = [c.name for c in scope.collections]
            scopes_collections[scope.name] = collection_names
        return scopes_collections
    except Exception as e:
        logger.error(f"Error getting scopes and collections: {e}")
        raise
