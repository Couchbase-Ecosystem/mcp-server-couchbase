"""
Tools for server operations.

This module contains tools for getting the server status, testing the connection, and getting the scopes and collections in the bucket.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.config import get_settings
from utils.constants import MCP_SERVER_NAME
from utils.context import ensure_bucket_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.server")


def get_server_configuration_status(ctx: Context) -> dict[str, Any]:
    """Get the server status and configuration without establishing connection.
    This tool can be used to verify if the server is running and check the configuration.
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


def test_cluster_connection(ctx: Context) -> dict[str, Any]:
    """Test the connection to Couchbase cluster and bucket.
    This tool verifies the connection to the Couchbase cluster and bucket by establishing the connection if it is not already established.
    Returns connection status and basic cluster information.
    """
    try:
        bucket = ensure_bucket_connection(ctx)

        # Test basic connectivity by getting bucket name
        bucket_name = bucket.name

        return {
            "status": "success",
            "cluster_connected": True,
            "bucket_connected": True,
            "bucket_name": bucket_name,
            "message": "Successfully connected to Couchbase cluster and bucket",
        }
    except Exception as e:
        return {
            "status": "error",
            "cluster_connected": False,
            "bucket_connected": False,
            "error": str(e),
            "message": "Failed to connect to Couchbase",
        }


def get_scopes_and_collections_in_bucket(ctx: Context) -> dict[str, list[str]]:
    """Get the names of all scopes and collections in the bucket.
    Returns a dictionary with scope names as keys and lists of collection names as values.
    """
    bucket = ensure_bucket_connection(ctx)
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
