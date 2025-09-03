"""
Tools for server operations.

This module contains tools for getting the server status, testing the connection, and getting the buckets in the cluster, the scopes and collections in the bucket.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from tools.query import run_cluster_query
from utils.config import get_settings
from utils.connection import connect_to_bucket
from utils.constants import MCP_SERVER_NAME
from utils.context import get_cluster_connection

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
        "read_only_query_mode": settings.get("read_only_query_mode", True),
        "password_configured": bool(settings.get("password")),
        "ca_cert_path_configured": bool(settings.get("ca_cert_path")),
        "client_cert_path_configured": bool(settings.get("client_cert_path")),
        "client_key_path_configured": bool(settings.get("client_key_path")),
    }

    app_context = ctx.request_context.lifespan_context
    connection_status = {
        "cluster_connected": app_context.cluster is not None,
    }

    return {
        "server_name": MCP_SERVER_NAME,
        "status": "running",
        "configuration": configuration,
        "connections": connection_status,
    }


def test_cluster_connection(
    ctx: Context, bucket_name: str | None = None
) -> dict[str, Any]:
    """Test the connection to Couchbase cluster and optionally to a bucket.
    This tool verifies the connection to the Couchbase cluster and bucket by establishing the connection if it is not already established.
    If bucket name is not provided, it will not try to connect to the bucket specified in the MCP server settings.
    Returns connection status and basic cluster information.
    """
    try:
        cluster = get_cluster_connection(ctx)
        bucket = None
        if bucket_name:
            bucket = connect_to_bucket(cluster, bucket_name)

        return {
            "status": "success",
            "cluster_connected": cluster.connected,
            "bucket_connected": bucket is not None,
            "bucket_name": bucket_name,
            "message": "Successfully connected to Couchbase cluster",
        }
    except Exception as e:
        return {
            "status": "error",
            "cluster_connected": False,
            "bucket_connected": False,
            "bucket_name": bucket_name,
            "error": str(e),
            "message": "Failed to connect to Couchbase cluster",
        }


def get_scopes_and_collections_in_bucket(
    ctx: Context, bucket_name: str
) -> dict[str, list[str]]:
    """Get the names of all scopes and collections in the bucket.
    Returns a dictionary with scope names as keys and lists of collection names as values.
    """
    cluster = get_cluster_connection(ctx)
    bucket = connect_to_bucket(cluster, bucket_name)
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


def get_buckets_in_cluster(ctx: Context) -> list[str]:
    """Get the names of all the accessible buckets in the cluster."""
    cluster = get_cluster_connection(ctx)
    bucket_manager = cluster.buckets()
    buckets_with_settings = bucket_manager.get_all_buckets()

    buckets = []
    for bucket in buckets_with_settings:
        buckets.append(bucket.name)

    return buckets


def get_scopes_in_bucket(ctx: Context, bucket_name: str) -> list[str]:
    """Get the names of all scopes in the given bucket."""
    cluster = get_cluster_connection(ctx)
    bucket = connect_to_bucket(cluster, bucket_name)
    try:
        scopes = bucket.collections().get_all_scopes()
        return [scope.name for scope in scopes]
    except Exception as e:
        logger.error(f"Error getting scopes in the bucket {bucket_name}: {e}")
        raise


def get_collections_in_scope(
    ctx: Context, bucket_name: str, scope_name: str
) -> list[str]:
    """Get the names of all collections in the given scope and bucket."""

    # Get the collections in the scope using system:all_keyspaces collection
    query = "SELECT DISTINCT(name) as collection_name FROM system:all_keyspaces where `bucket`=$bucket_name and `scope`=$scope_name"
    results = run_cluster_query(
        ctx, query, bucket_name=bucket_name, scope_name=scope_name
    )
    return [result["collection_name"] for result in results]
