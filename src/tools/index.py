"""
Tools for index operations.

This module contains tools for listing and managing indexes in the Couchbase cluster.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.constants import MCP_SERVER_NAME
from utils.context import get_cluster_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.index")


def list_indexes(
    ctx: Context,
    bucket_name: str | None = None,
    scope_name: str | None = None,
    collection_name: str | None = None,
) -> list[dict[str, Any]]:
    """List all indexes in the cluster with optional filtering by bucket, scope, and collection.
    Returns a list of indexes with their names, definitions, and metadata.
    Each index entry includes: name, bucket, scope, collection, state, index type, and index key definitions.

    Args:
        ctx: MCP context for cluster connection
        bucket_name: Optional bucket name to filter indexes
        scope_name: Optional scope name to filter indexes (requires bucket_name)
        collection_name: Optional collection name to filter indexes (requires bucket_name and scope_name)

    Returns:
        List of dictionaries containing index information
    """
    cluster = get_cluster_connection(ctx)

    try:
        # Build query with filters based on provided parameters
        query = "SELECT idx.* FROM system:indexes AS idx"
        conditions = []
        params = {}

        if bucket_name:
            conditions.append("bucket_id = $bucket_name")
            params["bucket_name"] = bucket_name

        if scope_name:
            if not bucket_name:
                raise ValueError("bucket_name is required when filtering by scope_name")
            conditions.append("scope_id = $scope_name")
            params["scope_name"] = scope_name

        if collection_name:
            if not bucket_name or not scope_name:
                raise ValueError(
                    "bucket_name and scope_name are required when filtering by collection_name"
                )
            conditions.append("keyspace_id = $collection_name")
            params["collection_name"] = collection_name

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY bucket_id, scope_id, keyspace_id, name"

        # Execute query with parameters
        logger.info(f"Executing query: {query} with params: {params}")
        result = cluster.query(query, **params)

        indexes = []
        for row in result:
            # Extract relevant index information
            index_info = {
                "name": row.get("name"),
                "bucket": row.get("bucket_id"),
                "scope": row.get("scope_id"),
                "collection": row.get("keyspace_id"),
                "state": row.get("state"),
                "index_type": row.get("using", "GSI"),
                "is_primary": row.get("is_primary", False),
                "index_key": row.get("index_key", []),
                "condition": row.get("condition"),
                "partition": row.get("partition"),
            }
            indexes.append(index_info)

        logger.info(f"Found {len(indexes)} indexes")
        return indexes
    except Exception as e:
        logger.error(f"Error listing indexes: {e}")
        raise
