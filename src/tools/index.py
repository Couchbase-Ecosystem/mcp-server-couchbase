"""
Tools for index operations.

This module contains tools for listing and managing indexes in the Couchbase cluster.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.constants import MCP_SERVER_NAME
from utils.context import get_cluster_connection
from utils.index_utils import generate_index_definition

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.index")


def list_indexes(
    ctx: Context,
    bucket_name: str | None = None,
    scope_name: str | None = None,
    collection_name: str | None = None,
) -> list[dict[str, Any]]:
    """List all indexes in the cluster with optional filtering by bucket, scope, and collection.
    Returns a simplified list of indexes with their names, primary flag, and CREATE INDEX definitions.
    Excludes sequential scan indexes. For GSI indexes, includes the CREATE INDEX definition.

    Args:
        ctx: MCP context for cluster connection
        bucket_name: Optional bucket name to filter indexes
        scope_name: Optional scope name to filter indexes (requires bucket_name)
        collection_name: Optional collection name to filter indexes (requires bucket_name and scope_name)

    Returns:
        List of dictionaries with keys: name (str), is_primary (bool), definition (str, GSI only)
    """
    cluster = get_cluster_connection(ctx)

    try:
        # Build query with filters based on provided parameters
        query = "SELECT * FROM system:all_indexes"
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
            # Extract the actual index data from the nested structure
            # When querying system:all_indexes, data is wrapped in 'all_indexes' key
            index_data = row.get("all_indexes", row)

            # Skip sequential scan indexes
            using = index_data.get("using", "").lower()
            if using == "sequentialscan":
                continue

            # Prepare data for index definition generation
            temp_data = {
                "name": index_data.get("name"),
                "bucket": index_data.get("bucket_id"),
                "scope": index_data.get("scope_id"),
                "collection": index_data.get("keyspace_id"),
                "index_type": index_data.get("using", "gsi"),
                "is_primary": index_data.get("is_primary", False),
                "index_key": index_data.get("index_key", []),
                "condition": index_data.get("condition"),
                "partition": index_data.get("partition"),
                "using": index_data.get("using", "gsi"),
            }

            # Generate index definition for GSI indexes
            index_definition = generate_index_definition(temp_data)

            # Only return the essential information
            index_info = {
                "name": index_data.get("name"),
                "is_primary": index_data.get("is_primary", False),
            }

            # Add definition only if it was generated (GSI indexes only)
            if index_definition:
                index_info["definition"] = index_definition

            indexes.append(index_info)

        logger.info(f"Found {len(indexes)} indexes (excluding sequential scans)")
        return indexes
    except Exception as e:
        logger.error(f"Error listing indexes: {e}")
        raise
