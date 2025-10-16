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


def list_indexes(ctx: Context, bucket_name: str | None = None) -> list[dict[str, Any]]:
    """List all indexes in the cluster or filter by bucket name.
    Returns a list of indexes with their names, definitions, and metadata.
    Each index entry includes: name, bucket, scope, collection, state, index type, and index key definitions.
    If bucket_name is provided, only indexes for that bucket are returned.
    """
    cluster = get_cluster_connection(ctx)

    try:
        # Query system catalog for index information
        if bucket_name:
            # Filter indexes by bucket
            query = "SELECT idx.* FROM system:indexes AS idx WHERE bucket_id = $bucket_name ORDER BY bucket_id, scope_id, keyspace_id, name"
            result = cluster.query(query, bucket_name=bucket_name)
        else:
            # Get all accessible indexes
            query = "SELECT idx.* FROM system:indexes AS idx ORDER BY bucket_id, scope_id, keyspace_id, name"
            result = cluster.query(query)

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
