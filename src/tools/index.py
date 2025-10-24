"""
Tools for index operations.

This module contains tools for listing and managing indexes in the Couchbase cluster and getting index recommendations using the Couchbase Index Advisor.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from tools.query import run_cluster_query
from utils.constants import MCP_SERVER_NAME
from utils.index_utils import generate_index_definition

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.index")


def get_index_advisor_recommendations(ctx: Context, query: str) -> dict[str, Any]:
    """Get index recommendations from Couchbase Index Advisor for a given SQL++ query.

    The Index Advisor analyzes the query and provides recommendations for optimal indexes.
    This tool works with SELECT, UPDATE, DELETE, or MERGE queries.
    The query should contain fully qualified keyspace (e.g., bucket.scope.collection).

    Returns a dictionary with:
    - current_used_indexes: Array of currently used indexes (if any)
    - recommended_indexes: Array of recommended secondary indexes (if any)
    - recommended_covering_indexes: Array of recommended covering indexes (if any)

    Each index object contains:
    - index: The CREATE INDEX SQL++ command
    - statements: Array of statement objects with the query and run count
    """
    try:
        # Build the ADVISOR query
        advisor_query = f"SELECT ADVISOR('{query}') AS advisor_result"

        logger.info("Running Index Advisor for the provided query")

        # Execute the ADVISOR function at cluster level using run_cluster_query
        advisor_results = run_cluster_query(ctx, advisor_query)

        if not advisor_results:
            return {
                "message": "No recommendations available",
                "current_used_indexes": [],
                "recommended_indexes": [],
                "recommended_covering_indexes": [],
            }

        # The result is wrapped in advisor_result key
        advisor_data = advisor_results[0].get("advisor_result", {})

        # Extract the relevant fields with defaults
        response = {
            "current_used_indexes": advisor_data.get("current_used_indexes", []),
            "recommended_indexes": advisor_data.get("recommended_indexes", []),
            "recommended_covering_indexes": advisor_data.get(
                "recommended_covering_indexes", []
            ),
        }

        # Add summary information for better user experience
        response["summary"] = {
            "current_indexes_count": len(response["current_used_indexes"]),
            "recommended_indexes_count": len(response["recommended_indexes"]),
            "recommended_covering_indexes_count": len(
                response["recommended_covering_indexes"]
            ),
            "has_recommendations": bool(
                response["recommended_indexes"]
                or response["recommended_covering_indexes"]
            ),
        }

        logger.info(
            f"Index Advisor completed. Found {response['summary']['recommended_indexes_count']} recommended indexes"
        )

        return response

    except Exception as e:
        logger.error(f"Error running Index Advisor: {e!s}", exc_info=True)
        raise


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
        result = run_cluster_query(ctx, query, **params)

        indexes = []
        for row in result:
            # Extract the actual index data from the nested structure
            # When querying system:all_indexes, data is wrapped in 'all_indexes' key
            index_data = row.get("all_indexes", row)

            # Skip sequential scan indexes
            using = index_data.get("using", "").lower()
            if using == "sequentialscan":
                continue

            # Check if definition exists in metadata, otherwise generate it
            index_definition = None
            metadata = index_data.get("metadata", {})

            # First, try to get definition from metadata
            if "definition" in metadata:
                index_definition = metadata["definition"]
                logger.debug(
                    f"Using definition from metadata for index: {index_data.get('name')}"
                )
            else:
                # If not in metadata, generate it
                logger.debug(
                    f"Generating definition for index: {index_data.get('name')}"
                )
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
                    "with_clause": index_data.get("with", {}),
                    "include_fields": index_data.get("include", []),
                }
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
        logger.error(f"Error listing indexes: {e}", exc_info=True)
        raise
