"""
Tools for index operations.

This module contains tools for listing and managing indexes in the Couchbase cluster and getting index recommendations using the Couchbase Index Advisor.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from tools.query import run_sql_plus_plus_query
from utils.config import get_settings
from utils.constants import MCP_SERVER_NAME
from utils.index_utils import (
    fetch_indexes_from_rest_api,
    process_index_data,
    validate_connection_settings,
    validate_filter_params,
)

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.index")


def get_index_advisor_recommendations(
    ctx: Context, bucket_name: str, scope_name: str, query: str
) -> dict[str, Any]:
    """Get index recommendations from Couchbase Index Advisor for a given SQL++ query.

    The Index Advisor analyzes the query and provides recommendations for optimal indexes.
    This tool works with SELECT, UPDATE, DELETE, or MERGE queries.
    The queries will be run on the specified scope in the specified bucket.

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

        # Execute the ADVISOR function at cluster level using run_sql_plus_plus_query
        advisor_results = run_sql_plus_plus_query(
            ctx, bucket_name, scope_name, advisor_query
        )

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
    index_name: str | None = None,
    include_raw_index_stats: bool = False,
) -> list[dict[str, Any]]:
    """List all indexes in the cluster with optional filtering by bucket, scope, collection, and index name.
    Returns a list of indexes with their names and CREATE INDEX definitions.
    Uses the Index Service REST API (/getIndexStatus) to retrieve index information directly.

    Args:
        ctx: MCP context for cluster connection
        bucket_name: Optional bucket name to filter indexes
        scope_name: Optional scope name to filter indexes (requires bucket_name)
        collection_name: Optional collection name to filter indexes (requires bucket_name and scope_name)
        index_name: Optional index name to filter indexes (requires bucket_name, scope_name, and collection_name)
        include_raw_index_stats: If True, include raw index stats (as-is from API) in addition
                              to cleaned-up version. Default is False.

    Returns:
        List of dictionaries with keys:
        - name (str): Index name
        - definition (str): Cleaned-up CREATE INDEX statement
        - status (str): Current status of the index (e.g., "Ready", "Building", "Deferred")
        - isPrimary (bool): Whether this is a primary index
        - bucket (str): Bucket name where the index exists
        - scope (str): Scope name where the index exists
        - collection (str): Collection name where the index exists
        - raw_index_stats (dict, optional): Complete raw index status object from API including metadata,
                                           state, keyspace info, etc. (only if include_raw_index_stats=True)
    """
    try:
        # Validate parameters
        validate_filter_params(bucket_name, scope_name, collection_name, index_name)

        # Get and validate connection settings
        settings = get_settings()
        validate_connection_settings(settings)

        # Fetch indexes from REST API
        logger.info(
            f"Fetching indexes from REST API for bucket={bucket_name}, "
            f"scope={scope_name}, collection={collection_name}, index={index_name}"
        )

        raw_indexes = fetch_indexes_from_rest_api(
            settings["connection_string"],
            settings["username"],
            settings["password"],
            bucket_name=bucket_name,
            scope_name=scope_name,
            collection_name=collection_name,
            index_name=index_name,
            ca_cert_path=settings.get("ca_cert_path"),
        )

        # Process and format the results
        indexes = [
            processed
            for idx in raw_indexes
            if (processed := process_index_data(idx, include_raw_index_stats))
            is not None
        ]

        logger.info(
            f"Found {len(indexes)} indexes from REST API "
            f"(include_raw_index_stats={include_raw_index_stats})"
        )
        return indexes

    except Exception as e:
        logger.error(f"Error listing indexes: {e}", exc_info=True)
        raise
