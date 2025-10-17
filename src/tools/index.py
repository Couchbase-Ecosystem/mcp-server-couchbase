"""
Tools for index operations and optimization.

This module contains tools for getting index recommendations using the Couchbase Index Advisor
and creating indexes based on those recommendations.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.constants import MCP_SERVER_NAME
from utils.context import get_cluster_connection

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
    cluster = get_cluster_connection(ctx)

    try:
        # Escape single quotes in the query by doubling them for SQL++ string literal
        escaped_query = query.replace("'", "''")

        # Build the ADVISOR query
        advisor_query = f"SELECT ADVISOR('{escaped_query}') AS advisor_result"

        logger.info("Running Index Advisor for the provided query")

        # Execute the ADVISOR function at cluster level
        result = cluster.query(advisor_query)

        # Extract the advisor result from the query response
        advisor_results = list(result)

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


def create_index_from_recommendation(
    ctx: Context, index_definition: str
) -> dict[str, Any]:
    """Create an index using the provided CREATE INDEX statement.

    This tool executes a CREATE INDEX statement, typically from Index Advisor recommendations.
    Note: This operation requires write permissions and will fail if:
    - The read-only query mode is enabled
    - The user lacks CREATE INDEX permissions
    - An index with the same name already exists

    The index_definition should be a complete CREATE INDEX statement, for example:
    CREATE INDEX adv_city_activity ON `travel-sample`.inventory.landmark(city, activity)

    Returns a dictionary with:
    - status: 'success' or 'error'
    - message: Description of the result
    - index_definition: The index statement that was executed (on success)
    """
    cluster = get_cluster_connection(ctx)

    app_context = ctx.request_context.lifespan_context
    read_only_query_mode = app_context.read_only_query_mode

    # Check if read-only mode is enabled
    if read_only_query_mode:
        logger.error("Cannot create index in read-only query mode")
        return {
            "status": "error",
            "message": "Index creation is not allowed in read-only query mode. Please disable read-only mode (CB_MCP_READ_ONLY_QUERY_MODE=false) to create indexes.",
            "index_definition": index_definition,
        }

    try:
        # Validate that the statement is a CREATE INDEX statement
        if not index_definition.strip().upper().startswith("CREATE INDEX"):
            logger.error("Invalid index definition: must start with CREATE INDEX")
            return {
                "status": "error",
                "message": "Invalid index definition. The statement must be a CREATE INDEX command.",
                "index_definition": index_definition,
            }

        logger.info(f"Creating index with definition: {index_definition}")

        # Execute the CREATE INDEX statement at cluster level
        result = cluster.query(index_definition)

        # Consume the result to ensure the query completes
        for _ in result:
            pass

        logger.info("Index created successfully")

        return {
            "status": "success",
            "message": "Index created successfully",
            "index_definition": index_definition,
        }

    except Exception as e:
        error_message = str(e)
        logger.error(f"Error creating index: {error_message}", exc_info=True)

        # Provide helpful error messages for common issues
        if "already exists" in error_message.lower():
            message = "An index with this name already exists. Consider using a different name or dropping the existing index first."
        elif (
            "permission" in error_message.lower()
            or "authorized" in error_message.lower()
        ):
            message = "Insufficient permissions to create index. Please ensure your user has the required permissions."
        else:
            message = f"Failed to create index: {error_message}"

        return {
            "status": "error",
            "message": message,
            "index_definition": index_definition,
            "error_details": error_message,
        }
