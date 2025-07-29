"""
Tools for querying the Couchbase database.

This module contains tools for getting the schema for a collection and running SQL++ queries.
"""

import logging
from typing import Any

from lark_sqlpp import modifies_data, modifies_structure, parse_sqlpp
from mcp.server.fastmcp import Context

from utils.constants import MCP_SERVER_NAME
from utils.context import ensure_bucket_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.query")


def get_schema_for_collection(
    ctx: Context, scope_name: str, collection_name: str
) -> dict[str, Any]:
    """Get the schema for a collection in the specified scope.
    Returns a dictionary with the collection name and the schema returned by running INFER query on the Couchbase collection.
    """
    schema = {"collection_name": collection_name, "schema": []}
    try:
        query = f"INFER {collection_name}"
        result = run_sql_plus_plus_query(ctx, scope_name, query)
        # Result is a list of list of schemas. We convert it to a list of schemas.
        if result:
            schema["schema"] = result[0]
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise
    return schema


def run_sql_plus_plus_query(
    ctx: Context, scope_name: str, query: str
) -> list[dict[str, Any]]:
    """Run a SQL++ query on a scope and return the results as a list of JSON objects."""
    bucket = ensure_bucket_connection(ctx)
    app_context = ctx.request_context.lifespan_context
    read_only_query_mode = app_context.read_only_query_mode
    logger.info(f"Running SQL++ queries in read-only mode: {read_only_query_mode}")

    try:
        scope = bucket.scope(scope_name)

        results = []
        # If read-only mode is enabled, check if the query is a data or structure modification query
        if read_only_query_mode:
            parsed_query = parse_sqlpp(query)
            data_modification_query = modifies_data(parsed_query)
            structure_modification_query = modifies_structure(parsed_query)

            if data_modification_query:
                logger.error("Data modification query is not allowed in read-only mode")
                raise ValueError(
                    "Data modification query is not allowed in read-only mode"
                )
            if structure_modification_query:
                logger.error(
                    "Structure modification query is not allowed in read-only mode"
                )
                raise ValueError(
                    "Structure modification query is not allowed in read-only mode"
                )

        # Run the query if it is not a data or structure modification query
        result = scope.query(query)
        for row in result:
            results.append(row)
        return results
    except Exception as e:
        logger.error(f"Error running query: {e!s}", exc_info=True)
        raise
