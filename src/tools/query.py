"""
Tools for querying the Couchbase database.

This module contains tools for getting the schema for a collection and running SQL++ queries.
"""

import logging
from typing import Any

from lark_sqlpp import modifies_data, modifies_structure, parse_sqlpp
from mcp.server.fastmcp import Context

from utils.connection import connect_to_bucket
from utils.constants import MCP_SERVER_NAME
from utils.context import get_cluster_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.query")


def get_schema_for_collection(
    ctx: Context, bucket_name: str, scope_name: str, collection_name: str
) -> dict[str, Any]:
    """Get the schema for a collection in the specified scope.
    Returns a dictionary with the collection name and the schema returned by running INFER query on the Couchbase collection.
    """
    schema = {"collection_name": collection_name, "schema": []}
    try:
        query = f"INFER `{collection_name}`"
        result = run_sql_plus_plus_query(ctx, bucket_name, scope_name, query)
        # Result is a list of list of schemas. We convert it to a list of schemas.
        if result:
            schema["schema"] = result[0]
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise
    return schema


def run_sql_plus_plus_query(
    ctx: Context, bucket_name: str, scope_name: str, query: str
) -> list[dict[str, Any]]:
    """Run a SQL++ query on a scope and return the results as a list of JSON objects."""
    cluster = get_cluster_connection(ctx)

    bucket = connect_to_bucket(cluster, bucket_name)

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


def run_cluster_query(ctx: Context, query: str, **kwargs: Any) -> list[dict[str, Any]]:
    """Run a query on the cluster object and return the results as a list of JSON objects."""

    cluster = get_cluster_connection(ctx)
    results = []

    try:
        result = cluster.query(query, **kwargs)
        for row in result:
            results.append(row)
        return results
    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise


def analyze_queries(
    ctx: Context, analysis_types: list[str], limit: int = 10
) -> dict[str, Any]:
    """Analyze query performance from system:completed_requests catalog.

    This tool provides comprehensive query performance analysis to identify bottlenecks,
    optimization opportunities, and performance issues.

    Available Analysis Types:
    - "longest_running": Top N queries with the highest average service time
    - "most_frequent": Top N most frequently executed queries
    - "largest_response": Queries returning the most data (by response size)
    - "large_result_count": Queries returning the most documents
    - "primary_index": Queries using primary indexes (typically inefficient)
    - "no_covering_index": Queries not using covering indexes (require document fetches)
    - "not_selective": Queries with poor selectivity (scan many, return few)
    - "all": Run all available analyses

    Args:
        ctx: MCP context for cluster connection
        analysis_types: List of analysis types to run (e.g., ["longest_running", "most_frequent"])
                       Use ["all"] to run all available analyses
        limit: Maximum number of results to return per analysis (default: 10)

    Returns:
        Dictionary containing results for each requested analysis type with:
        - description: What the analysis shows
        - results: List of query results
        - count: Number of results returned

    Example:
        analyze_queries(ctx, ["longest_running", "primary_index"], limit=5)
        Returns queries that are slow and queries using primary indexes.

    Note:
        Queries against system catalogs (INFER, CREATE INDEX, SYSTEM:*) are excluded
        from analysis to focus on application-level query performance.
    """
    # Define all available analyses
    analyses = {
        "longest_running": {
            "description": "Queries with the highest average service time (slowest queries)",
            "query": """
                SELECT statement,
                    DURATION_TO_STR(avgServiceTime) AS avgServiceTime,
                    COUNT(1) AS queries
                FROM system:completed_requests
                WHERE UPPER(statement) NOT LIKE 'INFER %'
                    AND UPPER(statement) NOT LIKE 'CREATE INDEX%'
                    AND UPPER(statement) NOT LIKE '% SYSTEM:%'
                GROUP BY statement
                LETTING avgServiceTime = AVG(STR_TO_DURATION(serviceTime))
                ORDER BY avgServiceTime DESC
                LIMIT $limit
            """,
        },
        "most_frequent": {
            "description": "Queries executed most frequently (high volume queries)",
            "query": """
                SELECT statement,
                    COUNT(1) AS queries
                FROM system:completed_requests
                WHERE UPPER(statement) NOT LIKE 'INFER %'
                    AND UPPER(statement) NOT LIKE 'CREATE INDEX%'
                    AND UPPER(statement) NOT LIKE '% SYSTEM:%'
                GROUP BY statement
                ORDER BY queries DESC
                LIMIT $limit
            """,
        },
        "largest_response": {
            "description": "Queries returning the most data by response size (memory-intensive queries)",
            "query": """
                SELECT statement,
                    avgResultSize AS avgResultSizeBytes,
                    (avgResultSize / 1000) AS avgResultSizeKB,
                    (avgResultSize / 1000000) AS avgResultSizeMB,
                    COUNT(1) AS queries
                FROM system:completed_requests
                WHERE UPPER(statement) NOT LIKE 'INFER %'
                    AND UPPER(statement) NOT LIKE 'CREATE INDEX%'
                    AND UPPER(statement) NOT LIKE '% SYSTEM:%'
                GROUP BY statement
                LETTING avgResultSize = AVG(resultSize)
                ORDER BY avgResultSize DESC
                LIMIT $limit
            """,
        },
        "large_result_count": {
            "description": "Queries returning the most documents (high document count queries)",
            "query": """
                SELECT statement,
                    avgResultCount,
                    COUNT(1) AS queries
                FROM system:completed_requests
                WHERE UPPER(statement) NOT LIKE 'INFER %'
                    AND UPPER(statement) NOT LIKE 'CREATE INDEX%'
                    AND UPPER(statement) NOT LIKE '% SYSTEM:%'
                GROUP BY statement
                LETTING avgResultCount = AVG(resultCount)
                ORDER BY avgResultCount DESC
                LIMIT $limit
            """,
        },
        "primary_index": {
            "description": "Queries using primary indexes (typically inefficient, should use secondary indexes)",
            "query": """
                SELECT *
                FROM system:completed_requests
                WHERE phaseCounts.`primaryScan` IS NOT MISSING
                    AND UPPER(statement) NOT LIKE '% SYSTEM:%'
                ORDER BY resultCount DESC
                LIMIT $limit
            """,
        },
        "no_covering_index": {
            "description": "Queries not using covering indexes (require document fetches, can be optimized)",
            "query": """
                SELECT *
                FROM system:completed_requests
                WHERE phaseCounts.`indexScan` IS NOT MISSING
                    AND phaseCounts.`fetch` IS NOT MISSING
                    AND UPPER(statement) NOT LIKE '% SYSTEM:%'
                ORDER BY resultCount DESC
                LIMIT $limit
            """,
        },
        "not_selective": {
            "description": "Queries with poor selectivity (scan many documents but return few, inefficient filtering)",
            "query": """
                SELECT statement,
                   AVG(phaseCounts.`indexScan` - resultCount) AS diff
                FROM system:completed_requests
                WHERE phaseCounts.`indexScan` > resultCount
                GROUP BY statement
                ORDER BY diff DESC
                LIMIT $limit
            """,
        },
    }

    # Expand "all" to all available analysis types
    if "all" in analysis_types:
        analysis_types = list(analyses.keys())

    # Validate analysis types
    invalid_types = [t for t in analysis_types if t not in analyses]
    if invalid_types:
        raise ValueError(
            f"Invalid analysis types: {invalid_types}. "
            f"Valid types are: {list(analyses.keys())} or 'all'"
        )

    # Run requested analyses
    results = {}
    for analysis_type in analysis_types:
        try:
            analysis_config = analyses[analysis_type]
            query_results = run_cluster_query(
                ctx, analysis_config["query"], limit=limit
            )

            results[analysis_type] = {
                "description": analysis_config["description"],
                "results": query_results,
                "count": len(query_results),
            }

            logger.info(
                f"Analysis '{analysis_type}' completed: {len(query_results)} results"
            )

        except Exception as e:
            logger.error(f"Error running analysis '{analysis_type}': {e}")
            results[analysis_type] = {
                "description": analysis_config["description"],
                "error": str(e),
                "count": 0,
            }

    return results
