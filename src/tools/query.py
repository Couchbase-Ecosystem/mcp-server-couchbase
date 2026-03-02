"""
Tools for querying the Couchbase database.

This module contains tools for getting the schema for a collection and running SQL++ queries.
"""

import logging
from collections import Counter
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


def _is_explain_statement(query: str) -> bool:
    """Check if the query is an EXPLAIN statement."""
    return query.lstrip().upper().startswith("EXPLAIN ")


def run_sql_plus_plus_query(
    ctx: Context, bucket_name: str, scope_name: str, query: str
) -> list[dict[str, Any]]:
    """Run a SQL++ query on a scope and return the results as a list of JSON objects.

    The query will be run on the specified scope in the specified bucket.
    The query should use collection names directly without bucket/scope prefixes, as the scope context is automatically set.

    Example:
        query = "SELECT * FROM users WHERE age > 18"
        # Incorrect: "SELECT * FROM bucket.scope.users WHERE age > 18"
    """
    cluster = get_cluster_connection(ctx)

    bucket = connect_to_bucket(cluster, bucket_name)

    app_context = ctx.request_context.lifespan_context
    read_only_mode = app_context.read_only_mode
    read_only_query_mode = app_context.read_only_query_mode

    # Block query writes if either read_only_mode OR read_only_query_mode is True
    # READ_ONLY_MODE takes precedence and blocks all writes (KV and Query)
    # READ_ONLY_QUERY_MODE (deprecated) only blocks query writes
    block_query_writes = read_only_mode or read_only_query_mode

    try:
        scope = bucket.scope(scope_name)

        results = []
        # If read-only mode is enabled, check if the query is a data or structure modification query
        # EXPLAIN statements are always safe to execute and should bypass write checks.
        if block_query_writes and not _is_explain_statement(query):
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


def _extract_plan_from_explain_results(
    explain_results: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Extract the plan object from an EXPLAIN query response."""
    if not explain_results:
        return None

    first_row = explain_results[0]
    if not isinstance(first_row, dict):
        return None

    plan = first_row.get("plan")
    if isinstance(plan, dict):
        return plan

    if "#operator" in first_row or "~children" in first_row:
        return first_row

    for value in first_row.values():
        if isinstance(value, dict) and (
            "#operator" in value or "~children" in value or "plan" in value
        ):
            nested_plan = value.get("plan") if "plan" in value else value
            if isinstance(nested_plan, dict):
                return nested_plan

    return None


def _walk_plan(
    node: Any,
    operators: list[str],
    indexes_used: set[str],
    keyspaces: set[str],
) -> None:
    """Recursively walk an EXPLAIN plan node and collect details."""
    if isinstance(node, dict):
        operator = node.get("#operator")
        if isinstance(operator, str):
            operators.append(operator)

        index_name = node.get("index")
        if isinstance(index_name, str) and index_name:
            indexes_used.add(index_name)

        keyspace_name = node.get("keyspace")
        if isinstance(keyspace_name, str) and keyspace_name:
            keyspaces.add(keyspace_name)

        for value in node.values():
            _walk_plan(value, operators, indexes_used, keyspaces)
    elif isinstance(node, list):
        for item in node:
            _walk_plan(item, operators, indexes_used, keyspaces)


def evaluate_query_plan(plan: dict[str, Any] | None) -> dict[str, Any]:
    """Evaluate an EXPLAIN plan and return optimization findings."""
    if not plan:
        return {
            "summary": "No query plan found in EXPLAIN output.",
            "operators": [],
            "operator_counts": {},
            "indexes_used": [],
            "keyspaces": [],
            "findings": [],
        }

    operators: list[str] = []
    indexes_used: set[str] = set()
    keyspaces: set[str] = set()

    _walk_plan(plan, operators, indexes_used, keyspaces)

    operator_counts = dict(Counter(operators))
    findings: list[dict[str, str]] = []

    has_primary_scan = any(op.startswith("PrimaryScan") for op in operators)
    has_secondary_index_scan = any(op.startswith("IndexScan") for op in operators)
    has_fetch = "Fetch" in operator_counts

    if has_primary_scan:
        findings.append(
            {
                "severity": "warning",
                "issue": "primary_index_scan",
                "message": (
                    "Primary index scan detected. Consider creating a targeted "
                    "secondary index for better selectivity."
                ),
            }
        )

    if has_fetch and has_secondary_index_scan:
        findings.append(
            {
                "severity": "warning",
                "issue": "non_covering_index",
                "message": (
                    "Fetch operator detected after secondary index scan. "
                    "A covering index may reduce document fetches."
                ),
            }
        )

    if not findings:
        findings.append(
            {
                "severity": "info",
                "issue": "no_obvious_issues",
                "message": ("No common anti-patterns detected from the query plan."),
            }
        )

    summary = (
        "Plan has optimization opportunities."
        if any(f["severity"] == "warning" for f in findings)
        else "Plan looks healthy for common query-plan checks."
    )

    return {
        "summary": summary,
        "operators": sorted(operator_counts),
        "operator_counts": operator_counts,
        "indexes_used": sorted(indexes_used),
        "keyspaces": sorted(keyspaces),
        "findings": findings,
    }


def explain_sql_plus_plus_query(
    ctx: Context,
    bucket_name: str,
    scope_name: str,
    query: str,
) -> dict[str, Any]:
    """Generate and evaluate an EXPLAIN plan for a SQL++ query. It provides information about the execution plan for the query.

    This tool is stateless: callers must provide query and scope context explicitly.
    """
    normalized_query = query.strip()
    if not normalized_query:
        raise ValueError("Query cannot be empty.")

    explain_statement = (
        normalized_query
        if _is_explain_statement(normalized_query)
        else f"EXPLAIN {normalized_query}"
    )

    explain_results = run_sql_plus_plus_query(
        ctx,
        bucket_name,
        scope_name,
        explain_statement,
    )

    plan = _extract_plan_from_explain_results(explain_results)
    plan_evaluation = evaluate_query_plan(plan)

    return {
        "query": query,
        "explain_statement": explain_statement,
        "query_context": {
            "bucket_name": bucket_name,
            "scope_name": scope_name,
            "source": "explicit",
        },
        "plan": plan,
        "plan_evaluation": plan_evaluation,
        "results": explain_results,
    }


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


def _run_query_tool_with_empty_message(
    ctx: Context,
    query: str,
    *,
    limit: int,
    empty_message: str,
    extra_payload: dict[str, Any] | None = None,
    **query_kwargs: Any,
) -> list[dict[str, Any]]:
    """Execute a cluster query with a consistent empty-result response."""
    results = run_cluster_query(ctx, query, limit=limit, **query_kwargs)

    if results:
        return results

    payload: dict[str, Any] = {"message": empty_message, "results": []}
    if extra_payload:
        payload.update(extra_payload)
    return [payload]


def get_longest_running_queries(ctx: Context, limit: int = 10) -> list[dict[str, Any]]:
    """Get the N longest running queries from the system:completed_requests catalog.

    Args:
        limit: Number of queries to return (default: 10)

    Returns:
        List of queries with their average service time and count
    """
    query = """
    SELECT statement,
        DURATION_TO_STR(avgServiceTime) AS avgServiceTime,
        COUNT(1) AS queries
    FROM system:completed_requests
    WHERE UPPER(statement) NOT LIKE 'INFER %'
        AND UPPER(statement) NOT LIKE 'CREATE INDEX%'
        AND UPPER(statement) NOT LIKE 'CREATE PRIMARY INDEX%'
        AND UPPER(statement) NOT LIKE '% SYSTEM:%'
    GROUP BY statement
    LETTING avgServiceTime = AVG(STR_TO_DURATION(serviceTime))
    ORDER BY avgServiceTime DESC
    LIMIT $limit
    """

    return _run_query_tool_with_empty_message(
        ctx,
        query,
        limit=limit,
        empty_message=(
            "No completed queries were available to calculate longest running queries."
        ),
    )


def get_most_frequent_queries(ctx: Context, limit: int = 10) -> list[dict[str, Any]]:
    """Get the N most frequent queries from the system:completed_requests catalog.

    Args:
        limit: Number of queries to return (default: 10)

    Returns:
        List of queries with their frequency count
    """
    query = """
    SELECT statement,
        COUNT(1) AS queries
    FROM system:completed_requests
    WHERE UPPER(statement) NOT LIKE 'INFER %'
        AND UPPER(statement) NOT LIKE 'CREATE INDEX%'
        AND UPPER(statement) NOT LIKE 'CREATE PRIMARY INDEX%'
        AND UPPER(statement) NOT LIKE 'EXPLAIN %'
        AND UPPER(statement) NOT LIKE 'ADVISE %'
        AND UPPER(statement) NOT LIKE '% SYSTEM:%'
    GROUP BY statement
    LETTING queries = COUNT(1)
    ORDER BY queries DESC
    LIMIT $limit
    """

    return _run_query_tool_with_empty_message(
        ctx,
        query,
        limit=limit,
        empty_message=(
            "No completed queries were available to calculate most frequent queries."
        ),
    )


def get_queries_with_largest_response_sizes(
    ctx: Context, limit: int = 10
) -> list[dict[str, Any]]:
    """Get queries with the largest response sizes from the system:completed_requests catalog.

    Args:
        limit: Number of queries to return (default: 10)

    Returns:
        List of queries with their average result size in bytes, KB, and MB
    """
    query = """
    SELECT statement,
        avgResultSize AS avgResultSizeBytes,
        (avgResultSize / 1000) AS avgResultSizeKB,
        (avgResultSize / 1000000) AS avgResultSizeMB,
        COUNT(1) AS queries
    FROM system:completed_requests
    WHERE UPPER(statement) NOT LIKE 'INFER %'
        AND UPPER(statement) NOT LIKE 'CREATE INDEX%'
        AND UPPER(statement) NOT LIKE 'CREATE PRIMARY INDEX%'
        AND UPPER(statement) NOT LIKE '% SYSTEM:%'
    GROUP BY statement
    LETTING avgResultSize = AVG(resultSize)
    ORDER BY avgResultSize DESC
    LIMIT $limit
    """

    return _run_query_tool_with_empty_message(
        ctx,
        query,
        limit=limit,
        empty_message=(
            "No completed queries were available to calculate response sizes."
        ),
    )


def get_queries_with_large_result_count(
    ctx: Context, limit: int = 10
) -> list[dict[str, Any]]:
    """Get queries with the largest result counts from the system:completed_requests catalog.

    Args:
        limit: Number of queries to return (default: 10)

    Returns:
        List of queries with their average result count
    """
    query = """
    SELECT statement,
        avgResultCount,
        COUNT(1) AS queries
    FROM system:completed_requests
    WHERE UPPER(statement) NOT LIKE 'INFER %' AND
        UPPER(statement) NOT LIKE 'CREATE INDEX%' AND
        UPPER(statement) NOT LIKE 'CREATE PRIMARY INDEX%' AND
        UPPER(statement) NOT LIKE '% SYSTEM:%'
    GROUP BY statement
    LETTING avgResultCount = AVG(resultCount)
    ORDER BY avgResultCount DESC
    LIMIT $limit
    """

    return _run_query_tool_with_empty_message(
        ctx,
        query,
        limit=limit,
        empty_message=(
            "No completed queries were available to calculate result counts."
        ),
    )


def get_queries_using_primary_index(
    ctx: Context, limit: int = 10
) -> list[dict[str, Any]]:
    """Get queries that use a primary index from the system:completed_requests catalog.

    Args:
        limit: Number of queries to return (default: 10)

    Returns:
        List of queries that use primary indexes, ordered by result count
    """
    query = """
    SELECT *
    FROM system:completed_requests
    WHERE phaseCounts.`primaryScan` IS NOT MISSING
        AND UPPER(statement) NOT LIKE '% SYSTEM:%'
    ORDER BY resultCount DESC
    LIMIT $limit
    """

    return _run_query_tool_with_empty_message(
        ctx,
        query,
        limit=limit,
        empty_message=(
            "No queries using the primary index were found in system:completed_requests."
        ),
    )


def get_queries_not_using_covering_index(
    ctx: Context, limit: int = 10
) -> list[dict[str, Any]]:
    """Get queries that don't use a covering index from the system:completed_requests catalog.

    Args:
        limit: Number of queries to return (default: 10)

    Returns:
        List of queries that perform index scans but also require fetches (not covering)
    """
    query = """
    SELECT *
    FROM system:completed_requests
    WHERE phaseCounts.`indexScan` IS NOT MISSING
        AND phaseCounts.`fetch` IS NOT MISSING
        AND UPPER(statement) NOT LIKE '% SYSTEM:%'
    ORDER BY resultCount DESC
    LIMIT $limit
    """

    return _run_query_tool_with_empty_message(
        ctx,
        query,
        limit=limit,
        empty_message=(
            "No queries that require fetches after index scans were found "
            "in system:completed_requests."
        ),
    )


def get_queries_not_selective(ctx: Context, limit: int = 10) -> list[dict[str, Any]]:
    """Get queries that are not very selective from the system:completed_requests catalog.

    Args:
        limit: Number of queries to return (default: 10)

    Returns:
        List of queries where index scans return significantly more documents than the final result
    """
    query = """
    SELECT statement,
       AVG(phaseCounts.`indexScan` - resultCount) AS diff
    FROM system:completed_requests
    WHERE phaseCounts.`indexScan` > resultCount
    GROUP BY statement
    ORDER BY diff DESC
    LIMIT $limit
    """

    return _run_query_tool_with_empty_message(
        ctx,
        query,
        limit=limit,
        empty_message=(
            "No non-selective queries were found in system:completed_requests."
        ),
    )
