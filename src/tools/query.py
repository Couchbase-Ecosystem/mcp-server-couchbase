"""
Tools for querying the Couchbase database.

This module contains tools for getting the schema for a collection and running SQL++ queries.
"""

import json
import logging
from typing import Annotated, Any

from lark_sqlpp import modifies_data, modifies_structure, parse_sqlpp
from mcp.server.fastmcp import Context
from pydantic import Field

from utils.agent import call_agent, extract_answer
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
        if block_query_writes:
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



def _compact_property(prop_name: str, prop_info: dict[str, Any]) -> dict[str, Any]:
    """Compact a single INFER property into an LLM-friendly dict.

    Keeps: field name, type, %docs (presence percentage), samples,
    and recursively compacts nested objects and array items.

    To keep the prompt size manageable, ``samples`` are only included on
    *leaf* fields (string, number, boolean, null).  For object and array
    fields we already recurse into their ``properties`` / ``items``, so
    the full-document samples would be redundant.
    """
    entry: dict[str, Any] = {"field": prop_name}
    field_type = prop_info.get("type")

    if field_type is not None:
        entry["type"] = field_type

    if "%docs" in prop_info:
        entry["%docs"] = prop_info["%docs"]

    # ── Nested object properties ─────────────────────────────────────
    has_nested = False
    if "properties" in prop_info:
        has_nested = True
        entry["properties"] = [
            _compact_property(k, v)
            for k, v in prop_info["properties"].items()
        ]

    # ── Array items ──────────────────────────────────────────────────
    items_raw = prop_info.get("items")
    if items_raw is not None:
        if isinstance(items_raw, dict):
            # Homogeneous array — items is a single object schema
            has_nested = True
            if "properties" in items_raw:
                entry["items"] = [
                    _compact_property(k, v)
                    for k, v in items_raw["properties"].items()
                ]
            else:
                # Scalar array (e.g. items: {type: "number"})
                entry["item_type"] = items_raw.get("type")
        elif isinstance(items_raw, list):
            # Tuple-style (positional) array — items is a list of schemas
            has_nested = True
            compact_items: list[dict[str, Any]] = []
            for idx, item_schema in enumerate(items_raw):
                if isinstance(item_schema, dict) and "properties" in item_schema:
                    # Object element at this position
                    compact_items.append({
                        "index": idx,
                        "type": item_schema.get("type", "object"),
                        "properties": [
                            _compact_property(k, v)
                            for k, v in item_schema["properties"].items()
                        ],
                    })
                elif isinstance(item_schema, dict):
                    compact_items.append({
                        "index": idx,
                        "type": item_schema.get("type"),
                    })
            if compact_items:
                entry["items"] = compact_items

        if "minItems" in prop_info:
            entry["minItems"] = prop_info["minItems"]
        if "maxItems" in prop_info:
            entry["maxItems"] = prop_info["maxItems"]

    # ── Samples — only on leaf types to avoid bloat ──────────────────
    if "samples" in prop_info and not has_nested:
        entry["samples"] = prop_info["samples"]

    return entry


def _compact_infer_schema(infer_result: list[Any]) -> list[dict[str, Any]]:
    """Compact the raw INFER output into an LLM-friendly representation.

    INFER returns ``[[flavor_1, flavor_2, ...]]`` — an array wrapping an
    array of schema "flavors".  Each flavor is a JSON-Schema-like object
    with a ``properties`` dict where every property already carries:

    - ``type`` — data type (string, number, boolean, array, object, null)
    - ``#docs`` / ``%docs`` — how many / what % of sampled docs contain it
    - ``samples`` — example values drawn from the sample population

    This helper keeps the useful bits (field, type, %docs, samples) and
    recursively compacts nested objects and array items, stripping verbose
    JSON-Schema metadata the downstream agent doesn't need.

    Samples are only preserved on leaf-level fields to avoid massive
    redundancy — for compound types the nested schema already conveys
    the structure.
    """
    # INFER returns [[flavor, ...]] — unwrap the outer array
    flavors = infer_result[0] if infer_result else []
    if isinstance(flavors, dict):
        # Single flavor returned without inner list wrapping
        flavors = [flavors]

    compacted: list[dict[str, Any]] = []
    for flavor in flavors:
        flavor_entry: dict[str, Any] = {}
        if "Flavor" in flavor:
            flavor_entry["flavor"] = flavor["Flavor"]
        if "#docs" in flavor:
            flavor_entry["docs_sampled"] = flavor["#docs"]

        props = flavor.get("properties", {})
        flavor_entry["fields"] = [
            _compact_property(k, v)
            for k, v in props.items()
            if k != "~meta"  # skip internal meta field
        ]
        compacted.append(flavor_entry)

    return compacted


def _fetch_collection_indexes(
    scope: Any, bucket_name: str, scope_name: str, collection_name: str
) -> list[dict[str, Any]]:
    """Return GSI index definitions relevant to a single collection."""
    try:
        query = (
            "SELECT raw idx "
            "FROM system:indexes AS idx "
            "WHERE idx.bucket_id = $bucket "
            "  AND idx.scope_id = $scope "
            "  AND idx.keyspace_id = $collection"
        )
        result = scope.query(
            query,
            bucket=bucket_name,
            scope=scope_name,
            collection=collection_name,
        )
        indexes: list[dict[str, Any]] = []
        for row in result:
            indexes.append({
                "name": row.get("name"),
                "index_key": row.get("index_key"),
                "condition": row.get("condition"),
                "is_primary": row.get("is_primary", False),
            })
        return indexes
    except Exception as e:
        logger.warning(
            "Could not fetch indexes for %s: %s", collection_name, e
        )
        return []


def _build_query_generation_prompt(
    *,
    user_question: str,
    bucket_name: str,
    scope_name: str,
    collection_metadata: dict[str, dict[str, Any]],
) -> str:
    """Build a structured, multi-section prompt for the query-generation agent.

    Sections
    --------
    1. Target keyspace — bucket / scope context so the agent knows the query
       scope is already set and collection names should be used directly.
    2. Per-collection metadata — schema, indexes, and sample docs.
    3. SQL++ guidelines — concise rules that prevent common mistakes.
    4. User question — the natural-language request.
    """
    lines: list[str] = []

    # ── 1. Keyspace context ──────────────────────────────────────────────
    lines.append("## Target Keyspace")
    lines.append(f"- **Bucket:** `{bucket_name}`")
    lines.append(f"- **Scope:** `{scope_name}`")
    lines.append(
        "- The query will be executed with the scope context already set. "
        "Use collection names directly — do NOT prefix with bucket or scope."
    )
    lines.append("")

    # ── 2. Per-collection metadata ───────────────────────────────────────
    for coll_name, meta in collection_metadata.items():
        lines.append(f"## Collection: `{coll_name}`")

        # Schema (includes per-field sample values from INFER)
        schema = meta.get("schema")
        if schema:
            lines.append("### Schema (from INFER — includes field types, prevalence, and sample values)")
            lines.append("```json")
            lines.append(json.dumps(schema, indent=2))
            lines.append("```")
        else:
            lines.append("_No schema available._")

        # Indexes
        # indexes = meta.get("indexes")
        # if indexes:
        #     lines.append("### Indexes")
        #     lines.append("```json")
        #     lines.append(json.dumps(indexes, indent=2))
        #     lines.append("```")
        # else:
        #     lines.append("_No secondary indexes defined._")
        # lines.append("")

    # ── 3. SQL++ guidelines ──────────────────────────────────────────────
    # lines.append("## SQL++ Guidelines")
    # lines.append(
    #     "- Use backtick-quoted identifiers for collection and field names "
    #     "that contain special characters.\n"
    #     "- Prefer indexed fields in WHERE / JOIN predicates when indexes exist.\n"
    #     "- Use UNNEST for querying inside arrays.\n"
    #     "- Use META().id to retrieve the document key.\n"
    #     "- Always add a LIMIT clause unless the user explicitly wants all results.\n"
    #     "- For JOINs across collections in the same scope, use ANSI JOIN syntax.\n"
    #     "- Return only the SQL++ query without explanation unless the user asks for one."
    # )
    # lines.append("")

    # ── 4. User question ─────────────────────────────────────────────────
    lines.append("## User Question")
    lines.append(user_question)

    return "\n".join(lines)


def generate_query(
    ctx: Context,
    message: Annotated[
        str,
        Field(
            description=(
                "The user's natural-language request describing the data they "
                "want to retrieve or modify. Be as specific as possible — "
                "include field names, filter conditions, sort order, and "
                "result limits when known."
            ),
        ),
    ],
    bucket_name: Annotated[
        str,
        Field(
            description=(
                "Couchbase bucket that contains the target scope and "
                "collections. Must already exist on the cluster."
            ),
        ),
    ],
    scope_name: Annotated[
        str,
        Field(
            description=(
                "Scope inside the bucket where the target collections reside."
            ),
        ),
    ],
    collection_names: Annotated[
        list[str],
        Field(
            description=(
                "One or more collection names involved in the query. "
                "Include all collections needed for JOINs. "
                "Use get_scopes_and_collections_in_bucket to discover names "
                "if unsure."
            ),
        ),
    ],
) -> str:
    """Generate a SQL++ query from a natural-language description.

    Use this tool when the user wants to **create a SQL++ query** from a
    plain-English description. The tool introspects the target collections
    (schema, sample data) and delegates to a specialised
    query-generation agent that returns a ready-to-run SQL++ statement.

    Returns:
        A SQL++ query string ready to execute, or an error message.
    """
    logger.debug(
        "generate_query — message=%s, collections=%s, bucket=%s, scope=%s",
        message,
        collection_names,
        bucket_name,
        scope_name,
    )

    if not collection_names:
        return (
            "Error: At least one collection name is required. "
            "Use get_scopes_and_collections_in_bucket to discover available collections."
        )

    if not message or not message.strip():
        return "Error: A natural-language message describing the desired query is required."

    # ── Gather metadata for each collection ──────────────────────────────
    cluster = get_cluster_connection(ctx=ctx)
    bucket = connect_to_bucket(cluster, bucket_name)
    scope = bucket.scope(scope_name)

    collection_metadata: dict[str, dict[str, Any]] = {}
    for coll in collection_names:
        escaped = coll.replace("`", "``")

        # Schema via INFER (includes per-field samples automatically)
        try:
            infer_result = scope.query(f"INFER `{escaped}`")
            schema = list(infer_result)
            #schema = _compact_infer_schema(raw_schema)
        except Exception as e:
            logger.warning("INFER failed for %s: %s", coll, e)
            schema = []

        # Indexes
        #indexes = _fetch_collection_indexes(scope, bucket_name, scope_name, coll)

        collection_metadata[coll] = {
            "schema": schema,
            #"indexes": indexes,
        }

    # ── Build structured prompt ──────────────────────────────────────────
    prompt = _build_query_generation_prompt(
        user_question=message,
        bucket_name=bucket_name,
        scope_name=scope_name,
        collection_metadata=collection_metadata,
    )

    # ── Call the agent backend ─────────────────────────────────────
    try:
        resp_body = call_agent(
            content=prompt,
            extra_payload={"collection_names": ",".join(collection_names)},
        )
        return extract_answer(resp_body)
    except (ConnectionError, RuntimeError) as exc:
        return f"Error: {exc}"
