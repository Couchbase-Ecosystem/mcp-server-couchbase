"""
Tools for index operations.

This module contains tools for listing and managing indexes in the Couchbase cluster and getting index recommendations using the Couchbase Index Advisor.
"""

import logging
from typing import Any

from fastmcp import Context

from ..utils.config import get_settings
from ..utils.constants import (
    MCP_SERVER_NAME,
    QUERY_SERVICE_LIST_INDEXES_MIN_MAJOR_VERSION,
)
from ..utils.context import get_cluster_connection
from ..utils.index_utils import (
    fetch_indexes_from_rest_api,
    process_index_data_from_query,
    process_index_data_from_rest_api,
    resolve_cluster_major_version,
    validate_connection_settings,
    validate_filter_params,
)
from .query import run_cluster_query, run_sql_plus_plus_query

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.index")


async def get_index_advisor_recommendations(
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
        advisor_results = await run_sql_plus_plus_query(
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


async def fetch_indexes_via_query_service(
    ctx: Context,
    bucket_name: str | None,
    scope_name: str | None,
    collection_name: str | None,
    index_name: str | None,
    return_raw_index_stats: bool = False,
) -> list[dict[str, Any]]:
    """Fetch indexes from ``system:indexes`` via the query service.

    Bucket / scope / collection are normalized in SQL++ via a LET clause so
    legacy bucket-level indexes (which carry only ``keyspace_id``) and modern
    scoped indexes (which carry ``bucket_id`` + ``scope_id`` + ``keyspace_id``)
    both produce the same enriched row shape. User-supplied filters are
    applied against the normalized aliases so they match symmetrically — a
    legacy index in bucket X matches ``bucket_name=X`` just like a modern
    one does.

    Args:
        return_raw_index_stats: When True, return raw ``s`` rows (no
            injected bucket/scope/collection). When False (default), each
            row is enriched with normalized ``bucket`` / ``scope`` /
            ``collection`` keys.

    Returns:
        List of dict rows from ``system:indexes``.
    """
    # Always present — guards future Couchbase pool/namespace additions and
    # restricts to GSI indexes.
    clauses: list[str] = ["s.namespace_id = 'default'", "s.`using` = 'gsi'"]
    params: dict[str, Any] = {}

    if bucket_name:
        clauses.append("bid = $bucket_id")
        params["bucket_id"] = bucket_name
    if scope_name:
        clauses.append("sid = $scope_id")
        params["scope_id"] = scope_name
    if collection_name:
        clauses.append("kid = $keyspace_id")
        params["keyspace_id"] = collection_name
    if index_name:
        clauses.append("s.name = $index_name")
        params["index_name"] = index_name

    let_clause = (
        "LET bid = IFMISSING(s.bucket_id, s.keyspace_id), "
        "sid = IFMISSING(s.scope_id, '_default'), "
        "kid = NVL2(s.bucket_id, s.keyspace_id, '_default')"
    )
    if return_raw_index_stats:
        select_clause = "SELECT RAW s"
    else:
        select_clause = (
            "SELECT s.*, bid AS `bucket`, sid AS `scope`, kid AS `collection`"
        )

    query = (
        f"{select_clause} FROM system:indexes AS s {let_clause} "
        f"WHERE {' AND '.join(clauses)}"
    )
    logger.info(f"Running list_indexes query: {query}")

    rows = await run_cluster_query(ctx, query, named_parameters=params)
    return [row for row in rows if isinstance(row, dict)]


async def list_indexes(
    ctx: Context,
    bucket_name: str | None = None,
    scope_name: str | None = None,
    collection_name: str | None = None,
    index_name: str | None = None,
    return_raw_index_stats: bool = False,
) -> list[dict[str, Any]]:
    """List all indexes in the cluster with optional filtering by bucket, scope, collection, and index name.
    Returns a list of indexes with their names and CREATE INDEX definitions.

    The data source depends on the Couchbase Server version:
    - Cluster version >= 8.x: query ``system:indexes`` via the query
      service, which exposes the original CREATE INDEX statement directly in
      ``metadata.definition``. Bucket / scope / collection are normalized in
      SQL++ via a LET clause so legacy bucket-level indexes and modern
      scoped indexes share one output shape.
    - Cluster version < 8.x: fall back to the
      Index Service REST API ``/getIndexStatus`` endpoint.

    Args:
        ctx: MCP context for cluster connection
        bucket_name: Optional bucket name to filter indexes
        scope_name: Optional scope name to filter indexes (requires bucket_name)
        collection_name: Optional collection name to filter indexes (requires bucket_name and scope_name)
        index_name: Optional index name to filter indexes (requires bucket_name, scope_name, and collection_name)
        return_raw_index_stats: If True, return the unprocessed source row
            for each index instead of the processed shape. Default is False.

    Returns:
        List of dictionaries. When ``return_raw_index_stats`` is True, each
        entry is the raw source row as returned by the data source (shape
        depends on whether the query service or REST endpoint was used).

        Otherwise, for successfully processed rows, each entry has:
        - name (str): Index name
        - definition (str): CREATE INDEX statement
        - status (str): Current index state. SQL++ defines 7 canonical values:
          online, deferred, building, pending, offline, abridged, scheduled for creation.
          On the REST path, unknown/future statuses are returned lowercased for forward-compat.
        - isPrimary (bool): Whether this is a primary index
        - bucket (str): Bucket name where the index exists
        - scope (str): Scope name where the index exists
        - collection (str): Collection name where the index exists
        - lastScanTime (str): Last time the index was scanned

        If a row is missing a required field (i.e. there's a problem in
        fetching the index information), the entry instead contains:
        - error (str): Human-readable description of what could not be processed
        - raw_index_stats (dict): The unprocessed raw row from the source
    """
    try:
        # Validate parameters
        validate_filter_params(bucket_name, scope_name, collection_name, index_name)

        # Get and validate connection settings
        settings = get_settings(ctx)
        validate_connection_settings(settings)

        # Decide which path to use based on cluster version (via SDK).
        cluster = await get_cluster_connection(ctx)
        major_version = await resolve_cluster_major_version(cluster)

        if major_version >= QUERY_SERVICE_LIST_INDEXES_MIN_MAJOR_VERSION:
            logger.info(
                f"Fetching indexes via query service (system:indexes) for "
                f"bucket={bucket_name}, scope={scope_name}, "
                f"collection={collection_name}, index={index_name}"
            )
            raw_indexes = await fetch_indexes_via_query_service(
                ctx,
                bucket_name=bucket_name,
                scope_name=scope_name,
                collection_name=collection_name,
                index_name=index_name,
                return_raw_index_stats=return_raw_index_stats,
            )
            indexes = [
                process_index_data_from_query(idx, return_raw_index_stats)
                for idx in raw_indexes
            ]
            logger.info(f"Found {len(indexes)} indexes via query service")
            return indexes

        # Fallback / pre-8.x path: Index Service REST API
        logger.info(
            f"Fetching indexes from Index Service REST API for "
            f"bucket={bucket_name}, scope={scope_name}, "
            f"collection={collection_name}, index={index_name}"
        )
        raw_indexes = await fetch_indexes_from_rest_api(
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
            process_index_data_from_rest_api(idx, return_raw_index_stats)
            for idx in raw_indexes
        ]

        logger.info(f"Found {len(indexes)} indexes from REST API")
        return indexes

    except Exception as e:
        logger.error(f"Error listing indexes: {e}", exc_info=True)
        raise
