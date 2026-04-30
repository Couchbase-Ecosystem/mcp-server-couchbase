"""
Tools for index operations.

This module contains tools for listing and managing indexes in the Couchbase cluster and getting index recommendations using the Couchbase Index Advisor.
"""

import logging
from typing import Any

from fastmcp import Context

from ..utils.config import get_settings
from ..utils.constants import MCP_SERVER_NAME
from ..utils.context import get_cluster_connection
from ..utils.index_utils import (
    fetch_indexes_from_rest_api,
    parse_major_version,
    process_index_data,
    process_query_index_data,
    validate_connection_settings,
    validate_filter_params,
)
from .query import run_cluster_query, run_sql_plus_plus_query

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.index")

# Cluster major version at which list_indexes prefers the query service over
# the Index Service REST API (system:all_indexes returns full definitions
# starting from this version).
_QUERY_SERVICE_LIST_INDEXES_MIN_MAJOR_VERSION = 8


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


async def _fetch_indexes_via_query_service(
    ctx: Context,
    bucket_name: str | None,
    scope_name: str | None,
    collection_name: str | None,
    index_name: str | None,
) -> list[dict[str, Any]]:
    """Fetch indexes by querying ``system:all_indexes`` via the query service.
    Returns:
        List of unwrapped raw index rows from ``system:all_indexes``.
    """
    clauses: list[str] = ["`using` = 'gsi'"]
    params: dict[str, Any] = {}

    if bucket_name:
        clauses.append("bucket_id = $bucket_id")
        params["bucket_id"] = bucket_name
    if scope_name:
        clauses.append("scope_id = $scope_id")
        params["scope_id"] = scope_name
    if collection_name:
        clauses.append("keyspace_id = $keyspace_id")
        params["keyspace_id"] = collection_name
    if index_name:
        clauses.append("name = $index_name")
        params["index_name"] = index_name

    query = "SELECT RAW all_indexes FROM system:all_indexes WHERE " + " AND ".join(
        clauses
    )
    logger.info(f"Running list_indexes query: {query}")

    rows = await run_cluster_query(ctx, query, named_parameters=params)
    return [row for row in rows if isinstance(row, dict)]


async def _resolve_cluster_major_version(ctx: Context) -> int:
    """Detect the cluster's major version via the SDK.

    Reads the per-node ``version`` field from ``cluster.cluster_info().nodes``
    (Python SDK 4.1+) and returns the *minimum* major version across all nodes
    so we only enable the 8.x+ query-service path when every node supports it.

    The high-level helper properties (``server_version`` /
    ``server_version_short`` / ``server_version_full``) are intentionally not
    used: the SDK collapses them to ``None`` whenever the cluster reports
    mixed node versions, which is exactly the case where we still need an
    answer. Each node entry, in contrast, always carries a ``version`` string.

    Returns 0 (treated as "older than 8.x") if cluster_info() itself fails so
    that callers fall back to the existing Index Service REST API path.
    """
    try:
        cluster = await get_cluster_connection(ctx)
        info = await cluster.cluster_info()
    except Exception as e:
        logger.warning(
            f"Could not call cluster.cluster_info(), falling back to Index "
            f"Service REST API for list_indexes: {e}"
        )
        return 0

    nodes = info.nodes or []
    versions: list[str] = []
    for node in nodes:
        if isinstance(node, dict):
            version = node.get("version")
        else:
            version = getattr(node, "version", None)
        if version:
            versions.append(str(version))

    majors = [parse_major_version(v) for v in versions]
    min_major = min(majors) if majors else 0

    logger.info(f"Detected cluster node versions={versions} (min major={min_major})")
    return min_major


async def list_indexes(
    ctx: Context,
    bucket_name: str | None = None,
    scope_name: str | None = None,
    collection_name: str | None = None,
    index_name: str | None = None,
    include_raw_index_stats: bool = False,
) -> list[dict[str, Any]]:
    """List all indexes in the cluster with optional filtering by bucket, scope, collection, and index name.
    Returns a list of indexes with their names and CREATE INDEX definitions.

    The data source depends on the Couchbase Server version:
    - Cluster version >= 8.x: query ``system:all_indexes`` via the query
      service, which exposes the original CREATE INDEX statement directly in
      ``metadata.definition``.
    - Cluster version < 8.x (or version detection fails): fall back to the
      Index Service REST API ``/getIndexStatus`` endpoint.

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
        - status (str): Current status of the index
        - isPrimary (bool): Whether this is a primary index
        - bucket (str): Bucket name where the index exists
        - scope (str): Scope name where the index exists
        - collection (str): Collection name where the index exists
        - raw_index_stats (dict, optional): Complete raw index status object from the source
                                           used to fetch the index (only if include_raw_index_stats=True)
    """
    try:
        # Validate parameters
        validate_filter_params(bucket_name, scope_name, collection_name, index_name)

        # Get and validate connection settings
        settings = get_settings(ctx)
        validate_connection_settings(settings)

        # Decide which path to use based on cluster version (via SDK).
        major_version = await _resolve_cluster_major_version(ctx)

        if major_version >= _QUERY_SERVICE_LIST_INDEXES_MIN_MAJOR_VERSION:
            logger.info(
                f"Fetching indexes via query service (system:all_indexes) for "
                f"bucket={bucket_name}, scope={scope_name}, "
                f"collection={collection_name}, index={index_name}"
            )
            raw_indexes = await _fetch_indexes_via_query_service(
                ctx,
                bucket_name=bucket_name,
                scope_name=scope_name,
                collection_name=collection_name,
                index_name=index_name,
            )
            indexes = [
                processed
                for idx in raw_indexes
                if isinstance(idx, dict)
                and (
                    processed := process_query_index_data(idx, include_raw_index_stats)
                )
                is not None
            ]
            logger.info(
                f"Found {len(indexes)} indexes via query service "
                f"(include_raw_index_stats={include_raw_index_stats})"
            )
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
