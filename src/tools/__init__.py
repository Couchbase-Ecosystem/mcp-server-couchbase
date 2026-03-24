"""
Couchbase MCP Tools

This module contains all the MCP tools for Couchbase operations.

Tool Categories:
- READ_ONLY_TOOLS: Tools that only read data (always available)
- KV_WRITE_TOOLS: KV tools that modify data (disabled when READ_ONLY_MODE=True)
"""

from collections.abc import Callable

# Docs / API reference tools
from .docs import ask_couchbase_docs

# Index tools
from .index import get_index_advisor_recommendations, list_indexes

# Key-Value tools
from .kv import (
    delete_document_by_id,
    get_document_by_id,
    insert_document_by_id,
    replace_document_by_id,
    upsert_document_by_id,
)

# Query tools
from .query import (
    generate_or_modify_sql_plus_plus_query,
    get_longest_running_queries,
    get_most_frequent_queries,
    get_queries_not_selective,
    get_queries_not_using_covering_index,
    get_queries_using_primary_index,
    get_queries_with_large_result_count,
    get_queries_with_largest_response_sizes,
    get_schema_for_collection,
    run_sql_plus_plus_query,
    update_query_function_annotation,
)

# Server tools
from .server import (
    get_buckets_in_cluster,
    get_cluster_health_and_services,
    get_collections_in_scope,
    get_scopes_and_collections_in_bucket,
    get_scopes_in_bucket,
    get_server_configuration_status,
    test_cluster_connection,
)

# Read-only tools - always available regardless of mode settings
READ_ONLY_TOOLS = [
    # Server/Cluster management tools
    get_buckets_in_cluster,
    get_server_configuration_status,
    test_cluster_connection,
    get_scopes_and_collections_in_bucket,
    get_collections_in_scope,
    get_scopes_in_bucket,
    get_cluster_health_and_services,
    # KV read tool
    get_document_by_id,
    # Query tools (read operations)
    get_schema_for_collection,
    run_sql_plus_plus_query,  # Write protection handled at runtime via read_only_query_mode
    # Index tools
    get_index_advisor_recommendations,
    list_indexes,
    # Query performance analysis tools
    get_queries_not_selective,
    get_queries_not_using_covering_index,
    get_queries_using_primary_index,
    get_queries_with_large_result_count,
    get_queries_with_largest_response_sizes,
    get_longest_running_queries,
    get_most_frequent_queries,
    # Docs / API reference tools
    ask_couchbase_docs,
]

# Query generation tools - disabled by default, enabled via --enable-query-generation or CB_MCP_ENABLE_QUERY_GENERATION
QUERY_GENERATION_TOOLS = [
    generate_or_modify_sql_plus_plus_query,
]

# KV write tools - disabled when READ_ONLY_MODE is True
KV_WRITE_TOOLS = [
    upsert_document_by_id,
    insert_document_by_id,
    replace_document_by_id,
    delete_document_by_id,
]

# List of all tools for easy registration (kept for backward compatibility)
ALL_TOOLS = READ_ONLY_TOOLS + KV_WRITE_TOOLS + QUERY_GENERATION_TOOLS


def get_tools(
    read_only_mode: bool = True,
    enable_query_generation: bool = False,
) -> list[Callable]:
    """Get the list of tools based on the mode settings.

    This function determines which tools should be loaded based on the
    mode settings. When read_only_mode is True, write tools are excluded.
    When enable_query_generation is True, query generation tools are included.
    """
    # Update the query function annotation based on enable_query_generation flag
    update_query_function_annotation(enable_query_generation)

    tools = list(READ_ONLY_TOOLS)

    if not read_only_mode:
        # KV write tools are only loaded when READ_ONLY_MODE is False
        tools.extend(KV_WRITE_TOOLS)

    if enable_query_generation:
        tools.extend(QUERY_GENERATION_TOOLS)

    return tools


__all__ = [
    # Individual tools
    "get_server_configuration_status",
    "test_cluster_connection",
    "get_scopes_and_collections_in_bucket",
    "get_collections_in_scope",
    "get_scopes_in_bucket",
    "get_buckets_in_cluster",
    "get_document_by_id",
    "upsert_document_by_id",
    "insert_document_by_id",
    "replace_document_by_id",
    "delete_document_by_id",
    "get_schema_for_collection",
    "run_sql_plus_plus_query",
    "get_index_advisor_recommendations",
    "list_indexes",
    "get_cluster_health_and_services",
    "get_queries_not_selective",
    "get_queries_not_using_covering_index",
    "get_queries_using_primary_index",
    "get_queries_with_large_result_count",
    "get_queries_with_largest_response_sizes",
    "get_longest_running_queries",
    "get_most_frequent_queries",
    "generate_or_modify_sql_plus_plus_query",
    "ask_couchbase_docs",
    # Tool categories
    "READ_ONLY_TOOLS",
    "KV_WRITE_TOOLS",
    "QUERY_GENERATION_TOOLS",
    # Convenience
    "ALL_TOOLS",
    "get_tools",
]
