"""
Couchbase MCP Tools

This module contains all the MCP tools for Couchbase operations.
"""

# Catalog tools
from .catalog import (
    get_catalog_status,
    get_collection_schema_from_catalog,
    get_enriched_database_context,
    refresh_collection_schema,
)

# Index tools
from .index import get_index_advisor_recommendations, list_indexes

# Key-Value tools
from .kv import (
    delete_document_by_id,
    get_document_by_id,
    upsert_document_by_id,
)

# Query tools
from .query import (
    get_schema_for_collection,
    run_sql_plus_plus_query,
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

# List of all tools for easy registration
ALL_TOOLS = [
    get_buckets_in_cluster,
    get_server_configuration_status,
    test_cluster_connection,
    get_scopes_and_collections_in_bucket,
    get_collections_in_scope,
    get_scopes_in_bucket,
    get_document_by_id,
    upsert_document_by_id,
    delete_document_by_id,
    get_schema_for_collection,
    run_sql_plus_plus_query,
    get_index_advisor_recommendations,
    list_indexes,
    get_cluster_health_and_services,
    # Catalog tools
    get_catalog_status,
    get_collection_schema_from_catalog,
    get_enriched_database_context,
    refresh_collection_schema,
]

__all__ = [
    # Server tools
    "get_server_configuration_status",
    "test_cluster_connection",
    "get_scopes_and_collections_in_bucket",
    "get_collections_in_scope",
    "get_scopes_in_bucket",
    "get_buckets_in_cluster",
    "get_cluster_health_and_services",
    # KV tools
    "get_document_by_id",
    "upsert_document_by_id",
    "delete_document_by_id",
    # Query tools
    "get_schema_for_collection",
    "run_sql_plus_plus_query",
    # Index tools
    "get_index_advisor_recommendations",
    "list_indexes",
    # Catalog tools
    "get_catalog_status",
    "get_collection_schema_from_catalog",
    "get_enriched_database_context",
    "refresh_collection_schema",
    # Convenience
    "ALL_TOOLS",
]
