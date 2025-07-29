"""
Couchbase MCP Tools

This module contains all the MCP tools for Couchbase operations.
"""

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
    get_scopes_and_collections_in_bucket,
    get_server_configuration_status,
    test_cluster_connection,
)

# List of all tools for easy registration
ALL_TOOLS = [
    get_server_configuration_status,
    test_cluster_connection,
    get_scopes_and_collections_in_bucket,
    get_document_by_id,
    upsert_document_by_id,
    delete_document_by_id,
    get_schema_for_collection,
    run_sql_plus_plus_query,
]

__all__ = [
    # Individual tools
    "get_server_configuration_status",
    "test_cluster_connection",
    "get_scopes_and_collections_in_bucket",
    "get_document_by_id",
    "upsert_document_by_id",
    "delete_document_by_id",
    "get_schema_for_collection",
    "run_sql_plus_plus_query",
    # Convenience
    "ALL_TOOLS",
]
