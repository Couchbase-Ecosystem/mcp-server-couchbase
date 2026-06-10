"""
Shared fixtures and utilities for MCP server integration tests.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from _test_env import (
    _build_env,
    get_test_bucket,
    get_test_collection,
    get_test_scope,
    require_test_bucket,
)
from mcp import ClientSession, StdioServerParameters, stdio_client

__all__ = [
    "EXPECTED_TOOLS",
    "TOOLS_BY_CATEGORY",
    "TOOL_REQUIRED_PARAMS",
    "_build_env",
    "create_mcp_session",
    "ensure_list",
    "extract_payload",
    "get_test_bucket",
    "get_test_collection",
    "get_test_scope",
    "require_test_bucket",
]

_INTEGRATION_DIR = Path(__file__).resolve().parent


def pytest_collection_modifyitems(config, items):
    """Auto-tag every test in tests/integration/ with the `integration` marker.

    Lets users select / skip the whole tier with ``-m integration`` /
    ``-m "not integration"`` without decorating every test by hand.

    ``items`` is the full session list, not just tests under this conftest's
    directory, so we filter by path.
    """
    for item in items:
        try:
            item_path = Path(str(item.fspath)).resolve()
        except Exception:
            continue
        try:
            item_path.relative_to(_INTEGRATION_DIR)
        except ValueError:
            continue
        item.add_marker(pytest.mark.integration)


# Tools we expect to be registered by the server
EXPECTED_TOOLS = {
    "get_buckets_in_cluster",
    "get_server_configuration_status",
    "test_cluster_connection",
    "get_scopes_and_collections_in_bucket",
    "get_collections_in_scope",
    "get_scopes_in_bucket",
    "get_document_by_id",
    "upsert_document_by_id",
    "insert_document_by_id",
    "replace_document_by_id",
    "delete_document_by_id",
    "get_schema_for_collection",
    "run_sql_plus_plus_query",
    "explain_sql_plus_plus_query",
    "get_index_advisor_recommendations",
    "list_indexes",
    "get_cluster_health_and_services",
    # Performance analysis tools
    "get_longest_running_queries",
    "get_most_frequent_queries",
    "get_queries_with_largest_response_sizes",
    "get_queries_with_large_result_count",
    "get_queries_using_primary_index",
    "get_queries_not_using_covering_index",
    "get_queries_not_selective",
}

# Tools organized by category for validation
TOOLS_BY_CATEGORY = {
    "server": {
        "get_server_configuration_status",
        "test_cluster_connection",
        "get_buckets_in_cluster",
        "get_scopes_in_bucket",
        "get_scopes_and_collections_in_bucket",
        "get_collections_in_scope",
        "get_cluster_health_and_services",
    },
    "kv": {
        "get_document_by_id",
        "upsert_document_by_id",
        "insert_document_by_id",
        "replace_document_by_id",
        "delete_document_by_id",
    },
    "query": {
        "get_schema_for_collection",
        "run_sql_plus_plus_query",
        "explain_sql_plus_plus_query",
    },
    "index": {
        "list_indexes",
        "get_index_advisor_recommendations",
    },
    "performance": {
        "get_longest_running_queries",
        "get_most_frequent_queries",
        "get_queries_with_largest_response_sizes",
        "get_queries_with_large_result_count",
        "get_queries_using_primary_index",
        "get_queries_not_using_covering_index",
        "get_queries_not_selective",
    },
}

# Expected required parameters for tools that need them
TOOL_REQUIRED_PARAMS = {
    "get_scopes_in_bucket": ["bucket_name"],
    "get_scopes_and_collections_in_bucket": ["bucket_name"],
    "get_collections_in_scope": ["bucket_name", "scope_name"],
    "get_document_by_id": [
        "bucket_name",
        "scope_name",
        "collection_name",
        "document_id",
    ],
    "upsert_document_by_id": [
        "bucket_name",
        "scope_name",
        "collection_name",
        "document_id",
        "document_content",
    ],
    "delete_document_by_id": [
        "bucket_name",
        "scope_name",
        "collection_name",
        "document_id",
    ],
    "insert_document_by_id": [
        "bucket_name",
        "scope_name",
        "collection_name",
        "document_id",
        "document_content",
    ],
    "replace_document_by_id": [
        "bucket_name",
        "scope_name",
        "collection_name",
        "document_id",
        "document_content",
    ],
    "get_schema_for_collection": ["bucket_name", "scope_name", "collection_name"],
    "run_sql_plus_plus_query": ["bucket_name", "scope_name", "query"],
    "explain_sql_plus_plus_query": ["bucket_name", "scope_name", "query"],
    "get_index_advisor_recommendations": ["bucket_name", "scope_name", "query"],
}

# Default timeout (seconds) to guard against hangs when the Couchbase cluster
# is unreachable or slow. Override with CB_MCP_TEST_TIMEOUT if needed.
DEFAULT_TIMEOUT = int(os.getenv("CB_MCP_TEST_TIMEOUT", "120"))


@asynccontextmanager
async def create_mcp_session(
    env_overrides: dict[str, str] | None = None,
) -> AsyncIterator[ClientSession]:
    """Create a fresh MCP client session connected to the server over stdio.

    Optional ``env_overrides`` are merged onto the environment passed to the
    spawned server process, letting individual tests opt into things like
    ``CB_MCP_READ_ONLY_MODE`` or ``CB_MCP_DISABLED_TOOLS``.
    """
    env = _build_env()
    if env_overrides:
        env.update(env_overrides)
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mcp_server"],
        env=env,
    )

    async with asyncio.timeout(DEFAULT_TIMEOUT):
        async with stdio_client(params) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                yield session


def extract_payload(response: Any) -> Any:
    """Extract a usable payload from a tool response.

    MCP tool responses can return data in different formats:
    - A single content block with JSON-encoded data (dict, list, etc.)
    - Multiple content blocks, one per list item (for list returns)

    This function handles both cases.
    """
    content = getattr(response, "content", None) or []
    if not content:
        return None

    # If there are multiple content blocks, collect them all as a list
    # (each item in a list return may be a separate content block)
    if len(content) > 1:
        items = []
        for block in content:
            text = getattr(block, "text", None)
            if text is not None:
                try:
                    items.append(json.loads(text))
                except json.JSONDecodeError:
                    items.append(text)
        return items if items else None

    # Single content block - try to parse as JSON
    first = content[0]
    raw = getattr(first, "text", None)
    if raw is None and hasattr(first, "data"):
        raw = first.data

    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    return raw


def ensure_list(value: Any) -> list[Any]:
    """Ensure the value is a list.

    MCP can return single-item lists as just the item (not wrapped in a list).
    This helper wraps single non-list values in a list for consistent handling.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]
