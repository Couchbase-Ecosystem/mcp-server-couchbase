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
from typing import TYPE_CHECKING, Any

import pytest
from _test_env import (
    REQUIRED_ENV_VARS,
    _build_env,
    get_test_bucket,
    get_test_collection,
    get_test_scope,
    require_test_bucket,
)
from mcp import ClientSession, StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client

if TYPE_CHECKING:
    from typing import TextIO

__all__ = [
    "EXPECTED_TOOLS",
    "TOOLS_BY_CATEGORY",
    "TOOL_REQUIRED_PARAMS",
    "_build_env",
    "create_logging_test_session",
    "create_mcp_session",
    "create_stdio_session",
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
async def create_mcp_session() -> AsyncIterator[ClientSession]:
    """Create an MCP client session, transport chosen by the environment.

    When ``MCP_SERVER_URL`` is set (the http-transport CI leg exports it
    pointing at the standing server), connect over streamable-http and
    reuse that shared server. Otherwise spawn a fresh stdio subprocess.

    Tests that need a fresh server *process* per test — to apply env-var
    overrides like ``CB_MCP_READ_ONLY_MODE`` or ``CB_MCP_DISABLED_TOOLS``
    — must use :func:`create_stdio_session` instead, which always spawns
    and accepts ``env_overrides``. Per-test env can't be applied to the
    shared HTTP server.
    """
    server_url = os.getenv("MCP_SERVER_URL")
    if server_url:
        async with asyncio.timeout(DEFAULT_TIMEOUT):
            async with streamable_http_client(server_url) as (
                read_stream,
                write_stream,
                _,
            ):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    yield session
        return

    async with create_stdio_session() as session:
        yield session


@asynccontextmanager
async def create_stdio_session(
    env_overrides: dict[str, str] | None = None,
) -> AsyncIterator[ClientSession]:
    """Spawn a fresh stdio MCP server subprocess and connect to it.

    Use this when a test needs server-process-private state — typically
    because it sets ``CB_MCP_READ_ONLY_MODE``, ``CB_MCP_DISABLED_TOOLS``,
    or another env var that's read at server startup. These can't be
    applied to a shared HTTP server, so this helper bypasses transport
    routing and always spawns.

    ``env_overrides`` are merged onto the spawned process's environment
    after the standard ``_build_env()`` setup.
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


@asynccontextmanager
async def create_logging_test_session(
    extra_args: list[str] | None = None,
    env_overrides: dict[str, str] | None = None,
    cwd: Path | None = None,
    stderr_buffer: TextIO | None = None,
) -> AsyncIterator[ClientSession]:
    """Spawn the MCP server for CLI / logging tests; no cluster credentials.

    Cluster credentials are deliberately stripped from the inherited environment
    so the server boots in "no cluster" lazy mode (which is fine — tools that
    don't touch the cluster, like ``get_server_configuration_status``, work
    without connectivity). Use this helper for tests that exercise CLI flags,
    env-var routing, or filesystem effects of logging — not for tests that
    need to call cluster-touching tools.

    Optional arguments:
      - ``extra_args``: extra CLI flags appended after ``python -m mcp_server``.
      - ``env_overrides``: merged onto the server's environment after credential
        stripping. Use to set ``CB_MCP_LOG_LEVEL`` and friends.
      - ``cwd``: working directory for the spawned process. Set to a
        ``tmp_path`` when verifying default CWD-relative file paths.
      - ``stderr_buffer``: a writable file object backed by a real file
        descriptor (e.g. ``tmp_path / "server.stderr"`` opened in ``"w"``
        mode). The MCP SDK passes this straight to ``asyncio.subprocess``,
        which requires ``.fileno()`` — ``io.StringIO`` will not work.
        Read the captured stderr back from the same path after the session
        closes.
    """
    env = os.environ.copy()
    # Strip credentials so the server starts in lazy mode without skipping.
    for var in REQUIRED_ENV_VARS:
        env.pop(var, None)
    env["PYTHONUNBUFFERED"] = "1"
    # Match _build_env(): always spawn the subprocess in stdio mode so the
    # same test suite runs unchanged under the http-transport CI job (which
    # exports CB_MCP_TRANSPORT=http and keeps a standing server on :8000).
    env["CB_MCP_TRANSPORT"] = "stdio"
    env.pop("MCP_TRANSPORT", None)
    if env_overrides:
        env.update(env_overrides)

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mcp_server", *(extra_args or [])],
        env=env,
        cwd=str(cwd) if cwd is not None else None,
    )

    client_kwargs: dict[str, Any] = {}
    if stderr_buffer is not None:
        client_kwargs["errlog"] = stderr_buffer

    async with asyncio.timeout(DEFAULT_TIMEOUT):
        async with stdio_client(params, **client_kwargs) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                yield session
