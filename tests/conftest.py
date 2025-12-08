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
from mcp import ClientSession, StdioServerParameters, stdio_client

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

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
    "delete_document_by_id",
    "get_schema_for_collection",
    "run_sql_plus_plus_query",
    "get_index_advisor_recommendations",
    "list_indexes",
    "get_cluster_health_and_services",
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
        "delete_document_by_id",
    },
    "query": {
        "get_schema_for_collection",
        "run_sql_plus_plus_query",
    },
    "index": {
        "list_indexes",
        "get_index_advisor_recommendations",
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
    "get_schema_for_collection": ["bucket_name", "scope_name", "collection_name"],
    "run_sql_plus_plus_query": ["bucket_name", "scope_name", "query"],
    "get_index_advisor_recommendations": ["bucket_name", "scope_name", "query"],
}

# Minimum configuration needed to talk to a demo cluster
REQUIRED_ENV_VARS = ("CB_CONNECTION_STRING", "CB_USERNAME", "CB_PASSWORD")

# Default timeout (seconds) to guard against hangs when the Couchbase cluster
# is unreachable or slow. Override with CB_MCP_TEST_TIMEOUT if needed.
DEFAULT_TIMEOUT = int(os.getenv("CB_MCP_TEST_TIMEOUT", "120"))


def _build_env() -> dict[str, str]:
    """Build the environment passed to the test server process."""
    env = os.environ.copy()
    missing = [var for var in REQUIRED_ENV_VARS if not env.get(var)]
    if missing:
        pytest.skip(
            "Integration tests require demo cluster credentials. "
            f"Missing env vars: {', '.join(missing)}"
        )

    # Ensure the server module can be imported from the repo's src/ folder
    existing_path = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        f"{SRC_DIR}{os.pathsep}{existing_path}" if existing_path else str(SRC_DIR)
    )

    # Force stdio transport for the test server to match stdio_client
    env["CB_MCP_TRANSPORT"] = "stdio"
    # Ensure unbuffered output to avoid stdout/stderr buffering surprises
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


@asynccontextmanager
async def create_mcp_session() -> AsyncIterator[ClientSession]:
    """Create a fresh MCP client session connected to the server over stdio."""
    env = _build_env()
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


def get_test_bucket() -> str | None:
    """Get the test bucket name from environment, or None if not set."""
    return os.getenv("CB_MCP_TEST_BUCKET")


def get_test_scope() -> str:
    """Get the test scope name from environment, defaults to _default."""
    return os.getenv("CB_MCP_TEST_SCOPE", "_default")


def get_test_collection() -> str:
    """Get the test collection name from environment, defaults to _default."""
    return os.getenv("CB_MCP_TEST_COLLECTION", "_default")


def require_test_bucket() -> str:
    """Get the test bucket name, skipping test if not set."""
    bucket = get_test_bucket()
    if not bucket:
        pytest.skip("CB_MCP_TEST_BUCKET not set")
    return bucket


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
