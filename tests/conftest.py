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

    # Try to get text from the first content block
    first = content[0]
    raw = getattr(first, "text", None)
    if raw is None and hasattr(first, "data"):
        raw = first.data

    # If first block is valid JSON, return it (handles dicts and JSON-encoded lists)
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

    # If first block is not valid JSON, collect all content blocks into a list.
    # This handles list returns where each item is a separate content block
    # (including single-item lists).
    items = []
    for block in content:
        text = getattr(block, "text", None)
        if text is not None:
            # Try to parse each item as JSON, fall back to raw string
            try:
                items.append(json.loads(text))
            except json.JSONDecodeError:
                items.append(text)
    return items if items else raw


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
