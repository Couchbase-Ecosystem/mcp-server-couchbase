"""
High-level integration tests for the Couchbase MCP server.

These tests mirror the workflow from the Real Python MCP client tutorial
and validate that:
- The expected tools are exposed by the MCP server
- Tools can be invoked against a demo Couchbase cluster
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


def _extract_payload(response: Any) -> Any:
    """Extract a usable payload from a tool response."""
    content = getattr(response, "content", None) or []
    if not content:
        return None

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


@pytest.mark.asyncio
async def test_tools_are_registered() -> None:
    """Ensure all expected tools are exposed by the server."""
    async with create_mcp_session() as session:
        tools_response = await session.list_tools()
        tool_names = {tool.name for tool in tools_response.tools}
        missing = EXPECTED_TOOLS - tool_names
        assert not missing, f"Missing MCP tools: {sorted(missing)}"


@pytest.mark.asyncio
async def test_cluster_connection_tool_invocation() -> None:
    """Verify the cluster connectivity tool executes against the demo cluster."""
    async with create_mcp_session() as session:
        bucket = os.getenv("CB_MCP_TEST_BUCKET")
        arguments: dict[str, str] = {"bucket_name": bucket} if bucket else {}

        response = await session.call_tool(
            "test_cluster_connection", arguments=arguments
        )
        payload = _extract_payload(response)

        assert payload, "No data returned from test_cluster_connection"
        if isinstance(payload, dict):
            assert payload.get("status") == "success", payload
            if bucket:
                assert payload.get("bucket_name") == bucket


@pytest.mark.asyncio
async def test_can_list_buckets() -> None:
    """Call a data-returning tool to ensure the session is usable."""
    async with create_mcp_session() as session:
        response = await session.call_tool("get_buckets_in_cluster", arguments={})
        payload = _extract_payload(response)

        assert payload is not None, "No payload returned from get_buckets_in_cluster"
        # If the demo cluster has buckets, we should see them; otherwise we at least
        # confirm the tool executed without errors.
        if isinstance(payload, list):
            assert payload, "Expected at least one bucket from the demo cluster"
