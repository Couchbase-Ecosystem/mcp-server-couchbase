"""
General MCP server integration tests.

Tests for tool registration, basic connectivity, and MCP protocol compliance.
"""

from __future__ import annotations

import pytest
from conftest import (
    EXPECTED_TOOLS,
    create_mcp_session,
    extract_payload,
    get_test_bucket,
)


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
        bucket = get_test_bucket()
        arguments: dict[str, str] = {"bucket_name": bucket} if bucket else {}

        response = await session.call_tool(
            "test_cluster_connection", arguments=arguments
        )
        payload = extract_payload(response)

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
        payload = extract_payload(response)

        assert payload is not None, "No payload returned from get_buckets_in_cluster"
        # If the demo cluster has buckets, we should see them; otherwise we at least
        # confirm the tool executed without errors.
        if isinstance(payload, list):
            assert payload, "Expected at least one bucket from the demo cluster"
