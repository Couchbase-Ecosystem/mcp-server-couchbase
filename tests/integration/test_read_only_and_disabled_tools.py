"""
End-to-end integration tests for read-only mode and tool disabling.

These tests spawn the real MCP server over stdio with specific environment
overrides (``CB_MCP_READ_ONLY_MODE`` / ``CB_MCP_DISABLED_TOOLS``) and verify
that the resulting tool registry — exposed to MCP clients — reflects the
configuration. Unit tests in ``tests/unit/`` cover the underlying helpers
(``get_tools``, ``prepare_tools_for_registration``, ``parse_tool_names``);
these tests verify they're wired correctly through the server entry point.
"""

from __future__ import annotations

import pytest
from conftest import create_stdio_session, extract_payload

KV_WRITE_TOOLS = {
    "upsert_document_by_id",
    "insert_document_by_id",
    "replace_document_by_id",
    "delete_document_by_id",
}

ALWAYS_AVAILABLE_TOOLS = {
    "get_document_by_id",
    "get_buckets_in_cluster",
    "get_server_configuration_status",
    "run_sql_plus_plus_query",
    "explain_sql_plus_plus_query",
    "list_indexes",
}


@pytest.mark.asyncio
async def test_read_only_mode_filters_kv_write_tools() -> None:
    """READ_ONLY_MODE=true must hide every KV write tool from the registry."""
    async with create_stdio_session(
        env_overrides={"CB_MCP_READ_ONLY_MODE": "true"}
    ) as session:
        tools_response = await session.list_tools()
        tool_names = {tool.name for tool in tools_response.tools}

        leaked = KV_WRITE_TOOLS & tool_names
        assert not leaked, (
            f"KV write tools leaked into read-only registry: {sorted(leaked)}"
        )

        missing_reads = ALWAYS_AVAILABLE_TOOLS - tool_names
        assert not missing_reads, (
            f"Read tools missing in read-only mode: {sorted(missing_reads)}"
        )


@pytest.mark.asyncio
async def test_read_only_mode_reported_in_configuration_status() -> None:
    """get_server_configuration_status must reflect read-only mode."""
    async with create_stdio_session(
        env_overrides={"CB_MCP_READ_ONLY_MODE": "true"}
    ) as session:
        response = await session.call_tool(
            "get_server_configuration_status", arguments={}
        )
        payload = extract_payload(response)

        assert isinstance(payload, dict), f"Expected dict, got {type(payload)}"
        config = payload.get("configuration", {})
        assert config.get("read_only_mode") is True, (
            f"Expected read_only_mode=True in config, got: {config}"
        )


@pytest.mark.asyncio
async def test_write_mode_exposes_kv_write_tools() -> None:
    """READ_ONLY_MODE=false must expose every KV write tool."""
    async with create_stdio_session(
        env_overrides={"CB_MCP_READ_ONLY_MODE": "false"}
    ) as session:
        tools_response = await session.list_tools()
        tool_names = {tool.name for tool in tools_response.tools}

        missing = KV_WRITE_TOOLS - tool_names
        assert not missing, f"KV write tools missing in write mode: {sorted(missing)}"


@pytest.mark.asyncio
async def test_disabled_tools_excluded_from_registry() -> None:
    """Tools named in CB_MCP_DISABLED_TOOLS must not be registered."""
    disabled = "list_indexes,get_buckets_in_cluster"

    async with create_stdio_session(
        env_overrides={"CB_MCP_DISABLED_TOOLS": disabled}
    ) as session:
        tools_response = await session.list_tools()
        tool_names = {tool.name for tool in tools_response.tools}

        assert "list_indexes" not in tool_names, (
            "list_indexes should be disabled but was registered"
        )
        assert "get_buckets_in_cluster" not in tool_names, (
            "get_buckets_in_cluster should be disabled but was registered"
        )

        assert "get_document_by_id" in tool_names, (
            "get_document_by_id must remain when not in the disabled list"
        )
        assert "run_sql_plus_plus_query" in tool_names, (
            "run_sql_plus_plus_query must remain when not in the disabled list"
        )


@pytest.mark.asyncio
async def test_disabled_tools_reported_in_configuration_status() -> None:
    """get_server_configuration_status must surface the disabled-tools list."""
    disabled = "list_indexes,get_buckets_in_cluster"

    async with create_stdio_session(
        env_overrides={"CB_MCP_DISABLED_TOOLS": disabled}
    ) as session:
        response = await session.call_tool(
            "get_server_configuration_status", arguments={}
        )
        payload = extract_payload(response)

        assert isinstance(payload, dict), f"Expected dict, got {type(payload)}"
        config = payload.get("configuration", {})
        reported = set(config.get("disabled_tools", []))

        assert {"list_indexes", "get_buckets_in_cluster"} <= reported, (
            f"Expected disabled tools to be reported, got: {reported}"
        )


@pytest.mark.asyncio
async def test_disabled_tools_invalid_names_ignored() -> None:
    """Unknown tool names in CB_MCP_DISABLED_TOOLS must be silently ignored."""
    async with create_stdio_session(
        env_overrides={
            "CB_MCP_DISABLED_TOOLS": "list_indexes,not_a_real_tool,definitely_fake"
        }
    ) as session:
        tools_response = await session.list_tools()
        tool_names = {tool.name for tool in tools_response.tools}

        assert "list_indexes" not in tool_names
        # Server must still be healthy and other tools registered.
        assert "get_document_by_id" in tool_names
        assert "get_server_configuration_status" in tool_names


@pytest.mark.asyncio
async def test_calling_disabled_tool_fails() -> None:
    """A disabled tool must not be callable via the MCP session."""
    async with create_stdio_session(
        env_overrides={"CB_MCP_DISABLED_TOOLS": "get_buckets_in_cluster"}
    ) as session:
        response = await session.call_tool("get_buckets_in_cluster", arguments={})

        assert getattr(response, "isError", False), (
            f"Calling a disabled tool should produce an error response, got: {response}"
        )


@pytest.mark.asyncio
async def test_read_only_mode_blocks_calling_write_tools() -> None:
    """A KV write tool must not be invokable when read-only mode is on."""
    async with create_stdio_session(
        env_overrides={"CB_MCP_READ_ONLY_MODE": "true"}
    ) as session:
        response = await session.call_tool(
            "delete_document_by_id",
            arguments={
                "bucket_name": "dummy",
                "scope_name": "_default",
                "collection_name": "_default",
                "document_id": "nonexistent_id",
            },
        )

        assert getattr(response, "isError", False), (
            "Calling a write tool in read-only mode should produce an "
            f"error response, got: {response}"
        )
