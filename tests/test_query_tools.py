"""
Integration tests for query.py tools.

Tests for:
- get_schema_for_collection
- run_sql_plus_plus_query
"""

from __future__ import annotations

import pytest
from conftest import (
    create_mcp_session,
    ensure_list,
    extract_payload,
    get_test_collection,
    get_test_scope,
    require_test_bucket,
)


@pytest.mark.asyncio
async def test_get_schema_for_collection() -> None:
    """Verify get_schema_for_collection returns schema information."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "get_schema_for_collection",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
            },
        )
        payload = extract_payload(response)

        assert isinstance(payload, dict), f"Expected dict, got {type(payload)}"
        assert "collection_name" in payload
        assert payload["collection_name"] == collection
        assert "schema" in payload
        # Schema is a list (may be empty if collection has no documents)
        assert isinstance(payload["schema"], list)


@pytest.mark.asyncio
async def test_run_sql_plus_plus_query_select() -> None:
    """Verify run_sql_plus_plus_query can execute a SELECT query."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()

    # Simple query to count documents (works even on empty collection)
    query = f"SELECT COUNT(*) as doc_count FROM `{collection}`"

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "run_sql_plus_plus_query",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "query": query,
            },
        )
        payload = ensure_list(extract_payload(response))

        assert isinstance(payload, list), f"Expected list, got {type(payload)}"
        # Query should return at least one row
        assert len(payload) >= 1
        # First row should have doc_count field
        assert "doc_count" in payload[0]


@pytest.mark.asyncio
async def test_run_sql_plus_plus_query_with_limit() -> None:
    """Verify run_sql_plus_plus_query respects LIMIT clause."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()

    query = f"SELECT * FROM `{collection}` LIMIT 5"

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "run_sql_plus_plus_query",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "query": query,
            },
        )
        payload = ensure_list(extract_payload(response))

        assert isinstance(payload, list), f"Expected list, got {type(payload)}"
        # Should return at most 5 documents
        assert len(payload) <= 5


@pytest.mark.asyncio
async def test_run_sql_plus_plus_query_meta() -> None:
    """Verify run_sql_plus_plus_query can retrieve document metadata."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()

    # Query to get document IDs using META()
    query = f"SELECT META().id as doc_id FROM `{collection}` LIMIT 1"

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "run_sql_plus_plus_query",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "query": query,
            },
        )
        payload = ensure_list(extract_payload(response))

        assert isinstance(payload, list), f"Expected list, got {type(payload)}"
        # If collection has documents, should return doc_id
        if payload:
            assert "doc_id" in payload[0]
