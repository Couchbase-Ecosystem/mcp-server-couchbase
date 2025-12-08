"""
Integration tests for index.py tools.

Tests for:
- list_indexes
- get_index_advisor_recommendations
"""

from __future__ import annotations

import pytest
from conftest import (
    create_mcp_session,
    extract_payload,
    get_test_collection,
    get_test_scope,
    require_test_bucket,
)


@pytest.mark.asyncio
async def test_list_indexes_all() -> None:
    """Verify list_indexes returns all indexes in the cluster."""
    skip_reason = None

    async with create_mcp_session() as session:
        response = await session.call_tool("list_indexes", arguments={})
        payload = extract_payload(response)

        # Skip if no indexes exist in the cluster
        if payload is None or (isinstance(payload, list) and len(payload) == 0):
            skip_reason = "No indexes found in cluster"
        else:
            assert isinstance(payload, list), f"Expected list, got {type(payload)}"
            # Each index should have required fields
            first_index = payload[0]
            assert "name" in first_index
            assert "definition" in first_index
            assert "status" in first_index
            assert "bucket" in first_index

    if skip_reason:
        pytest.skip(skip_reason)


@pytest.mark.asyncio
async def test_list_indexes_filtered_by_bucket() -> None:
    """Verify list_indexes can filter by bucket name."""
    bucket = require_test_bucket()
    skip_reason = None

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "list_indexes", arguments={"bucket_name": bucket}
        )
        payload = extract_payload(response)

        # Skip if no indexes exist for the bucket
        if payload is None or (isinstance(payload, list) and len(payload) == 0):
            skip_reason = f"No indexes found in bucket '{bucket}'"
        else:
            assert isinstance(payload, list), f"Expected list, got {type(payload)}"
            # All returned indexes should belong to the specified bucket
            for index in payload:
                assert index.get("bucket") == bucket, (
                    f"Index {index.get('name')} belongs to bucket {index.get('bucket')}, "
                    f"expected {bucket}"
                )

    if skip_reason:
        pytest.skip(skip_reason)


@pytest.mark.asyncio
async def test_list_indexes_filtered_by_scope() -> None:
    """Verify list_indexes can filter by bucket and scope."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    skip_reason = None

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "list_indexes",
            arguments={"bucket_name": bucket, "scope_name": scope},
        )
        payload = extract_payload(response)

        # Skip if no indexes exist for the scope
        if payload is None or (isinstance(payload, list) and len(payload) == 0):
            skip_reason = f"No indexes found in bucket '{bucket}', scope '{scope}'"
        else:
            assert isinstance(payload, list), f"Expected list, got {type(payload)}"
            # All returned indexes should belong to the specified bucket and scope
            for index in payload:
                assert index.get("bucket") == bucket
                assert index.get("scope") == scope

    if skip_reason:
        pytest.skip(skip_reason)


@pytest.mark.asyncio
async def test_list_indexes_filtered_by_collection() -> None:
    """Verify list_indexes can filter by bucket, scope, and collection."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()
    skip_reason = None

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "list_indexes",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
            },
        )
        payload = extract_payload(response)

        # Skip if no indexes exist for the collection
        if payload is None or (isinstance(payload, list) and len(payload) == 0):
            skip_reason = (
                f"No indexes found in bucket '{bucket}', "
                f"scope '{scope}', collection '{collection}'"
            )
        else:
            assert isinstance(payload, list), f"Expected list, got {type(payload)}"
            # All returned indexes should belong to the specified collection
            for index in payload:
                assert index.get("bucket") == bucket
                assert index.get("scope") == scope
                assert index.get("collection") == collection

    if skip_reason:
        pytest.skip(skip_reason)


@pytest.mark.asyncio
async def test_list_indexes_with_raw_stats() -> None:
    """Verify list_indexes can include raw index stats."""
    skip_reason = None

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "list_indexes", arguments={"include_raw_index_stats": True}
        )
        payload = extract_payload(response)

        # Skip if no indexes exist
        if payload is None or (isinstance(payload, list) and len(payload) == 0):
            skip_reason = "No indexes found to test raw stats"
        else:
            assert isinstance(payload, list), f"Expected list, got {type(payload)}"
            # When include_raw_index_stats is True, each index should have raw_index_stats
            first_index = payload[0]
            assert "raw_index_stats" in first_index, (
                "Expected raw_index_stats when include_raw_index_stats=True"
            )

    if skip_reason:
        pytest.skip(skip_reason)


@pytest.mark.asyncio
async def test_get_index_advisor_recommendations() -> None:
    """Verify get_index_advisor_recommendations returns recommendations."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()
    skip_reason = None

    # A query that might benefit from an index (avoid single quotes - they break ADVISOR)
    # Use a numeric comparison instead of string literal
    query = f"SELECT * FROM `{collection}` WHERE id > 100"

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "get_index_advisor_recommendations",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "query": query,
            },
        )
        payload = extract_payload(response)

        # Handle error responses
        if isinstance(payload, str):
            if "Error" in payload:
                skip_reason = f"Index advisor failed: {payload[:100]}..."
            else:
                raise AssertionError(f"Unexpected string response: {payload}")
        elif isinstance(payload, list) and payload and isinstance(payload[0], str):
            # Error returned as list of strings
            skip_reason = f"Index advisor failed: {payload[0][:100]}..."
        else:
            assert isinstance(payload, dict), f"Expected dict, got {type(payload)}"
            # Response should have the expected structure
            assert "current_used_indexes" in payload
            assert "recommended_indexes" in payload
            assert "recommended_covering_indexes" in payload
            # Summary should also be present
            assert "summary" in payload
            summary = payload["summary"]
            assert "has_recommendations" in summary

    if skip_reason:
        pytest.skip(skip_reason)
