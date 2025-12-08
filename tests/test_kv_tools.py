"""
Integration tests for kv.py tools.

Tests for:
- get_document_by_id
- upsert_document_by_id
- delete_document_by_id
"""

from __future__ import annotations

import uuid

import pytest
from conftest import (
    create_mcp_session,
    extract_payload,
    get_test_collection,
    get_test_scope,
    require_test_bucket,
)


@pytest.mark.asyncio
async def test_upsert_document_by_id() -> None:
    """Verify upsert_document_by_id can create a new document."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()

    # Generate a unique document ID for this test
    doc_id = f"test_doc_{uuid.uuid4().hex[:8]}"
    doc_content = {"name": "Test Document", "type": "test", "value": 42}

    async with create_mcp_session() as session:
        response = await session.call_tool(
            "upsert_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
                "document_content": doc_content,
            },
        )
        payload = extract_payload(response)

        # upsert returns True on success
        assert payload is True, f"Expected True on upsert success, got {payload}"

        # Clean up: delete the test document
        await session.call_tool(
            "delete_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
            },
        )


@pytest.mark.asyncio
async def test_get_document_by_id() -> None:
    """Verify get_document_by_id can retrieve a document."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()

    # Create a test document first
    doc_id = f"test_doc_{uuid.uuid4().hex[:8]}"
    doc_content = {"name": "Test Get Document", "type": "test", "value": 123}

    async with create_mcp_session() as session:
        # Upsert the document
        await session.call_tool(
            "upsert_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
                "document_content": doc_content,
            },
        )

        # Now retrieve it
        response = await session.call_tool(
            "get_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
            },
        )
        payload = extract_payload(response)

        assert isinstance(payload, dict), f"Expected dict, got {type(payload)}"
        assert payload.get("name") == "Test Get Document"
        assert payload.get("type") == "test"
        assert payload.get("value") == 123

        # Clean up
        await session.call_tool(
            "delete_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
            },
        )


@pytest.mark.asyncio
async def test_delete_document_by_id() -> None:
    """Verify delete_document_by_id can remove a document."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()

    # Create a test document first
    doc_id = f"test_doc_{uuid.uuid4().hex[:8]}"
    doc_content = {"name": "Test Delete Document", "type": "test"}

    async with create_mcp_session() as session:
        # Upsert the document
        await session.call_tool(
            "upsert_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
                "document_content": doc_content,
            },
        )

        # Delete it
        response = await session.call_tool(
            "delete_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
            },
        )
        payload = extract_payload(response)

        # delete returns True on success
        assert payload is True, f"Expected True on delete success, got {payload}"


@pytest.mark.asyncio
async def test_upsert_and_update_document() -> None:
    """Verify upsert_document_by_id can update an existing document."""
    bucket = require_test_bucket()
    scope = get_test_scope()
    collection = get_test_collection()

    doc_id = f"test_doc_{uuid.uuid4().hex[:8]}"
    original_content = {"name": "Original", "version": 1}
    updated_content = {"name": "Updated", "version": 2, "extra_field": "new"}

    async with create_mcp_session() as session:
        # Create original document
        await session.call_tool(
            "upsert_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
                "document_content": original_content,
            },
        )

        # Update the document
        await session.call_tool(
            "upsert_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
                "document_content": updated_content,
            },
        )

        # Verify the update
        response = await session.call_tool(
            "get_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
            },
        )
        payload = extract_payload(response)

        assert payload.get("name") == "Updated"
        assert payload.get("version") == 2
        assert payload.get("extra_field") == "new"

        # Clean up
        await session.call_tool(
            "delete_document_by_id",
            arguments={
                "bucket_name": bucket,
                "scope_name": scope,
                "collection_name": collection,
                "document_id": doc_id,
            },
        )
