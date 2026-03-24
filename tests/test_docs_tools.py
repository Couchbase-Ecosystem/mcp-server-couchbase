"""
Integration tests for docs.py tools.

Tests for:
- ask_couchbase_docs
"""

from __future__ import annotations

import pytest
from conftest import (
    create_mcp_session,
    extract_payload,
)


@pytest.mark.asyncio
async def test_ask_couchbase_docs_returns_string() -> None:
    """Verify ask_couchbase_docs returns a non-empty string for a valid question."""
    async with create_mcp_session() as session:
        response = await session.call_tool(
            "ask_couchbase_docs",
            arguments={
                "question": "How to create a bucket in Couchbase?",
            },
        )
        payload = extract_payload(response)

        assert isinstance(payload, str), f"Expected str, got {type(payload)}"
        assert len(payload.strip()) > 0, "Expected a non-empty answer"
