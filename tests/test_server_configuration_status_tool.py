"""Unit tests for get_server_configuration_status tool payload."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tools.server import get_server_configuration_status


def _make_ctx(settings=None, cluster_provider=None):
    return SimpleNamespace(
        request_context=SimpleNamespace(
            lifespan_context=SimpleNamespace(
                cluster_provider=cluster_provider,
                settings=settings if settings is not None else {},
            )
        )
    )


@pytest.mark.asyncio
async def test_configuration_status_exposes_tool_lists():
    ctx = _make_ctx(
        {
            "connection_string": "couchbases://example",
            "username": "test-user",
            "read_only_mode": True,
            "read_only_query_mode": True,
            "disabled_tools": {"z_tool", "a_tool"},
            "confirmation_required_tools": {
                "delete_document_by_id",
                "replace_document_by_id",
            },
        }
    )

    payload = await get_server_configuration_status(ctx)
    config = payload["configuration"]

    assert config["disabled_tools"] == ["a_tool", "z_tool"]
    assert config["confirmation_required_tools"] == [
        "delete_document_by_id",
        "replace_document_by_id",
    ]


@pytest.mark.asyncio
async def test_configuration_status_defaults_tool_lists_to_empty():
    payload = await get_server_configuration_status(_make_ctx())
    config = payload["configuration"]

    assert config["disabled_tools"] == []
    assert config["confirmation_required_tools"] == []
