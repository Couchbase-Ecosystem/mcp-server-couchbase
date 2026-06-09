"""Unit tests for get_server_configuration_status tool payload."""

from __future__ import annotations

from types import SimpleNamespace

from cb_mcp.tools.server import get_server_configuration_status


def _make_ctx(settings=None, cluster_provider=None, logging_config=None):
    return SimpleNamespace(
        request_context=SimpleNamespace(
            lifespan_context=SimpleNamespace(
                cluster_provider=cluster_provider,
                settings=settings if settings is not None else {},
                logging_config=logging_config,
            )
        )
    )


def test_configuration_status_exposes_tool_lists():
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

    payload = get_server_configuration_status(ctx)
    config = payload["configuration"]

    assert config["disabled_tools"] == ["a_tool", "z_tool"]
    assert config["confirmation_required_tools"] == [
        "delete_document_by_id",
        "replace_document_by_id",
    ]


def test_configuration_status_defaults_tool_lists_to_empty():
    payload = get_server_configuration_status(_make_ctx())
    config = payload["configuration"]

    assert config["disabled_tools"] == []
    assert config["confirmation_required_tools"] == []


def test_logging_block_passed_through_from_lifespan_context():
    """The tool surfaces whatever shape AppContext.logging_config carries.

    The tool itself has no dependency on the logging module — it just reads
    the dict the host server entrypoint placed on the lifespan context. This
    keeps the tool reusable across MCP server implementations that may use
    different logging stacks.
    """
    logging_snapshot = {
        "level": "DEBUG",
        "sinks": ["file", "stderr"],
        "log_file": "/var/log/mcp.log",
        "error_log_file": "/var/log/mcp.error.log",
        "max_bytes": 1048576,
        "backup_count": 5,
    }
    payload = get_server_configuration_status(
        _make_ctx(logging_config=logging_snapshot)
    )
    assert payload["logging"] == logging_snapshot


def test_logging_block_is_none_when_lifespan_omits_it():
    """A host server that doesn't populate logging_config gets a clean ``None``.

    Decoupling check: a third-party implementation using a different logging
    stack can leave AppContext.logging_config unset, and the tool degrades to
    ``"logging": null`` without raising.
    """
    payload = get_server_configuration_status(_make_ctx())
    assert payload["logging"] is None


def test_logging_block_alongside_existing_configuration_keys():
    """The new logging block is a peer of configuration/connections, not nested."""
    payload = get_server_configuration_status(
        _make_ctx(
            settings={"read_only_mode": True},
            logging_config={"level": "INFO", "sinks": ["stderr"]},
        )
    )
    assert "logging" in payload
    assert "configuration" in payload
    assert "connections" in payload
    # logging is NOT inside configuration — it's a top-level peer.
    assert "logging" not in payload["configuration"]
