"""End-to-end integration tests for the logging pipeline.

These tests spawn the real MCP server as a subprocess (via the shared
``create_logging_test_session`` helper) and observe the resulting filesystem
state and stderr stream to verify the wiring between Click, ``configure_logging``,
and the on-disk handlers. Unit tests in ``tests/unit/`` cover the individual
functions in isolation; these tests verify they're plumbed correctly through
the server entrypoint.

Cluster credentials are deliberately *not* required: the server boots fine in
lazy mode and the tools needed here (``get_server_configuration_status``) don't
touch the cluster. This keeps the logging integration tests runnable in stock
CI without secrets.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest
from conftest import create_logging_test_session, extract_payload


@pytest.mark.asyncio
async def test_default_file_sinks_create_both_files(tmp_path) -> None:
    """``--log-sinks file`` with no paths creates the two default files in CWD."""
    async with create_logging_test_session(
        extra_args=["--log-sinks", "file"],
        cwd=tmp_path,
    ):
        pass

    main = tmp_path / "mcp_server.log"
    err = tmp_path / "mcp_server.error.log"
    assert main.exists(), "default main log not created in CWD"
    assert err.exists(), "default error log not created in CWD"
    assert "Logging configured" in main.read_text()


@pytest.mark.asyncio
async def test_custom_log_file_paths_honoured(tmp_path) -> None:
    """Explicit ``--log-file`` / ``--error-log-file`` paths are honoured verbatim."""
    main_path = tmp_path / "subdir-main.log"
    err_path = tmp_path / "subdir-err.log"
    async with create_logging_test_session(
        extra_args=[
            "--log-sinks",
            "file",
            "--log-file",
            str(main_path),
            "--error-log-file",
            str(err_path),
        ],
    ):
        pass

    assert main_path.exists()
    assert err_path.exists()
    # The default-named files shouldn't have been created instead.
    assert not (tmp_path / "mcp_server.log").exists()
    assert not (tmp_path / "mcp_server.error.log").exists()


@pytest.mark.asyncio
async def test_debug_level_writes_env_info_record(tmp_path) -> None:
    """At DEBUG, ``log_environment_info`` writes a JSON record to the log file."""
    main_path = tmp_path / "main.log"
    async with create_logging_test_session(
        extra_args=[
            "--log-level",
            "DEBUG",
            "--log-sinks",
            "file",
            "--log-file",
            str(main_path),
            "--error-log-file",
            str(tmp_path / "err.log"),
        ],
    ):
        pass

    content = main_path.read_text()
    # The diagnostic record starts with "Environment | " followed by a JSON object.
    assert "Environment | " in content, "env-info DEBUG record missing from log file"
    # Extract and parse the JSON payload to confirm it's well-formed.
    line = next(line for line in content.splitlines() if "Environment | " in line)
    payload = line.split("Environment | ", 1)[1]
    parsed = json.loads(payload)
    # Verify a couple of fields we know about so this isn't just a "is it JSON?" check.
    assert "os" in parsed and "python" in parsed and "logging" in parsed
    assert parsed["logging"]["level"] == "DEBUG"


@pytest.mark.asyncio
async def test_env_var_log_level_equivalent_to_flag(tmp_path) -> None:
    """``CB_MCP_LOG_LEVEL`` env var has the same effect as ``--log-level``."""
    main_path = tmp_path / "main.log"
    async with create_logging_test_session(
        extra_args=[
            "--log-sinks",
            "file",
            "--log-file",
            str(main_path),
            "--error-log-file",
            str(tmp_path / "err.log"),
        ],
        env_overrides={"CB_MCP_LOG_LEVEL": "DEBUG"},
    ):
        pass

    content = main_path.read_text()
    # Same env-info DEBUG record indicates the env-var path resolved to DEBUG.
    assert "Environment | " in content


@pytest.mark.asyncio
async def test_off_level_silences_couchbase_records_on_stderr(tmp_path) -> None:
    """``--log-level OFF`` produces zero ``couchbase`` records on stderr.

    The MCP SDK's ``stdio_client`` passes ``errlog`` straight through to the
    ``asyncio.subprocess`` machinery, which requires a real file descriptor
    (``fileno()``). An ``io.StringIO`` doesn't qualify; a real file does.
    """
    stderr_path = tmp_path / "server.stderr"
    with stderr_path.open("w", encoding="utf-8") as stderr_file:
        async with create_logging_test_session(
            extra_args=["--log-level", "OFF"],
            stderr_buffer=stderr_file,
        ):
            pass

    stderr = stderr_path.read_text()
    # External loggers (FastMCP, uvicorn) are not adopted under approach-A
    # logging, so we only assert about the ``couchbase`` logger tree.
    assert " - couchbase " not in stderr, (
        f"OFF mode leaked couchbase records to stderr:\n{stderr}"
    )


@pytest.mark.asyncio
async def test_append_on_restart_preserves_history(tmp_path) -> None:
    """Two sequential server starts append to the same log file."""
    main_path = tmp_path / "main.log"
    err_path = tmp_path / "err.log"
    args = [
        "--log-sinks",
        "file",
        "--log-file",
        str(main_path),
        "--error-log-file",
        str(err_path),
    ]
    async with create_logging_test_session(extra_args=args):
        pass
    first_size = main_path.stat().st_size
    first_text = main_path.read_text()
    assert first_size > 0

    async with create_logging_test_session(extra_args=args):
        pass
    second_text = main_path.read_text()

    # File grew (history preserved) and the first run's records are still there.
    assert main_path.stat().st_size > first_size, (
        "log file did not grow on restart — was it truncated instead of appended?"
    )
    assert first_text in second_text, "first-run records were overwritten on restart"
    # Two distinct "Logging configured" lines now (one per run).
    assert second_text.count("Logging configured") >= 2


@pytest.mark.asyncio
async def test_logging_block_exposed_via_mcp_tool(tmp_path) -> None:
    """``get_server_configuration_status`` returns a populated ``logging`` block.

    End-to-end verification of the AppContext.logging_config contract: the CLI
    entrypoint stashes the resolved snapshot on the lifespan context, the tool
    reads it, and the MCP client sees it in the response payload.
    """
    main_path = tmp_path / "main.log"
    async with create_logging_test_session(
        extra_args=[
            "--log-level",
            "DEBUG",
            "--log-sinks",
            "file",
            "--log-file",
            str(main_path),
            "--error-log-file",
            str(tmp_path / "err.log"),
        ],
    ) as session:
        response = await session.call_tool(
            "get_server_configuration_status", arguments={}
        )
        payload = extract_payload(response)

    assert isinstance(payload, dict)
    logging_block = payload["logging"]
    assert logging_block["level"] == "DEBUG"
    assert sorted(logging_block["sinks"]) == ["file"]
    assert logging_block["log_file"] == str(main_path)
    assert logging_block["max_bytes"] == 1048576
    assert logging_block["backup_count"] == 5


def test_empty_log_file_rejected_at_startup() -> None:
    """``--log-file ""`` is rejected by Click; server exits non-zero with a clear error.

    Uses ``subprocess.run`` directly because the server fails to start, so the
    MCP-session helper would just see "connection failed" without a useful
    diagnostic. We need to read the exit code and stderr to confirm the
    rejection happened cleanly at the Click validator boundary.
    """
    result = subprocess.run(
        [sys.executable, "-m", "mcp_server", "--log-file", ""],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode != 0, (
        f"server should have rejected empty --log-file, but exited 0\n"
        f"stderr: {result.stderr}"
    )
    assert "path cannot be empty" in result.stderr, (
        f"expected Click rejection message in stderr, got:\n{result.stderr}"
    )
