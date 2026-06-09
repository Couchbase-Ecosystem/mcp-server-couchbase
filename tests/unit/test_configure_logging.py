"""Tests for configure_logging end-to-end behaviour.

The Couchbase SDK's ``configure_logging`` is one-shot per process (it raises
``InvalidArgumentException`` on a second call), so we patch
:func:`cb_mcp.utils.logging.couchbase.configure_logging` for every test. Each
test also restores the ``couchbase`` logger and the module-level snapshot
afterwards via an autouse fixture, so tests don't bleed state into one another.
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from unittest.mock import patch

import pytest

import cb_mcp.utils.logging as logmod
from cb_mcp.utils.constants import MCP_SERVER_NAME
from cb_mcp.utils.logging import (
    LEVEL_OFF,
    ResolvedLoggingConfig,
    configure_logging,
    get_resolved_logging_config,
)


@pytest.fixture(autouse=True)
def reset_logging_state():
    """Restore the couchbase logger and the resolved-config singleton."""
    yield
    logger = logging.getLogger(MCP_SERVER_NAME)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.propagate = True
    logger.setLevel(logging.NOTSET)
    logmod._resolved_config = None


@pytest.fixture(autouse=True)
def mock_sdk_configure_logging():
    """Couchbase SDK ``configure_logging`` is one-shot per process; mock it.

    The patch target is the ``couchbase`` symbol *as imported into our logging
    module* — patching ``couchbase.configure_logging`` directly wouldn't catch
    references already resolved at module load time.
    """
    with patch.object(logmod.couchbase, "configure_logging") as mock:
        yield mock


def _call(level="INFO", sinks=None, log_file="m.log", error_log_file="e.log", **kwargs):
    """Helper that fills in the boilerplate arguments."""
    configure_logging(
        level=level,
        sinks=sinks if sinks is not None else {"stderr"},
        log_file=log_file,
        error_log_file=error_log_file,
        log_max_bytes=kwargs.pop("log_max_bytes", 1024),
        log_backup_count=kwargs.pop("log_backup_count", 1),
        **kwargs,
    )


class TestStderrSinkHandlerAttachment:
    """Default sinks={'stderr'} attaches exactly one handler to the couchbase logger."""

    def test_attaches_single_stream_handler(self):
        _call(sinks={"stderr"})
        logger = logging.getLogger(MCP_SERVER_NAME)
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)

    def test_propagate_false_to_avoid_root_double_emit(self):
        _call(sinks={"stderr"})
        logger = logging.getLogger(MCP_SERVER_NAME)
        assert logger.propagate is False

    def test_level_set_on_logger(self):
        _call(level="DEBUG", sinks={"stderr"})
        logger = logging.getLogger(MCP_SERVER_NAME)
        assert logger.level == logging.DEBUG


class TestFileSinkSplitContract:
    """File sink always attaches both main + error file handlers."""

    def test_attaches_two_rotating_file_handlers(self, tmp_path):
        _call(
            sinks={"file"},
            log_file=str(tmp_path / "main.log"),
            error_log_file=str(tmp_path / "err.log"),
        )
        logger = logging.getLogger(MCP_SERVER_NAME)
        rotating = [h for h in logger.handlers if isinstance(h, RotatingFileHandler)]
        assert len(rotating) == 2

    def test_main_handler_filters_out_error_and_above(self, tmp_path):
        """The main file must not contain ERROR/CRITICAL records.

        We attach the _below_error filter on the main file handler; this test
        asserts that the filter is wired by directly probing it.
        """
        _call(
            sinks={"file"},
            log_file=str(tmp_path / "main.log"),
            error_log_file=str(tmp_path / "err.log"),
        )
        logger = logging.getLogger(MCP_SERVER_NAME)
        rotating = [h for h in logger.handlers if isinstance(h, RotatingFileHandler)]
        # Find the one with a filter — that's the main handler.
        main = next(h for h in rotating if h.filters)
        warning_record = logging.LogRecord(
            "x", logging.WARNING, "f", 1, "w", None, None
        )
        error_record = logging.LogRecord("x", logging.ERROR, "f", 1, "e", None, None)
        assert main.filter(warning_record) is True
        assert main.filter(error_record) is False

    def test_error_handler_level_set_to_error(self, tmp_path):
        _call(
            sinks={"file"},
            log_file=str(tmp_path / "main.log"),
            error_log_file=str(tmp_path / "err.log"),
        )
        logger = logging.getLogger(MCP_SERVER_NAME)
        rotating = [h for h in logger.handlers if isinstance(h, RotatingFileHandler)]
        error = next(h for h in rotating if h.level == logging.ERROR)
        assert error.level == logging.ERROR

    def test_records_split_no_duplication_between_files(self, tmp_path):
        """End-to-end emission test: WARNING goes only to main, ERROR only to error."""
        main_path = tmp_path / "main.log"
        err_path = tmp_path / "err.log"
        _call(
            level="DEBUG",
            sinks={"file"},
            log_file=str(main_path),
            error_log_file=str(err_path),
        )
        log = logging.getLogger(f"{MCP_SERVER_NAME}.test")
        log.warning("a-warning")
        log.error("an-error")

        # Force handlers to flush.
        for h in logging.getLogger(MCP_SERVER_NAME).handlers:
            h.flush()

        main_text = main_path.read_text()
        err_text = err_path.read_text()
        assert "a-warning" in main_text
        assert "a-warning" not in err_text
        assert "an-error" in err_text
        assert "an-error" not in main_text


class TestStderrAndFileTogether:
    """sinks={'stderr', 'file'} produces three handlers total."""

    def test_three_handlers_attached(self, tmp_path):
        _call(
            sinks={"stderr", "file"},
            log_file=str(tmp_path / "m.log"),
            error_log_file=str(tmp_path / "e.log"),
        )
        logger = logging.getLogger(MCP_SERVER_NAME)
        assert len(logger.handlers) == 3


class TestOffMode:
    """OFF level attaches no handlers, sets sentinel level, records snapshot."""

    def test_no_handlers_attached(self):
        _call(level="OFF", sinks={"stderr", "file"})
        logger = logging.getLogger(MCP_SERVER_NAME)
        assert logger.handlers == []

    def test_logger_level_set_to_sentinel(self):
        _call(level="OFF")
        logger = logging.getLogger(MCP_SERVER_NAME)
        assert logger.level == LEVEL_OFF

    def test_sdk_called_with_sentinel(self, mock_sdk_configure_logging):
        _call(level="OFF")
        # SDK is told OFF too — drops records at the C++ boundary.
        mock_sdk_configure_logging.assert_called_with(MCP_SERVER_NAME, LEVEL_OFF)

    def test_snapshot_reflects_inactive_state(self):
        _call(level="OFF", sinks={"stderr", "file"})
        snap = get_resolved_logging_config()
        assert snap is not None
        assert snap.level == "OFF"
        assert snap.sinks == ()
        assert snap.log_file is None
        assert snap.error_log_file is None


class TestLenientLevelFallback:
    """Invalid `level` argument falls back to DEFAULT_LOG_LEVEL, doesn't raise."""

    def test_invalid_level_does_not_raise(self):
        _call(level="VERBOSE")  # not in ALLOWED_LOG_LEVELS

    def test_invalid_level_falls_back_to_default(self):
        _call(level="VERBOSE")
        snap = get_resolved_logging_config()
        assert snap is not None
        assert snap.level == "INFO"

    def test_invalid_level_emits_deferred_error_record(self, capsys):
        """The error record fires only after handlers are wired so it's visible.

        We capture stderr directly because ``configure_logging`` sets
        ``propagate = False`` on the ``couchbase`` logger; pytest's ``caplog``
        hooks into the root logger by default and wouldn't see records that
        don't propagate.
        """
        _call(level="NONSENSE", sinks={"stderr"})
        captured = capsys.readouterr()
        assert "NONSENSE" in captured.err
        assert "Ignored invalid log level" in captured.err


class TestSnapshot:
    """ResolvedLoggingConfig snapshot reflects the active state."""

    def test_snapshot_populated_after_call(self):
        _call(level="DEBUG", sinks={"stderr"})
        snap = get_resolved_logging_config()
        assert snap is not None
        assert isinstance(snap, ResolvedLoggingConfig)
        assert snap.level == "DEBUG"
        assert snap.sinks == ("stderr",)
        assert snap.log_file is None
        assert snap.error_log_file is None

    def test_file_paths_visible_only_when_file_sink_active(self, tmp_path):
        # User passed paths but only stderr sink; paths should NOT appear in snapshot.
        _call(
            sinks={"stderr"},
            log_file=str(tmp_path / "m.log"),
            error_log_file=str(tmp_path / "e.log"),
        )
        snap = get_resolved_logging_config()
        assert snap is not None
        assert snap.log_file is None
        assert snap.error_log_file is None

    def test_sinks_sorted_for_deterministic_output(self, tmp_path):
        _call(
            sinks={"stderr", "file"},
            log_file=str(tmp_path / "m.log"),
            error_log_file=str(tmp_path / "e.log"),
        )
        snap = get_resolved_logging_config()
        assert snap is not None
        assert snap.sinks == ("file", "stderr")  # sorted alphabetically


class TestAsDict:
    """ResolvedLoggingConfig.as_dict shape and field naming."""

    def test_keys_match_documented_shape(self):
        cfg = ResolvedLoggingConfig(
            level="DEBUG",
            sinks=("stderr",),
            log_file=None,
            error_log_file=None,
            log_max_bytes=42,
            log_backup_count=3,
        )
        d = cfg.as_dict()
        # JSON-friendly key names (max_bytes / backup_count without log_ prefix).
        assert set(d.keys()) == {
            "level",
            "sinks",
            "log_file",
            "error_log_file",
            "max_bytes",
            "backup_count",
        }

    def test_sinks_serialised_as_list(self):
        cfg = ResolvedLoggingConfig(
            level="INFO",
            sinks=("file", "stderr"),
            log_file="m.log",
            error_log_file="e.log",
            log_max_bytes=1,
            log_backup_count=1,
        )
        d = cfg.as_dict()
        assert d["sinks"] == ["file", "stderr"]


class TestIdempotency:
    """configure_logging can be called multiple times without accumulating handlers."""

    def test_handlers_not_duplicated_on_second_call(self):
        _call(sinks={"stderr"})
        first_count = len(logging.getLogger(MCP_SERVER_NAME).handlers)
        _call(sinks={"stderr"})
        second_count = len(logging.getLogger(MCP_SERVER_NAME).handlers)
        assert first_count == second_count == 1
