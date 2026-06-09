"""Logging configuration for the Couchbase MCP Server.

Centralises handler/formatter wiring so the CLI entrypoint only needs a
single call. All MCP modules log under the ``MCP_SERVER_NAME`` ("couchbase")
logger hierarchy; the Couchbase Python SDK is routed into the same tree via
``couchbase.configure_logging``, which means handlers attached here apply to
SDK records as well.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler

import couchbase

from .constants import (
    ALLOWED_LOG_LEVELS,
    ALLOWED_LOG_SINKS,
    DEFAULT_LOG_DATEFMT,
    DEFAULT_LOG_FORMAT,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_SINKS,
    MCP_SERVER_NAME,
)

# Sentinel above CRITICAL used to disable the MCP logger when --log-level=OFF.
# ``Logger.isEnabledFor(level)`` short-circuits before a LogRecord is built when
# the threshold is unreachable, so this is the cheapest way to silence the
# logger without touching other loggers in the process.
LEVEL_OFF = logging.CRITICAL + 1


def _below_error(record: logging.LogRecord) -> bool:
    """Filter predicate: keep records strictly below ERROR.

    Used on the main file handler when an error file is also configured: the
    main file captures DEBUG/INFO/WARNING, the error file captures
    ERROR/CRITICAL, with no overlap.
    """
    return record.levelno < logging.ERROR


def parse_log_level(value: str) -> tuple[str, str | None]:
    """Parse a log level value, falling back to the default for invalid input.

    Returns ``(resolved_level, invalid_input)``. When ``value`` matches one of
    ``ALLOWED_LOG_LEVELS`` (case-insensitive), ``invalid_input`` is ``None``.
    Otherwise the resolved level is ``DEFAULT_LOG_LEVEL`` and the original
    input is returned so the caller can surface it via the logger once
    handlers are wired.
    """
    token = value.strip().upper()
    if token in ALLOWED_LOG_LEVELS:
        return token, None
    return DEFAULT_LOG_LEVEL, value


def parse_log_sinks(value: str) -> tuple[set[str], list[str]]:
    """Parse a comma-separated CB_MCP_LOG_SINKS value.

    Tokens are case-insensitive and whitespace around them is trimmed. Valid
    tokens are accumulated; unknown tokens are collected separately so the
    caller can surface them via the logger once it is configured. If no valid
    tokens survive, the default sink is used so the server still produces
    output.

    Returns a tuple ``(sinks, invalid_tokens)`` where ``sinks`` is a non-empty
    set drawn from ``ALLOWED_LOG_SINKS`` and ``invalid_tokens`` lists any
    rejected tokens in their original case.
    """
    sinks: set[str] = set()
    invalid: list[str] = []
    for part in value.split(","):
        token = part.strip()
        if token:
            normalised = token.lower()
            if normalised in ALLOWED_LOG_SINKS:
                sinks.add(normalised)
            else:
                invalid.append(token)
    if not sinks:
        sinks.add(DEFAULT_LOG_SINKS)
    return sinks, invalid


def configure_logging(
    level: str,
    sinks: set[str],
    log_file: str,
    error_log_file: str,
    log_max_bytes: int,
    log_backup_count: int,
    invalid_sinks: list[str] | None = None,
    invalid_level: str | None = None,
) -> None:
    """Configure the root MCP logger and the Couchbase SDK logs.

    The ``sinks`` set is authoritative: ``"stderr"`` attaches a stderr
    handler; ``"file"`` always attaches **two** rotating file handlers — a
    main log (DEBUG/INFO/WARNING) and an error log (ERROR/CRITICAL) — with
    no overlap. Any path the caller omits falls back to the default name in
    the process CWD (``mcp_server.log`` / ``mcp_server.error.log``).

    Setting ``level="OFF"`` suppresses output regardless of sinks.
    """
    level_name = level.upper()
    if level_name not in ALLOWED_LOG_LEVELS:
        # Defer logging about the invalid level until after handlers are configured,
        # so the message is visible even when the user sets an unrecognised level.
        invalid_level = level
        level_name = DEFAULT_LOG_LEVEL

    logger = logging.getLogger(MCP_SERVER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    logger.propagate = False

    if level_name == "OFF":
        logger.setLevel(LEVEL_OFF)
        couchbase.configure_logging(MCP_SERVER_NAME, LEVEL_OFF)
        return

    logger.setLevel(level_name)

    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, datefmt=DEFAULT_LOG_DATEFMT)

    effective_sinks = set(sinks)

    if "stderr" in effective_sinks:
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(formatter)
        logger.addHandler(stderr_handler)

    # File logging is always a two-file split when enabled. The caller (CLI
    # or direct) is responsible for providing both paths; Click defaults
    # supply DEFAULT_LOG_FILE / DEFAULT_ERROR_LOG_FILE when the flags are
    # omitted, so we don't need an in-function fallback.
    if "file" in effective_sinks:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=log_max_bytes,
            backupCount=log_backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(_below_error)
        logger.addHandler(file_handler)

        error_handler = RotatingFileHandler(
            error_log_file,
            maxBytes=log_max_bytes,
            backupCount=log_backup_count,
            encoding="utf-8",
        )
        error_handler.setFormatter(formatter)
        error_handler.setLevel(logging.ERROR)
        logger.addHandler(error_handler)

    couchbase.configure_logging(MCP_SERVER_NAME, logger.level)

    if invalid_level:
        logger.error(
            "Ignored invalid log level %r in --log-level/CB_MCP_LOG_LEVEL; "
            "allowed values are %s. Continuing with level=%s.",
            invalid_level,
            list(ALLOWED_LOG_LEVELS),
            level_name,
        )

    if invalid_sinks:
        logger.error(
            "Ignored invalid log sink value(s) %s in --log-sinks/CB_MCP_LOG_SINKS; "
            "allowed values are %s. Continuing with sinks=%s.",
            invalid_sinks,
            list(ALLOWED_LOG_SINKS),
            ",".join(sorted(effective_sinks)),
        )

    # Show file paths in the summary only when the file sink is active; the
    # values are populated by Click defaults regardless, but printing them
    # for a stderr-only run would falsely suggest files are being written.
    file_sink_active = "file" in effective_sinks
    logger.info(
        "Logging configured: level=%s, sinks=%s, log_file=%s, error_log_file=%s, "
        "max_bytes=%d, backup_count=%d",
        level_name,
        ",".join(sorted(effective_sinks)),
        log_file if file_sink_active else "-",
        error_log_file if file_sink_active else "-",
        log_max_bytes,
        log_backup_count,
    )
