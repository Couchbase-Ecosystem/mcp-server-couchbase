"""
Verifier-only structured logger with call-stack-based indentation.

Usage:
    from catalog.enrichment.relationship_verifier.logger import get_verifier_logger

    logger = get_verifier_logger()
    logger.info("Starting verification")
    logger.debug("Query result", extra={"rows": 5})
"""

from __future__ import annotations

import inspect
import logging
import os
import threading
from datetime import datetime
from pathlib import Path

# Default log level (can be overridden via environment variable)
DEFAULT_LOG_LEVEL = "INFO"
ENV_LOG_LEVEL = "CB_VERIFIER_LOG_LEVEL"

# Log directory lives under relationship_verifier/logger/logs
LOG_DIR = Path(__file__).parent / "logs"


def _compute_verifier_depth() -> int:
    """
    Compute the current call-stack depth relative to the verifier package.

    Only counts frames inside catalog/enrichment/relationship_verifier/,
    excluding logging internals and stdlib.
    """
    verifier_root = "catalog/enrichment/relationship_verifier"

    frame = inspect.currentframe()
    depth = 0

    while frame is not None:
        filename = frame.f_code.co_filename

        # Count only frames inside the verifier package
        if (
            (verifier_root in filename)
            and ("verifier_logger.py" not in filename)
            and ("logger/__init__.py" not in filename)
        ):
            # Exclude this logger module itself
            depth += 1

        frame = frame.f_back

    return max(depth, 0)


def _indent_text(text: str, indent_level: int, indent_str: str = "  ") -> str:
    """
    Apply indentation to text, including all lines.

    Ensures multiline messages align with the same indentation prefix.
    """
    prefix = indent_str * indent_level

    # Split by newlines, apply prefix to each line, rejoin
    lines = text.split("\n")
    return "\n".join(prefix + line for line in lines)


class VerifierFormatter(logging.Formatter):
    """
    Custom formatter that adds call-stack-based indentation.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def format(self, record: logging.LogRecord) -> str:
        # Get the original formatted message
        original_msg = super().format(record)

        # Compute indentation based on verifier call depth
        indent_level = _compute_verifier_depth()

        # Apply indentation
        return _indent_text(original_msg, indent_level)


class VerifierLogger:
    """
    Verifier-specific logger with structured, indented output.

    Features:
    - One file per run (timestamped)
    - Call-stack-based indentation for human-readable nesting
    - Multiline message alignment
    - Thread-safe singleton
    """

    _instance: VerifierLogger | None = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls) -> VerifierLogger:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if VerifierLogger._initialized:
            return

        with VerifierLogger._lock:
            if VerifierLogger._initialized:
                return

            # Ensure log directory exists
            LOG_DIR.mkdir(parents=True, exist_ok=True)

            # Create timestamped log file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pid = os.getpid()
            log_filename = f"verifier_{timestamp}_pid{pid}.log"
            log_path = LOG_DIR / log_filename

            # Create Python logger
            self._logger = logging.getLogger("verifier")
            self._logger.setLevel(self._get_log_level())

            # Remove any existing handlers
            self._logger.handlers.clear()

            # Add file handler with custom formatter
            file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
            file_handler.setLevel(self._get_log_level())

            # Custom formatter with timestamp and level
            formatter = VerifierFormatter(
                fmt="%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)

            # Store log path for reference
            self._log_path = log_path

            # Store baseline depth at init time
            self._baseline_depth = _compute_verifier_depth()

            VerifierLogger._initialized = True

    def _get_log_level(self) -> int:
        """Get log level from environment or default."""
        level_name = os.getenv(ENV_LOG_LEVEL, DEFAULT_LOG_LEVEL).upper()
        return getattr(logging, level_name, logging.INFO)

    @property
    def log_path(self) -> Path:
        """Path to the current log file."""
        return self._log_path

    def debug(self, msg: str, *args, **kwargs) -> None:
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs) -> None:
        self._logger.critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs) -> None:
        """Log an exception with traceback."""
        self._logger.exception(msg, *args, **kwargs)


def get_verifier_logger() -> VerifierLogger:
    """
    Get the singleton verifier logger instance.

    Returns:
        VerifierLogger: The shared logger instance.
    """
    return VerifierLogger()
