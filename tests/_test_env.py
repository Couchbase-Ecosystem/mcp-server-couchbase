"""Shared environment helpers for the MCP server test suite.

These helpers are imported by both ``tests/conftest.py`` (used by the
integration tests) and ``tests/accuracy/conftest.py`` (used by the
accuracy tests). Keeping them in their own module avoids the
``conftest``-vs-``conftest`` name collision that would otherwise force a
nested conftest to load the parent via ``importlib``.
"""

from __future__ import annotations

import os

import pytest

REQUIRED_ENV_VARS = ("CB_CONNECTION_STRING", "CB_USERNAME", "CB_PASSWORD")


def _build_env() -> dict[str, str]:
    """Build the environment passed to the test server process."""
    env = os.environ.copy()
    missing = [var for var in REQUIRED_ENV_VARS if not env.get(var)]
    if missing:
        pytest.skip(
            "Integration tests require demo cluster credentials. "
            f"Missing env vars: {', '.join(missing)}"
        )

    env["CB_MCP_TRANSPORT"] = "stdio"
    env["CB_MCP_READ_ONLY_MODE"] = "false"
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def get_test_bucket() -> str | None:
    """Get the test bucket name from environment, or None if not set."""
    return os.getenv("CB_MCP_TEST_BUCKET")


def get_test_scope() -> str:
    """Get the test scope name from environment, defaults to _default."""
    return os.getenv("CB_MCP_TEST_SCOPE", "_default")


def get_test_collection() -> str:
    """Get the test collection name from environment, defaults to _default."""
    return os.getenv("CB_MCP_TEST_COLLECTION", "_default")


def require_test_bucket() -> str:
    """Get the test bucket name, skipping test if not set."""
    bucket = get_test_bucket()
    if not bucket:
        pytest.skip("CB_MCP_TEST_BUCKET not set")
    return bucket
