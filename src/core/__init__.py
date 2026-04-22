"""
Couchbase MCP shared core.

Portable, host-agnostic contracts and helpers that both the standalone server
and the managed Capella runtime depend on. Everything here must be safe to
import from either host — no sync-only or async-only SDK calls.
"""

from .auth import AUTH_STATE_KEY, AuthContext
from .contracts import ClusterProvider

__all__ = [
    "AUTH_STATE_KEY",
    "AuthContext",
    "ClusterProvider",
]
