"""
Host-agnostic contracts implemented by each MCP server host.

A *host* is a concrete MCP server — today either the standalone CLI in
this repo or the managed Capella runtime. Tool bodies live per-host, but
both hosts reach a Couchbase cluster through the same
``ClusterProvider`` shape so that the rest of the machinery
(lifespans, middleware, shared helpers) can be written against a single
interface.
"""

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from acouchbase.cluster import Cluster
from fastmcp import Context


@runtime_checkable
class ClusterProvider(Protocol):
    """Resolves a Couchbase cluster for a given request.

    Implementations decide how credentials are sourced (static config,
    Secrets Manager, etc.) and how clusters are cached (one per server,
    one per principal, etc.).

    """

    async def get_cluster(self, ctx: Context) -> Cluster:
        """Return (or begin returning) a cluster for this request."""
        ...

    async def close(self) -> None:
        """Release any clusters held by this provider and perform cleanup."""
        ...

    async def get_configuration(self, ctx: Context) -> Mapping[str, Any]:
        """Provider-specific configuration suitable for status reporting.

        Must not include secrets — return ``_configured`` booleans instead.
        Implementations may use ``ctx`` to return per-caller configuration
        (e.g., per-API-key in managed hosts) or ignore it (static hosts).
        """
        ...

    async def is_connected(self, ctx: Context) -> bool:
        """True if a cluster is currently open for this caller.

        Implementations may use ``ctx`` to check per-caller connection state
        (e.g., per-principal cache entry) or ignore it (static hosts).
        """
        ...
