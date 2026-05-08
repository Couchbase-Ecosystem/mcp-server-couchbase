from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from acouchbase.cluster import Cluster
from fastmcp import Context

from ..core.contracts import ClusterProvider


@dataclass
class AppContext:
    """Lifespan-scoped context for the MCP server.

    Attributes:
        cluster_provider: The host's ``ClusterProvider`` implementation.
            The standalone MCP server populates this with ``StaticClusterProvider``
            during lifespan startup; other implementations supply their own.
        settings: Snapshot of CLI/environment-resolved configuration
            captured once at lifespan startup. Tools should read values
            from here via :func:`cb_mcp.utils.config.get_settings` rather than
            reaching for a module global.
        read_only_mode: When True, all write operations (KV and Query) are
            disabled and KV write tools are not loaded.
        read_only_query_mode: When True, query-based write operations are
            disabled. DEPRECATED: use ``read_only_mode`` instead.
    """

    cluster_provider: ClusterProvider | None = None
    settings: Mapping[str, Any] = field(default_factory=dict)
    read_only_mode: bool = True
    read_only_query_mode: bool = True


def get_cluster_provider(ctx: Context):
    """Return the ClusterProvider for this request."""
    return ctx.request_context.lifespan_context.cluster_provider


async def get_cluster_connection(ctx: Context) -> Cluster:
    """Return the Couchbase cluster for this request via the provider."""
    provider = get_cluster_provider(ctx)
    if provider is None:
        raise RuntimeError(
            "Cluster provider not initialized. "
            "The lifespan must populate AppContext.cluster_provider before tools run."
        )
    return await provider.get_cluster(ctx)
