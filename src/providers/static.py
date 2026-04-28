import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from acouchbase.cluster import Cluster
from fastmcp import Context

from cb_mcp.utils.connection import connect_to_couchbase_cluster
from cb_mcp.utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.providers.static")


class StaticClusterProvider:
    """Cluster provider for the standalone host.

    Opens a single cluster for the life of the server using the
    connection string, credentials, and cert paths supplied via CLI
    flags or environment variables. The cluster is created lazily on
    first request so that ``--help`` and tool discovery don't require a
    live Couchbase.

    Concurrent first calls coalesce on an asyncio lock so only one
    connection attempt is made.
    """

    def __init__(self, settings: Mapping[str, Any]) -> None:
        self._settings = settings
        self._cluster: Cluster | None = None
        self._lock = asyncio.Lock()

    async def get_cluster(
        self, ctx: Context
    ) -> Cluster:  # ctx unused; settings come from init
        """Return the shared cluster, connecting on the first call."""
        if self._cluster is not None:
            return self._cluster
        async with self._lock:
            if self._cluster is None:
                self._cluster = await self._connect()
        return self._cluster

    async def _connect(self) -> Cluster:
        """Open a new cluster connection from the init-time settings."""
        try:
            return await connect_to_couchbase_cluster(
                self._settings.get("connection_string"),  # type: ignore[arg-type]
                self._settings.get("username"),  # type: ignore[arg-type]
                self._settings.get("password"),  # type: ignore[arg-type]
                self._settings.get("ca_cert_path"),
                self._settings.get("client_cert_path"),
                self._settings.get("client_key_path"),
            )
        except Exception as e:
            logger.error(
                "Failed to connect to Couchbase: %s\n"
                "Verify connection string, and either:\n"
                "- Username/password are correct, or\n"
                "- Client certificate and key exist and match server mapping.\n"
                "If using self-signed or custom CA, set CB_CA_CERT_PATH to the CA file.",
                e,
            )
            raise

    async def close(self) -> None:
        """Close the cluster connection and reset internal state."""
        cluster = self._cluster
        if cluster is not None:
            await cluster.close()
            self._cluster = None

    @property
    def is_connected(self) -> bool:
        """True once get_cluster has successfully opened a connection."""
        return self._cluster is not None
