import logging
from dataclasses import dataclass

from couchbase.cluster import Cluster
from fastmcp.server.context import Context

from utils.config import get_settings
from utils.connection import connect_to_couchbase_cluster
from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.context")


@dataclass
class AppContext:
    """Context for the MCP server.

    Attributes:
        cluster: The Couchbase cluster connection (lazily initialized).
        read_only_mode: When True, all write operations (KV and Query) are disabled
                       and KV write tools are not loaded.
                       This is the recommended mode for safety. Default is True.
        read_only_query_mode: When True, query-based write operations are disabled.
                             DEPRECATED: Use read_only_mode instead.
    """

    cluster: Cluster | None = None
    read_only_mode: bool = True
    read_only_query_mode: bool = True


def _set_cluster_in_lifespan_context(ctx: Context) -> None:
    """Set the cluster in the lifespan context.
    If the cluster is not set, it will try to connect to the cluster using the connection string, username, and password.
    If the connection fails, it will raise an exception.
    """
    try:
        settings = get_settings()
        connection_string = settings.get("connection_string")
        username = settings.get("username")
        password = settings.get("password")
        ca_cert_path = settings.get("ca_cert_path")
        client_cert_path = settings.get("client_cert_path")
        client_key_path = settings.get("client_key_path")

        cluster = connect_to_couchbase_cluster(
            connection_string,  # type: ignore
            username,  # type: ignore
            password,  # type: ignore
            ca_cert_path,
            client_cert_path,
            client_key_path,
        )
        ctx.request_context.lifespan_context.cluster = cluster
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


def get_cluster_connection(ctx: Context) -> Cluster:
    """Return the cluster connection from the lifespan context.
    If the cluster is not set, it will try to connect to the cluster using the connection string, username, and password.
    """
    app_context = ctx.request_context.lifespan_context
    if not app_context.cluster:
        _set_cluster_in_lifespan_context(ctx)
    return app_context.cluster
