import logging
from dataclasses import dataclass

from couchbase.cluster import Bucket, Cluster
from mcp.server.fastmcp import Context

from utils.config import get_settings, validate_connection_config
from utils.connection import connect_to_couchbase_cluster
from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.context")


@dataclass
class AppContext:
    """Context for the MCP server."""

    cluster: Cluster | None = None
    bucket: Bucket | None = None
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
        cluster = connect_to_couchbase_cluster(
            connection_string,  # type: ignore
            username,  # type: ignore
            password,  # type: ignore
            ca_cert_path,  # type: ignore
            client_cert_path  # type: ignore
        )
        ctx.request_context.lifespan_context.cluster = cluster
    except Exception as e:
        logger.error(
            f"Failed to connect to Couchbase: {e} \n Please check your connection string, username and password"
        )
        raise


def ensure_bucket_connection(ctx: Context, bucket_name: str) -> Bucket:
    """Ensure bucket connection is established and return the bucket object."""
    try:
        cluster = ensure_cluster_connection(ctx)
    except Exception as e:
        logger.error(f"Unable to connect to Couchbase cluster {e}")
        raise
    try:
        bucket = cluster.bucket(bucket_name)
    except Exception as e:
        logger.error(f"Error accessing bucket: {e}")
        raise
    return bucket

def ensure_cluster_connection(ctx: Context) -> Cluster:
    """Ensure cluster connection is established and return the cluster object."""
    validate_connection_config()
    app_context = ctx.request_context.lifespan_context
    if not app_context.cluster:
        try:
            _set_cluster_in_lifespan_context(ctx)
        except Exception as e:
            raise
    return app_context.cluster
