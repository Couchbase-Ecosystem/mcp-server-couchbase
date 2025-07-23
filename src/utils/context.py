import logging
from dataclasses import dataclass

from couchbase.cluster import Bucket, Cluster
from mcp.server.fastmcp import Context

from utils.config import get_settings, validate_connection_config
from utils.connection import connect_to_bucket, connect_to_couchbase_cluster
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
        cluster = connect_to_couchbase_cluster(
            connection_string,  # type: ignore
            username,  # type: ignore
            password,  # type: ignore
        )
        ctx.request_context.lifespan_context.cluster = cluster
    except Exception as e:
        logger.error(
            f"Failed to connect to Couchbase: {e} \n Please check your connection string, username and password"
        )
        raise


def _set_bucket_in_lifespan_context(ctx: Context) -> None:
    """Set the bucket in the lifespan context.
    If the bucket is not set, it will try to connect to the bucket using the cluster object in the lifespan context.
    If the cluster is not set, it will try to connect to the cluster using the connection string, username, and password.
    If the connection fails, it will raise an exception.
    """
    settings = get_settings()
    bucket_name = settings.get("bucket_name")

    # If the bucket is not set, try to connect to the bucket using the cluster object in the lifespan context
    app_context = ctx.request_context.lifespan_context

    try:
        # If the cluster is not set, try to connect to the cluster
        if not app_context.cluster:
            _set_cluster_in_lifespan_context(ctx)
        cluster = app_context.cluster

        # Try to connect to the bucket using the cluster object
        bucket = connect_to_bucket(cluster, bucket_name)  # type: ignore
        app_context.bucket = bucket
    except Exception as e:
        logger.error(
            f"Failed to connect to bucket: {e} \n Please check your bucket name and credentials."
        )
        raise


def ensure_bucket_connection(ctx: Context) -> Bucket:
    """Ensure bucket connection is established and return the bucket object."""
    validate_connection_config()
    app_context = ctx.request_context.lifespan_context
    if not app_context.bucket:
        _set_bucket_in_lifespan_context(ctx)
    return app_context.bucket
