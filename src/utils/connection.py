import logging
from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.connection")


def connect_to_couchbase_cluster(
    connection_string: str, username: str, password: str
) -> Cluster:
    """Connect to Couchbase cluster and return the cluster object if successful.
    If the connection fails, it will raise an exception.
    """

    try:
        logger.info("Connecting to Couchbase cluster...")
        auth = PasswordAuthenticator(username, password)
        options = ClusterOptions(auth)
        options.apply_profile("wan_development")

        cluster = Cluster(connection_string, options)  # type: ignore
        cluster.wait_until_ready(timedelta(seconds=5))

        logger.info("Successfully connected to Couchbase cluster")
        return cluster
    except Exception as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise
