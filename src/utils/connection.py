import logging
from datetime import timedelta
import os
from couchbase.auth import PasswordAuthenticator, CertificateAuthenticator
from couchbase.cluster import Bucket,Cluster
from couchbase.options import ClusterOptions

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.connection")


def connect_to_couchbase_cluster(
    connection_string: str, username: str, password: str, ca_cert_path : str = None, client_cert_path : str = None
) -> Cluster:
    """Connect to Couchbase cluster and return the cluster object if successful.
    Requires either a username/password or client certificate path. A CA certificate path is optional, if None then local trust store is used.
    If the connection fails, it will raise an exception.
    """
    try:
        logger.info("Connecting to Couchbase cluster...")
        if client_cert_path:
                    
            tls_conf = {
                "cert_path" :  os.path.join(client_cert_path, "client.pem"),
                "key_path" :  os.path.join(client_cert_path, "client.key"),
            }
            #set ca cert as trust store if provided
            if ca_cert_path:
                tls_conf["trust_store_path"] = ca_cert_path
            auth = CertificateAuthenticator(**tls_conf)
        else:
            auth = PasswordAuthenticator(username, password, cert_path = ca_cert_path)
        options = ClusterOptions(auth)
        options.apply_profile("wan_development")
        cluster = Cluster(connection_string, options)  # type: ignore
        cluster.wait_until_ready(timedelta(seconds=5))

        logger.info("Successfully connected to Couchbase cluster")
        return cluster
    except Exception as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise


def connect_to_bucket(cluster: Cluster, bucket_name: str) -> Bucket:
    """Connect to a bucket and return the bucket object if successful.
    If the operation fails, it will raise an exception.
    """
    try:
        logger.info(f"Connecting to bucket: {bucket_name}")
        bucket = cluster.bucket(bucket_name)
        return bucket
    except Exception as e:
        logger.error(f"Failed to connect to bucket: {e}")
        raise
