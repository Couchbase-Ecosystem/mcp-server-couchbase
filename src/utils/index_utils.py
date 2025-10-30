"""
Utility functions for index operations.

This module contains helper functions for working with Couchbase indexes.
"""

import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.index_utils")


def _get_capella_root_ca_path() -> str:
    """Get the path to the Capella root CA certificate.

    Returns:
        Path to the Capella root CA certificate file.
    """
    # Get the path to the certs directory relative to this file
    utils_dir = Path(__file__).parent
    project_root = utils_dir.parent.parent
    capella_ca_path = project_root / "certs" / "capella_root_ca.pem"
    return str(capella_ca_path)


def _extract_host_from_connection_string(connection_string: str) -> str:
    """Extract the host from a Couchbase connection string.

    Args:
        connection_string: Connection string like 'couchbase://host' or 'couchbases://host'

    Returns:
        The host extracted from the connection string
    """
    # Parse the connection string
    parsed = urlparse(connection_string)

    # If there's a netloc (host), return it
    if parsed.netloc:
        # Remove port if present
        host = parsed.netloc.split(":")[0]
        return host

    # Fallback: try to extract manually
    # Handle cases like 'couchbase://host:8091' or just 'host'
    host = connection_string.replace("couchbase://", "").replace("couchbases://", "")
    host = host.split(":")[0].split("/")[0]
    return host


def fetch_indexes_from_rest_api(
    connection_string: str,
    username: str,
    password: str,
    bucket_name: str | None = None,
    scope_name: str | None = None,
    collection_name: str | None = None,
    ca_cert_path: str | None = None,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Fetch indexes from Couchbase Index Service REST API.

    Uses the /getIndexStatus endpoint to retrieve index information.
    This endpoint returns indexes with their definitions directly from the Index Service.

    Args:
        connection_string: Couchbase connection string
        username: Username for authentication
        password: Password for authentication
        bucket_name: Optional bucket name to filter indexes
        scope_name: Optional scope name to filter indexes
        collection_name: Optional collection name to filter indexes
        ca_cert_path: Optional path to CA certificate for SSL verification.
                     If not provided and using couchbases://, will use Capella root CA.
        timeout: Request timeout in seconds (default: 30)

    Returns:
        List of index status dictionaries containing name, definition, and other metadata
    """
    try:
        # Extract host from connection string
        host = _extract_host_from_connection_string(connection_string)

        # Determine protocol and port based on whether TLS is enabled
        # TLS enabled (couchbases://): HTTPS with port 19102
        # TLS disabled (couchbase://): HTTP with port 9102
        is_tls_enabled = connection_string.lower().startswith("couchbases://")
        is_capella_connection = connection_string.lower().endswith(
            ".cloud.couchbase.com"
        )
        protocol = "https" if is_tls_enabled else "http"
        port = 19102 if is_tls_enabled else 9102

        logger.info(
            f"TLS {'enabled' if is_tls_enabled else 'disabled'}, using {protocol.upper()} with port {port}"
        )

        # Build the REST API URL
        base_url = f"{protocol}://{host}:{port}"
        url = f"{base_url}/getIndexStatus"

        # Build query parameters
        params = {}
        if bucket_name:
            params["bucket"] = bucket_name
        if scope_name:
            params["scope"] = scope_name
        if collection_name:
            params["collection"] = collection_name

        logger.info(f"Fetching indexes from REST API: {url} with params: {params}")

        # Determine SSL verification setting
        # For TLS connections (couchbases://), use certificate verification
        # For non-TLS connections (couchbase://), verification is not needed
        verify_ssl: bool | str = True
        if is_tls_enabled:
            if ca_cert_path:
                # Priority 1: Use provided certificate path
                verify_ssl = ca_cert_path
                logger.info(
                    f"Using provided CA certificate for SSL verification: {ca_cert_path}"
                )
            elif is_capella_connection:
                # Priority 2: Use Capella root CA for Capella connections
                capella_ca = _get_capella_root_ca_path()
                if os.path.exists(capella_ca):
                    verify_ssl = capella_ca
                    logger.info(
                        f"Using Capella root CA for SSL verification: {capella_ca}"
                    )
                else:
                    # Fall back to system CA bundle if Capella CA not found
                    verify_ssl = True
                    logger.warning(
                        f"Capella CA certificate not found at {capella_ca}, "
                        "falling back to system CA bundle"
                    )
            else:
                # Priority 3: Fall back to system CA bundle for other TLS connections
                verify_ssl = True
                logger.info("Using system CA bundle for SSL verification")
        else:
            # For non-TLS connections, SSL verification is not applicable
            verify_ssl = True

        # Make the request
        response = requests.get(
            url,
            params=params,
            auth=(username, password),
            verify=verify_ssl,
            timeout=timeout,
        )

        response.raise_for_status()
        data = response.json()

        # The API returns a dictionary with 'status' key containing the list of indexes
        indexes = data.get("status", [])

        logger.info(f"Successfully fetched {len(indexes)} indexes from REST API")
        return indexes

    except requests.RequestException as e:
        logger.error(f"Error fetching indexes from REST API: {e}")
        raise RuntimeError(f"Failed to fetch indexes from REST API: {e}") from e
    except Exception as e:
        logger.error(f"Unexpected error in fetch_indexes_from_rest_api: {e}")
        raise
