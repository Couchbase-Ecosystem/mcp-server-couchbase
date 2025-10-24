"""
Utility functions for index operations.

This module contains helper functions for working with Couchbase indexes.
"""

import logging
from typing import Any
from urllib.parse import urlparse

import requests
import urllib3

from .constants import MCP_SERVER_NAME

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.index_utils")


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
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Fetch indexes from Couchbase Index Service REST API.

    Uses the /getIndexStatus endpoint on port 19102 to retrieve index information.
    This endpoint returns indexes with their definitions directly from the Index Service.

    Args:
        connection_string: Couchbase connection string
        username: Username for authentication
        password: Password for authentication
        bucket_name: Optional bucket name to filter indexes
        scope_name: Optional scope name to filter indexes
        collection_name: Optional collection name to filter indexes
        timeout: Request timeout in seconds (default: 30)

    Returns:
        List of index status dictionaries containing name, definition, and other metadata
    """
    try:
        # Extract host from connection string
        host = _extract_host_from_connection_string(connection_string)

        # Build the REST API URL (Index Service runs on port 19102)
        base_url = f"https://{host}:19102"
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

        # Make the request
        response = requests.get(
            url,
            params=params,
            auth=(username, password),
            verify=False,  # Disable SSL verification for self-signed certs
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
