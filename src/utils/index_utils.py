"""
Utility functions for index operations.

This module contains helper functions for working with Couchbase indexes.
"""

import logging
import os
from typing import Any
from urllib.parse import urlparse

import requests

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.index_utils")


def validate_filter_params(
    bucket_name: str | None,
    scope_name: str | None,
    collection_name: str | None,
) -> None:
    """Validate that filter parameters are provided in the correct hierarchy."""
    if scope_name and not bucket_name:
        raise ValueError("bucket_name is required when filtering by scope_name")
    if collection_name and (not bucket_name or not scope_name):
        raise ValueError(
            "bucket_name and scope_name are required when filtering by collection_name"
        )


def validate_connection_settings(settings: dict[str, Any]) -> None:
    """Validate that required connection settings are present."""
    required = ["connection_string", "username", "password"]
    missing = [key for key in required if not settings.get(key)]
    if missing:
        raise ValueError(f"Missing required connection settings: {', '.join(missing)}")


def clean_index_definition(definition: Any) -> str:
    """Clean up index definition string by removing quotes and escape characters."""
    if isinstance(definition, str) and definition:
        return definition.strip('"').replace('\\"', '"')
    return ""


def process_index_data(
    idx: dict[str, Any], include_raw_index_stats: bool
) -> dict[str, Any] | None:
    """Process raw index data into formatted index info.

    Args:
        idx: Raw index data from the API
        include_raw_index_stats: Whether to include complete raw stats in the output

    Returns:
        Formatted index info dictionary, or None if the index should be skipped (e.g., no name).
    """
    name = idx.get("name", "")
    if not name:
        return None

    # Start with name and optional definition
    index_info: dict[str, Any] = {"name": name}

    clean_def = clean_index_definition(idx.get("definition", ""))
    if clean_def:
        index_info["definition"] = clean_def

    # Copy standard fields from raw index data
    standard_fields = ["status", "bucket", "scope", "collection"]
    for field in standard_fields:
        if field in idx:
            index_info[field] = idx[field]

    # Always include isPrimary as a boolean
    index_info["isPrimary"] = idx.get("isPrimary", False)

    # Optionally include complete raw stats
    if include_raw_index_stats:
        index_info["raw_index_stats"] = idx

    return index_info


def _get_capella_root_ca_path() -> str:
    """Get the path to the Capella root CA certificate.

    Returns:
        Path to the Capella root CA certificate file.
    """
    # Get the path to the certs directory relative to this file
    utils_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(utils_dir))
    capella_ca_path = os.path.join(project_root, "certs", "capella_root_ca.pem")
    return capella_ca_path


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
        # Priority 1: Capella connections always use Capella root CA
        # Priority 2: TLS connections use provided cert or system CA bundle
        # Priority 3: Non-TLS connections use verify=False
        verify_ssl: bool | str = True
        if is_capella_connection:
            # Priority 1: Use Capella root CA for Capella connections (overrides user-provided cert)
            capella_ca = _get_capella_root_ca_path()
            if os.path.exists(capella_ca):
                verify_ssl = capella_ca
                logger.info(
                    f"Capella connection detected, using Capella root CA for SSL verification: {capella_ca}"
                )
            else:
                # Fall back to system CA bundle if Capella CA not found
                verify_ssl = True
                logger.warning(
                    f"Capella CA certificate not found at {capella_ca}, "
                    "falling back to system CA bundle"
                )
        elif is_tls_enabled:
            # Priority 2: For non-Capella TLS connections, use provided cert or system CA bundle
            if ca_cert_path:
                verify_ssl = ca_cert_path
                logger.info(
                    f"Using provided CA certificate for SSL verification: {ca_cert_path}"
                )
            else:
                verify_ssl = True
                logger.info("Using system CA bundle for SSL verification")
        else:
            # Priority 3: For non-TLS connections (HTTP), disable SSL verification
            verify_ssl = False
            logger.info("Non-TLS connection, SSL verification disabled")

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
