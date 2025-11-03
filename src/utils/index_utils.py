"""
Utility functions for index operations.

This module contains helper functions for working with Couchbase indexes.
"""

import logging
import os
from importlib.resources import files
from typing import Any
from urllib.parse import urlparse

import httpx

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.index_utils")


def validate_filter_params(
    bucket_name: str | None,
    scope_name: str | None,
    collection_name: str | None,
    index_name: str | None = None,
) -> None:
    """Validate that filter parameters are provided in the correct hierarchy."""
    if scope_name and not bucket_name:
        raise ValueError("bucket_name is required when filtering by scope_name")
    if collection_name and (not bucket_name or not scope_name):
        raise ValueError(
            "bucket_name and scope_name are required when filtering by collection_name"
        )
    if index_name and (not bucket_name or not scope_name or not collection_name):
        raise ValueError(
            "bucket_name, scope_name, and collection_name are required when filtering by index_name"
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

    Uses importlib.resources to locate the certificate file, which works when the package is installed with fallback for development.

    Returns:
        Path to the Capella root CA certificate file.
    """
    try:
        # Use importlib.resources to get the certificate path (works for installed packages)
        cert_file = files("certs").joinpath("capella_root_ca.pem")
        # Convert to string path - this works for both installed packages and dev mode
        return str(cert_file)
    except (ImportError, FileNotFoundError, TypeError):
        # Fallback for development: use src/certs/ directory
        utils_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.dirname(utils_dir)
        fallback_path = os.path.join(src_dir, "certs", "capella_root_ca.pem")

        if os.path.exists(fallback_path):
            logger.info(f"Using fallback certificate path: {fallback_path}")
            return fallback_path

        # If we still can't find it, log a warning and return the fallback path anyway
        logger.warning(
            f"Could not locate Capella root CA certificate at {fallback_path}. "
            "SSL verification may fail for Capella connections."
        )
        return fallback_path


def _extract_hosts_from_connection_string(connection_string: str) -> list[str]:
    """Extract all hosts from a Couchbase connection string.

    Args:
        connection_string: Connection string like 'couchbase://host' or 'couchbases://host1,host2,host3'

    Returns:
        List of hosts extracted from the connection string
    """
    # Parse the connection string
    parsed = urlparse(connection_string)

    # If there's a netloc (host), extract all hosts
    if parsed.netloc:
        # Split by comma to handle multiple hosts
        # Remove port if present from each host
        hosts = [host.split(":")[0].strip() for host in parsed.netloc.split(",")]
        return hosts

    # Fallback: try to extract manually
    # Handle cases like 'couchbase://host:8091' or just 'host'
    host_part = connection_string.replace("couchbase://", "").replace(
        "couchbases://", ""
    )
    host_part = host_part.split("/")[0]
    hosts = [host.split(":")[0].strip() for host in host_part.split(",")]
    return hosts


def _determine_ssl_verification(
    connection_string: str, ca_cert_path: str | None
) -> bool | str:
    """Determine SSL verification setting based on connection string and cert path.

    Args:
        connection_string: Couchbase connection string
        ca_cert_path: Optional path to CA certificate

    Returns:
        SSL verification setting (bool or path to cert file)
    """
    is_tls_enabled = connection_string.lower().startswith("couchbases://")
    is_capella_connection = connection_string.lower().endswith(".cloud.couchbase.com")

    # Priority 1: Capella connections always use Capella root CA
    if is_capella_connection:
        capella_ca = _get_capella_root_ca_path()
        if os.path.exists(capella_ca):
            logger.info(
                f"Capella connection detected, using Capella root CA: {capella_ca}"
            )
            return capella_ca
        logger.warning(
            f"Capella CA certificate not found at {capella_ca}, "
            "falling back to system CA bundle"
        )
        return True

    # Priority 2: Non-Capella TLS connections use provided cert or system CA bundle
    if is_tls_enabled:
        if ca_cert_path:
            logger.info(f"Using provided CA certificate: {ca_cert_path}")
            return ca_cert_path
        logger.info("Using system CA bundle for SSL verification")
        return True

    # Priority 3: Non-TLS connections (HTTP), disable SSL verification
    logger.info("Non-TLS connection, SSL verification disabled")
    return False


def _build_query_params(
    bucket_name: str | None,
    scope_name: str | None,
    collection_name: str | None,
    index_name: str | None = None,
) -> dict[str, str]:
    """Build query parameters for the index REST API.

    Args:
        bucket_name: Optional bucket name
        scope_name: Optional scope name
        collection_name: Optional collection name
        index_name: Optional index name

    Returns:
        Dictionary of query parameters
    """
    params = {}
    if bucket_name:
        params["bucket"] = bucket_name
    if scope_name:
        params["scope"] = scope_name
    if collection_name:
        params["collection"] = collection_name
    if index_name:
        params["index"] = index_name
    return params


def fetch_indexes_from_rest_api(
    connection_string: str,
    username: str,
    password: str,
    bucket_name: str | None = None,
    scope_name: str | None = None,
    collection_name: str | None = None,
    index_name: str | None = None,
    ca_cert_path: str | None = None,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Fetch indexes from Couchbase Index Service REST API.

    Uses the /getIndexStatus endpoint to retrieve index information.
    This endpoint returns indexes with their definitions directly from the Index Service.

    Args:
        connection_string: Couchbase connection string (may contain multiple hosts)
        username: Username for authentication
        password: Password for authentication
        bucket_name: Optional bucket name to filter indexes
        scope_name: Optional scope name to filter indexes
        collection_name: Optional collection name to filter indexes
        index_name: Optional index name to filter indexes
        ca_cert_path: Optional path to CA certificate for SSL verification.
                     If not provided and using Capella, will use Capella root CA.
        timeout: Request timeout in seconds (default: 30)

    Returns:
        List of index status dictionaries containing name, definition, and other metadata
    """
    # Extract all hosts from connection string
    hosts = _extract_hosts_from_connection_string(connection_string)

    # Determine protocol and port based on whether TLS is enabled
    is_tls_enabled = connection_string.lower().startswith("couchbases://")
    protocol = "https" if is_tls_enabled else "http"
    port = 19102 if is_tls_enabled else 9102

    logger.info(
        f"TLS {'enabled' if is_tls_enabled else 'disabled'}, "
        f"using {protocol.upper()} with port {port}"
    )

    # Build query parameters and determine SSL verification
    params = _build_query_params(bucket_name, scope_name, collection_name, index_name)
    verify_ssl = _determine_ssl_verification(connection_string, ca_cert_path)

    # Try each host one by one until we get a successful response
    last_error = None
    for host in hosts:
        try:
            url = f"{protocol}://{host}:{port}/getIndexStatus"
            logger.info(
                f"Attempting to fetch indexes from: {url} with params: {params}"
            )

            response = httpx.get(
                url,
                params=params,
                auth=(username, password),
                verify=verify_ssl,
                timeout=timeout,
            )

            response.raise_for_status()
            data = response.json()
            indexes = data.get("status", [])

            logger.info(f"Successfully fetched {len(indexes)} indexes from {host}")
            return indexes

        except httpx.HTTPError as e:
            logger.warning(f"Failed to fetch indexes from {host}: {e}")
            last_error = e
        except Exception as e:
            logger.warning(f"Unexpected error when fetching from {host}: {e}")
            last_error = e

    # If we get here, all hosts failed
    error_msg = f"Failed to fetch indexes from all hosts: {hosts}"
    if last_error:
        error_msg += f". Last error: {last_error}"
    logger.error(error_msg)
    raise RuntimeError(error_msg)
