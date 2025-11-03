"""
Utility functions for index operations.

This module contains helper functions for working with Couchbase indexes.
"""

import json
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


def _extract_hosts_from_connection_string(connection_string: str) -> list[str]:
    """Extract all hosts from a Couchbase connection string.

    Args:
        connection_string: Connection string like 'couchbase://host' or 'couchbases://host1,host2,host3'

    Returns:
        List of hosts extracted from the connection string
    """
    # Parse the connection string
    parsed = urlparse(connection_string)

    # If there's a netloc (host), extract it
    if parsed.netloc:
        # Remove port if present and split by comma for multiple hosts
        netloc = parsed.netloc
        # Split by comma to handle multiple hosts
        hosts = [host.split(":")[0] for host in netloc.split(",")]
        return hosts

    # Fallback: try to extract manually
    # Handle cases like 'couchbase://host:8091' or just 'host'
    host_part = connection_string.replace("couchbase://", "").replace(
        "couchbases://", ""
    )
    host_part = host_part.split("/")[0]  # Remove any path
    # Split by comma for multiple hosts
    hosts = [host.split(":")[0] for host in host_part.split(",")]
    return hosts


def _extract_host_from_endpoint(endpoint: Any, service_name: str) -> str | None:
    """Extract host from an endpoint object.

    Args:
        endpoint: Endpoint object from ping result
        service_name: Name of the service for logging

    Returns:
        Host address or None if not found
    """
    if hasattr(endpoint, "remote") and endpoint.remote:
        # Remote is in format "host:port"
        host = endpoint.remote.split(":")[0]
        logger.info(
            f"Found index service on host {host} from ping data (service: {service_name})"
        )
        return host
    return None


def _get_index_service_host_from_ping(cluster: Any) -> str | None:
    """Get the host running the index service from cluster ping data.

    Args:
        cluster: Couchbase cluster object

    Returns:
        Host address running the index service, or None if not found
    """
    try:
        # Get ping results
        ping_result = cluster.ping()

        # Try to use the endpoints attribute directly (SDK object)
        if hasattr(ping_result, "endpoints"):
            # The SDK PingResult object has endpoints attribute
            # which is a dict with service types as keys
            for service_type, endpoints in ping_result.endpoints.items():
                service_name = str(service_type).lower()
                # Check if this is an index service
                # Common names: 'index', 'indexing', 'servicetype.index'
                if "index" in service_name and endpoints and len(endpoints) > 0:
                    # Get the first endpoint and try to extract host
                    host = _extract_host_from_endpoint(endpoints[0], service_name)
                    if host:
                        return host

        # Fallback: try JSON format
        ping_json = json.loads(ping_result.as_json())
        services = ping_json.get("services", {})

        # Look for index service with various possible names
        for service_type in ["index", "indexing", "Index", "INDEX"]:
            if service_type in services:
                endpoints = services[service_type]
                if endpoints and len(endpoints) > 0:
                    endpoint = endpoints[0]
                    # Try different field names for the remote address
                    for field in ["remote", "host", "address", "hostname"]:
                        if endpoint.get(field):
                            remote = endpoint[field]
                            # Extract host from "host:port" format
                            host = remote.split(":")[0] if ":" in remote else remote
                            logger.info(
                                f"Found index service on host {host} from ping JSON (service: {service_type})"
                            )
                            return host

        logger.warning("Index service not found in ping data")
        logger.debug(f"Available services: {list(services.keys())}")
        return None

    except Exception as e:
        logger.warning(f"Failed to get index service host from ping: {e}")
        return None


def _determine_target_host(
    connection_string: str, cluster: Any | None
) -> tuple[str, list[str]]:
    """Determine which host to use for REST API calls.

    Args:
        connection_string: Couchbase connection string
        cluster: Optional cluster object to use for ping

    Returns:
        Tuple of (target_host, all_hosts)
    """
    hosts = _extract_hosts_from_connection_string(connection_string)

    # Priority 1: If cluster is provided, use ping to find index service host
    host = None
    if cluster is not None:
        host = _get_index_service_host_from_ping(cluster)
        if host:
            logger.info(f"Using index service host from ping: {host}")

    # Priority 2: Use the first host from the connection string
    if host is None:
        host = hosts[0]
        if len(hosts) > 1:
            logger.info(
                f"Connection string has {len(hosts)} hosts. Using first host: {host}"
            )
        else:
            logger.info(f"Using host from connection string: {host}")

    return host, hosts


def _get_protocol_and_port(connection_string: str) -> tuple[str, int]:
    """Determine protocol and port based on connection string.

    Args:
        connection_string: Couchbase connection string

    Returns:
        Tuple of (protocol, port)
    """
    # TLS enabled (couchbases://): HTTPS with port 19102
    # TLS disabled (couchbase://): HTTP with port 9102
    is_tls_enabled = connection_string.lower().startswith("couchbases://")
    protocol = "https" if is_tls_enabled else "http"
    port = 19102 if is_tls_enabled else 9102

    logger.info(
        f"TLS {'enabled' if is_tls_enabled else 'disabled'}, using {protocol.upper()} with port {port}"
    )

    return protocol, port


def _determine_ssl_verification(
    connection_string: str, ca_cert_path: str | None
) -> bool | str:
    """Determine SSL verification setting based on connection type.

    Args:
        connection_string: Couchbase connection string
        ca_cert_path: Optional user-provided CA certificate path

    Returns:
        SSL verification setting (bool or path to cert)
    """
    is_tls_enabled = connection_string.lower().startswith("couchbases://")
    is_capella_connection = connection_string.lower().endswith(".cloud.couchbase.com")

    # Priority 1: Capella connections always use Capella root CA
    if is_capella_connection:
        capella_ca = _get_capella_root_ca_path()
        if os.path.exists(capella_ca):
            logger.info(
                f"Capella connection detected, using Capella root CA for SSL verification: {capella_ca}"
            )
            return capella_ca
        # Fall back to system CA bundle if Capella CA not found
        logger.warning(
            f"Capella CA certificate not found at {capella_ca}, "
            "falling back to system CA bundle"
        )
        return True

    # Priority 2: For non-Capella TLS connections, use provided cert or system CA bundle
    if is_tls_enabled:
        if ca_cert_path:
            logger.info(
                f"Using provided CA certificate for SSL verification: {ca_cert_path}"
            )
            return ca_cert_path
        logger.info("Using system CA bundle for SSL verification")
        return True

    # Priority 3: For non-TLS connections (HTTP), disable SSL verification
    logger.info("Non-TLS connection, SSL verification disabled")
    return False


def fetch_indexes_from_rest_api(
    connection_string: str,
    username: str,
    password: str,
    bucket_name: str | None = None,
    scope_name: str | None = None,
    collection_name: str | None = None,
    ca_cert_path: str | None = None,
    timeout: int = 30,
    cluster: Any | None = None,
) -> list[dict[str, Any]]:
    """Fetch indexes from Couchbase Index Service REST API.

    Uses the /getIndexStatus endpoint to retrieve index information.
    This endpoint returns indexes with their definitions directly from the Index Service.

    Args:
        connection_string: Couchbase connection string (can have multiple hosts)
        username: Username for authentication
        password: Password for authentication
        bucket_name: Optional bucket name to filter indexes
        scope_name: Optional scope name to filter indexes
        collection_name: Optional collection name to filter indexes
        ca_cert_path: Optional path to CA certificate for SSL verification.
                     If not provided and using couchbases://, will use Capella root CA.
        timeout: Request timeout in seconds (default: 30)
        cluster: Optional Couchbase cluster object to use for finding index service host.
                If provided, will use ping to find the node with index service.

    Returns:
        List of index status dictionaries containing name, definition, and other metadata
    """
    try:
        # Determine target host for the REST API call
        host, _ = _determine_target_host(connection_string, cluster)

        # Get protocol and port
        protocol, port = _get_protocol_and_port(connection_string)

        # Build the REST API URL
        url = f"{protocol}://{host}:{port}/getIndexStatus"

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
        verify_ssl = _determine_ssl_verification(connection_string, ca_cert_path)

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
