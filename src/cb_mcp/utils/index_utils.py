"""
Utility functions for index operations.

This module contains helper functions for working with Couchbase indexes.
"""

import logging
import os
from collections.abc import Mapping
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


def validate_connection_settings(settings: Mapping[str, Any]) -> None:
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


# Mapping from REST API /getIndexStatus status strings to SQL++ query service states.
# Source: https://github.com/couchbase/indexing/blob/master/secondary/indexer/request_handler.go
# The REST API produces these status strings from internal indexer states.
# SQL++ system:all_indexes uses 7 canonical states:
# online, deferred, building, pending, offline, abridged, scheduled for creation
_REST_STATUS_TO_QUERY_STATE: dict[str, str] = {
    "Ready": "online",
    # "Created" can mean deferred (WITH {\"defer_build\":true}) or pending (waiting to build).
    # Resolved at call-site by inspecting the definition field — see map_rest_status_to_query_state.
    "Created": "pending",
    "Building": "building",
    "Moving": "building",
    "Error": "offline",
    "Paused": "offline",
    "Warmup": "pending",
    "Not Available": "offline",
    "Retrying": "offline",
    "Scheduled for Creation": "scheduled for creation",
    "Training": "building",
    "Graph Building": "building",
    # "Scheduled for build" and "Training complete, scheduled for build" come from
    # INDEX_STATE_READY (same as "Created"). Use defer_build check at call-site.
    "Scheduled for build": "pending",
    "Training complete, scheduled for build": "pending",
}


def map_rest_status_to_query_state(rest_status: str, definition: str = "") -> str:
    """Map a REST API index status string to its SQL++ query service equivalent.

    The REST API /getIndexStatus endpoint returns status strings like
    "Ready", "Created", "Building", etc. This function normalizes them
    to the SQL++ system:all_indexes state values: online, deferred, building,
    pending, offline, scheduled for creation.

    For statuses with qualifiers (e.g. "Building (Upgrading)"), the prefix
    before the parenthesis is used for lookup.

    For "Created" status, the definition field is inspected: if it contains
    ``defer_build`` the index is explicitly deferred; otherwise it is pending
    (created normally but not yet built).

    Args:
        rest_status: Status string from the REST API.
        definition: The raw CREATE INDEX definition string from the REST API.
            Used to distinguish deferred from pending for "Created" indexes.

    Returns:
        Normalized SQL++ query service state string.
    """
    # Direct lookup first
    if rest_status in _REST_STATUS_TO_QUERY_STATE:
        # "Created", "Scheduled for build", and "Training complete, scheduled for build"
        # all share INDEX_STATE_READY. Use defer_build in definition to distinguish
        # deferred (explicit defer) from pending (waiting to build).
        if rest_status in (
            "Created",
            "Scheduled for build",
            "Training complete, scheduled for build",
        ):
            return "deferred" if "defer_build" in definition.lower() else "pending"
        return _REST_STATUS_TO_QUERY_STATE[rest_status]

    # Handle qualified statuses like "Building (Upgrading)", "Created (Downgrading)"
    prefix = rest_status.split("(")[0].strip()
    if prefix in _REST_STATUS_TO_QUERY_STATE:
        if prefix in (
            "Created",
            "Scheduled for build",
            "Training complete, scheduled for build",
        ):
            return "deferred" if "defer_build" in definition.lower() else "pending"
        return _REST_STATUS_TO_QUERY_STATE[prefix]

    # Unknown status — there's a problem in fetching the status (the value
    # returned isn't one we recognize). Log a warning so it can be reported,
    # but still return a usable value (lowercased) so the caller doesn't break.
    logger.warning(
        "Encountered unexpected REST API index status %r. There's a problem "
        "in fetching the status. Please report this issue. Returning the "
        "status lowercased.",
        rest_status,
    )
    return rest_status.lower()


def _raw_fallback(idx: dict[str, Any], reason: str) -> dict[str, Any]:
    """Build a fallback response when an index row cannot be fully processed.

    Returns the raw index data as-is (no processing) under ``raw_index_stats``
    and an ``error`` field explaining what went wrong. Also logs a warning so
    the issue is surfaced server-side.
    """
    logger.warning(
        "Failed to process index data (%s). There's a problem in fetching the "
        "index information. Please report this issue. Returning raw index data "
        "as-is.",
        reason,
    )
    return {
        "error": (
            f"Failed to process index data: {reason}. Returning raw row "
            "under 'raw_index_stats' — please report this issue."
        ),
        "raw_index_stats": idx,
    }


def _validate_rest_row(idx: dict[str, Any]) -> str | None:
    """Return an error reason if *idx* from the REST API is missing required fields."""
    if not (idx.get("indexName") or idx.get("name")):
        return "missing 'indexName'/'name' field"
    definition = idx.get("definition")
    if not definition or not isinstance(definition, str):
        return "missing or invalid 'definition' field"
    if not idx.get("status"):
        return "missing 'status' field"
    if not idx.get("bucket"):
        return "missing 'bucket' field"
    if "lastScanTime" not in idx:
        return "missing 'lastScanTime' field"
    return None


def _validate_query_row(idx: dict[str, Any]) -> str | None:
    """Return an error reason if *idx* from system:indexes is missing required fields."""
    if not idx.get("name"):
        return "missing 'name' field"
    metadata = idx.get("metadata")
    if not isinstance(metadata, dict) or not metadata.get("definition"):
        return "missing or invalid 'metadata.definition' field"
    if not idx.get("state"):
        return "missing 'state' field"
    for field in ("bucket", "scope", "collection"):
        if not idx.get(field):
            return f"missing {field!r} field (LET clause may not have run)"
    if "last_scan_time" not in metadata:
        return "missing 'metadata.last_scan_time' field"
    return None


def process_index_data_from_rest_api(
    idx: dict[str, Any],
    return_raw_index_stats: bool = False,
) -> dict[str, Any]:
    """Process raw index data from the REST API into formatted index info.

    Args:
        idx: Raw index data from the /getIndexStatus API
        return_raw_index_stats: If True, return the unprocessed index row.

    Returns:
        Formatted index info dictionary, or the unprocessed input row when
        ``return_raw_index_stats`` is True. If a required field (name,
        definition, or status) is missing or invalid, returns a fallback dict
        containing ``error`` and the unprocessed raw row under
        ``raw_index_stats``.
    """
    if return_raw_index_stats:
        return idx

    error = _validate_rest_row(idx)
    if error:
        return _raw_fallback(idx, error)

    name = idx.get("indexName") or idx.get("name")
    raw_definition = idx["definition"]
    raw_status = idx["status"]

    index_info: dict[str, Any] = {
        "name": name,
        "definition": clean_index_definition(raw_definition),
        "status": map_rest_status_to_query_state(raw_status, raw_definition),
        "isPrimary": bool(idx.get("isPrimary", False)),
        "bucket": idx["bucket"],
    }

    if "scope" in idx:
        index_info["scope"] = idx["scope"]
    if "collection" in idx:
        index_info["collection"] = idx["collection"]

    index_info["lastScanTime"] = idx["lastScanTime"] or "NA"

    return index_info


def process_index_data_from_query(
    idx: dict[str, Any],
    return_raw_index_stats: bool = False,
) -> dict[str, Any]:
    """Process a row from ``system:indexes`` into formatted index info.

    Bucket / scope / collection are normalized in SQL++ by
    ``fetch_indexes_via_query_service`` via a LET clause, so legacy
    bucket-level indexes (only ``keyspace_id`` present) and modern scoped
    indexes (``bucket_id`` + ``scope_id`` + ``keyspace_id``) both arrive
    here with the same enriched shape — no branching needed here.

    Args:
        idx: A single index row from ``system:indexes`` with ``bucket`` /
            ``scope`` / ``collection`` already injected by the LET clause
            in the fetch query.
        return_raw_index_stats: If True, return the unprocessed index row.

    Returns:
        Formatted index info dictionary, or the unprocessed input row when
        ``return_raw_index_stats`` is True. If a required field (name,
        metadata.definition, or state) is missing or invalid, returns a
        fallback dict containing ``error`` and the unprocessed raw row under
        ``raw_index_stats``.
    """
    if return_raw_index_stats:
        return idx

    error = _validate_query_row(idx)
    if error:
        return _raw_fallback(idx, error)

    metadata = idx["metadata"]

    return {
        "name": idx["name"],
        "definition": metadata["definition"],
        "status": idx["state"],
        "bucket": idx["bucket"],
        "scope": idx["scope"],
        "collection": idx["collection"],
        "isPrimary": bool(idx.get("is_primary", False)),
        "lastScanTime": metadata["last_scan_time"],
    }


def parse_major_version(version_str: str | None) -> int:
    """Extract the integer major version from a Couchbase version string.

    Examples:
        - "8.0.0-1928-enterprise" -> 8
        - "7.6.0"                 -> 7

    Args:
        version_str: Node ``version`` string returned by the cluster, such as a value from ``cluster_info().nodes``.

    Returns:
        Major version as int.

    Raises:
        ValueError: If *version_str* is empty, None, or cannot be parsed.
    """
    if not version_str:
        raise ValueError("version_str is empty or None")
    major_version = version_str.strip().split(".", 1)[0]
    # Handle prefixes like "v8" defensively.
    major_version = major_version.lstrip("vV")
    try:
        return int(major_version)
    except ValueError:
        raise ValueError(f"Cannot parse major version from {version_str!r}") from None


async def resolve_cluster_major_version(cluster: Any) -> int:
    """Detect the cluster's major version via the SDK.

    Reads the per-node ``version`` field from ``cluster.cluster_info().nodes``
    (Python SDK 4.1+) and returns the *minimum* major version across all nodes
    so we only enable the 8.x+ query-service path when every node supports it.

    The high-level helper properties (``server_version`` /
    ``server_version_short`` / ``server_version_full``) are intentionally not
    used: the SDK collapses them to ``None`` whenever the cluster reports
    mixed node versions, which is exactly the case where we still need an
    answer. Each node entry, in contrast, always carries a ``version`` string.

    Args:
        cluster: An already-connected Couchbase ``Cluster`` instance.

    Raises if cluster_info() fails — callers should not silently degrade
    when version detection is unavailable.
    """
    info = await cluster.cluster_info()

    nodes = info.nodes or []
    versions: list[str] = []
    for node in nodes:
        if isinstance(node, dict):
            version = node.get("version")
        else:
            version = getattr(node, "version", None)
        if version:
            versions.append(str(version))

    if not versions:
        raise RuntimeError(
            "cluster_info() reported no nodes — cannot determine cluster version"
        )

    majors = [parse_major_version(v) for v in versions]
    min_major = min(majors)

    logger.info(f"Detected cluster node versions={versions} (min major={min_major})")
    return min_major


def _get_capella_root_ca_path() -> str:
    """Get the path to the Capella root CA certificate.

    Uses importlib.resources to locate the certificate file, which works when the package is installed with fallback for development.

    Returns:
        Path to the Capella root CA certificate file.
    """
    try:
        # Use importlib.resources to get the certificate path (works for installed packages)
        cert_file = files("cb_mcp.certs").joinpath("capella_root_ca.pem")
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


async def fetch_indexes_from_rest_api(
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
    async with httpx.AsyncClient(verify=verify_ssl, timeout=timeout) as client:
        for host in hosts:
            try:
                url = f"{protocol}://{host}:{port}/getIndexStatus"
                logger.info(
                    f"Attempting to fetch indexes from: {url} with params: {params}"
                )

                response = await client.get(
                    url,
                    params=params,
                    auth=(username, password),
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
