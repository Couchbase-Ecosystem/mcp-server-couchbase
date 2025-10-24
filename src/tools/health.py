"""
Tools for cluster health monitoring.

This module contains tools for health check operations on the Couchbase cluster,
including ping-based health checks and service diagnostics.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.connection import connect_to_bucket
from utils.constants import MCP_SERVER_NAME
from utils.context import get_cluster_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.health")


def _get_endpoint_latency(endpoint: Any) -> int | None:
    """Extract latency value from endpoint in microseconds.

    The SDK may provide latency as timedelta or raw microseconds.
    """
    if hasattr(endpoint, "latency"):
        latency = endpoint.latency
        # Convert timedelta to microseconds if needed
        if hasattr(latency, "total_seconds"):
            return int(latency.total_seconds() * 1_000_000)
        return latency
    if hasattr(endpoint, "latency_us"):
        return endpoint.latency_us
    return None


def _get_endpoint_state(endpoint: Any) -> str:
    """Extract state from endpoint as string."""
    if hasattr(endpoint.state, "name"):
        return endpoint.state.name
    return str(endpoint.state)


def _format_ping_endpoint(endpoint: Any) -> dict[str, Any]:
    """Format endpoint data from ping result."""
    return {
        "remote": endpoint.remote,
        "local": endpoint.local,
        "latency_us": _get_endpoint_latency(endpoint),
        "state": _get_endpoint_state(endpoint),
        "namespace": getattr(endpoint, "namespace", None),
    }


def _format_diagnostics_endpoint(endpoint: Any) -> dict[str, Any]:
    """Format endpoint data from diagnostics result."""
    return {
        "remote": endpoint.remote,
        "local": endpoint.local,
        "last_activity_us": endpoint.last_activity_us,
        "state": _get_endpoint_state(endpoint),
        "namespace": getattr(endpoint, "namespace", None),
    }


def _process_endpoints(
    endpoints: dict[str, list[Any]], formatter: callable
) -> dict[str, dict[str, Any]]:
    """Process endpoints dictionary and format each endpoint using provided formatter."""
    services = {}
    for service_type, endpoint_list in endpoints.items():
        services[service_type] = {
            "count": len(endpoint_list),
            "endpoints": [formatter(ep) for ep in endpoint_list],
        }
    return services


def _get_ping_health(bucket_name: str, cluster: Any) -> dict[str, Any]:
    """Get health status from bucket perspective using active ping."""
    bucket = connect_to_bucket(cluster, bucket_name)
    ping_result = bucket.ping()

    timestamp = "N/A"
    if hasattr(ping_result, "json"):
        timestamp = ping_result.json().get("timestamp", "N/A")

    return {
        "cluster_id": ping_result.id,
        "timestamp": timestamp,
        "check_type": "ping",
        "source": f"bucket:{bucket_name}",
        "services": _process_endpoints(ping_result.endpoints, _format_ping_endpoint),
    }


def _get_diagnostics_health(cluster: Any) -> dict[str, Any]:
    """Get health status from cluster perspective using diagnostics."""
    diagnostics_result = cluster.diagnostics()

    return {
        "cluster_id": diagnostics_result.id,
        "sdk": diagnostics_result.sdk,
        "check_type": "diagnostics",
        "source": "cluster",
        "services": _process_endpoints(
            diagnostics_result.endpoints, _format_diagnostics_endpoint
        ),
    }


def get_cluster_health_and_services(
    ctx: Context, bucket_name: str | None = None
) -> dict[str, Any]:
    """Get cluster health status and list of all running services.

    This tool provides comprehensive health monitoring by:
    - Getting health status of all running services with latency information (via ping)
    - Listing all services running on the cluster with their endpoints
    - Showing connection status and node information for each service

    If bucket_name is provided, it actively pings services from the bucket perspective (real-time).
    Otherwise, it uses cluster-level diagnostics for faster cached state.

    Returns:
    - Cluster health status with service-level details
    - List of all available services and their endpoints
    - Latency measurements (for ping mode)
    - Connection state for each endpoint
    """
    try:
        cluster = get_cluster_connection(ctx)

        if bucket_name:
            result = _get_ping_health(bucket_name, cluster)
        else:
            result = _get_diagnostics_health(cluster)

        return {
            "status": "success",
            "data": result,
        }
    except Exception as e:
        logger.error(f"Error getting cluster health: {e}")
        return {
            "status": "error",
            "error": str(e),
            "message": "Failed to get cluster health and services information",
        }
