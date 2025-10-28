"""
Health check utilities.

This module contains helper functions for processing health check and diagnostics data.
"""

import logging
from typing import Any

from utils.connection import connect_to_bucket
from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.health")


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
