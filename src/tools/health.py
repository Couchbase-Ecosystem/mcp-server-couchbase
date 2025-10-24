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
            # Get health from bucket perspective using active ping
            bucket = connect_to_bucket(cluster, bucket_name)
            try:
                # Ping all services to get active health status
                ping_result = bucket.ping()

                # Extract comprehensive information
                result = {
                    "cluster_id": ping_result.id,
                    "timestamp": ping_result.json()["timestamp"]
                    if hasattr(ping_result, "json")
                    else "N/A",
                    "check_type": "ping",
                    "source": f"bucket:{bucket_name}",
                    "services": {},
                }

                # Process each service type and its endpoints
                endpoints = ping_result.endpoints
                for service_type, endpoint_list in endpoints.items():
                    result["services"][service_type] = {
                        "count": len(endpoint_list),
                        "endpoints": [],
                    }

                    for endpoint in endpoint_list:
                        result["services"][service_type]["endpoints"].append(
                            {
                                "remote": endpoint.remote,
                                "local": endpoint.local,
                                "latency_us": endpoint.latency_us,
                                "state": endpoint.state.name
                                if hasattr(endpoint.state, "name")
                                else str(endpoint.state),
                                "namespace": getattr(endpoint, "namespace", None),
                            }
                        )

                return {
                    "status": "success",
                    "data": result,
                }
            except Exception as e:
                logger.error(f"Error pinging from bucket {bucket_name}: {e}")
                raise
        else:
            # Get health from cluster perspective using diagnostics (cached, faster)
            try:
                diagnostics_result = cluster.diagnostics()

                # Extract comprehensive information
                result = {
                    "cluster_id": diagnostics_result.id,
                    "sdk": diagnostics_result.sdk,
                    "check_type": "diagnostics",
                    "source": "cluster",
                    "services": {},
                }

                # Process each service type and its endpoints
                endpoints = diagnostics_result.endpoints
                for service_type, endpoint_list in endpoints.items():
                    result["services"][service_type] = {
                        "count": len(endpoint_list),
                        "endpoints": [],
                    }

                    for endpoint in endpoint_list:
                        result["services"][service_type]["endpoints"].append(
                            {
                                "remote": endpoint.remote,
                                "local": endpoint.local,
                                "last_activity_us": endpoint.last_activity_us,
                                "state": endpoint.state.name
                                if hasattr(endpoint.state, "name")
                                else str(endpoint.state),
                                "namespace": getattr(endpoint, "namespace", None),
                            }
                        )

                return {
                    "status": "success",
                    "data": result,
                }
            except Exception as e:
                logger.error(f"Error getting cluster diagnostics: {e}")
                raise

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "message": "Failed to get cluster health and services information",
        }
