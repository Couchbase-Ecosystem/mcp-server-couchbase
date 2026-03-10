---
sidebar_position: 1
title: Cluster & Health Tools
---

# Cluster & Health Tools

Tools for monitoring the MCP server status, testing connections, and checking cluster health.

**Source:** [`src/tools/server.py`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/server.py)

---

## `get_server_configuration_status`

Get the MCP server status and configuration without establishing a connection. Useful for verifying the server is running and checking its settings.

**Parameters:** None

**Returns:** A dictionary containing:
- `server_name` — Name of the MCP server
- `status` — Server status (e.g., `"running"`)
- `configuration` — Current settings including connection string, username, read-only mode status, and whether certificates are configured (sensitive values like passwords are not exposed)
- `connections` — Whether a cluster connection is currently established

---

## `test_cluster_connection`

Test the connection to the Couchbase cluster and optionally to a specific bucket. Establishes the connection if not already established.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | No | Bucket name to test connectivity. If not provided, only cluster-level connection is tested. |

**Returns:** A dictionary containing:
- `status` — `"success"` or `"error"`
- `cluster_connected` — Whether the cluster connection succeeded
- `bucket_connected` — Whether the bucket connection succeeded
- `bucket_name` — The bucket tested (if provided)
- `message` — Human-readable status message
- `error` — Error details (only on failure)

---

## `get_cluster_health_and_services`

Get cluster health status and a list of all running services with latency information via ping.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | No | If provided, pings services from the perspective of the specified bucket. Otherwise uses cluster-level ping. |

**Returns:** A dictionary containing:
- `status` — `"success"` or `"error"`
- `data` — Ping results with service-level connection details and latency measurements (on success)
- `error` — Error details (on failure)
