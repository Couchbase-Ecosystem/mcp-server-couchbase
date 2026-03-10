---
sidebar_position: 3
title: SSE (Deprecated)
---

# SSE Transport Mode

:::warning Deprecated
SSE mode has been [deprecated](https://modelcontextprotocol.io/docs/concepts/transports#server-sent-events-sse-deprecated) by the MCP specification. Use [Streamable HTTP](/docs/transport-modes/http) instead.
:::

The MCP Server can be run in [Server-Sent Events (SSE)](https://modelcontextprotocol.io/specification/2024-11-05/basic/transports#http-with-sse) transport mode.

## Usage

```bash
uvx couchbase-mcp-server \
  --connection-string='<couchbase_connection_string>' \
  --username='<database_username>' \
  --password='<database_password>' \
  --read-only-mode=true \
  --transport=sse
```

The server will be available at `http://localhost:8000/sse`.

By default, the server runs on port 8000. Configure the port with `--port` or `CB_MCP_PORT`.

## MCP Client Configuration

```json
{
  "mcpServers": {
    "couchbase-sse": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```
