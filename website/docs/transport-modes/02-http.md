---
sidebar_position: 2
title: Streamable HTTP
---

# Streamable HTTP Transport Mode

The MCP Server can be run in [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports#streamable-http) transport mode, which allows multiple clients to connect to the same server instance via HTTP.

:::note
Check if your [MCP client](https://modelcontextprotocol.io/clients) supports Streamable HTTP transport before using this mode.
:::

:::warning
This mode does not include authorization support.
:::

## Usage

Start the server in HTTP mode:

```bash
uvx couchbase-mcp-server \
  --connection-string='<couchbase_connection_string>' \
  --username='<database_username>' \
  --password='<database_password>' \
  --read-only-mode=true \
  --transport=http
```

The server will be available at `http://localhost:8000/mcp`.

By default, the server runs on port 8000. Configure the port with `--port` or `CB_MCP_PORT`.

## MCP Client Configuration

```json
{
  "mcpServers": {
    "couchbase-http": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

## When to Use

- When you need **multiple clients** to connect to the same server instance.
- When the server runs as a **standalone service** (e.g., in a container).
- When the MCP client supports HTTP transport (e.g., Cursor).
