---
sidebar_position: 4
title: Transport Modes
---

# Transport Modes

The MCP server supports three transport modes: **stdio** (default), **Streamable HTTP**, and **SSE** (deprecated).

---

## stdio (Default)

The default transport mode. The MCP client launches the server as a subprocess and communicates via standard input/output (stdin/stdout).

### Usage

This is the default mode — no additional configuration is needed. When you configure the server in an MCP client like Claude Desktop or Cursor, the client handles launching and managing the server process.

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "username",
        "CB_PASSWORD": "password"
      }
    }
  }
}
```

### When to Use

- **Recommended for most use cases** — simplest setup, no network configuration needed.
- The server runs as a child process of the MCP client.
- One server instance per client session.

### Explicitly Setting Transport

You can explicitly set the transport mode if needed:

```bash
uvx couchbase-mcp-server --transport=stdio
```

Or via environment variable:

```bash
CB_MCP_TRANSPORT=stdio
```

---

## Streamable HTTP

The MCP Server can be run in [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports#streamable-http) transport mode, which allows multiple clients to connect to the same server instance via HTTP.

:::note
Check if your [MCP client](https://modelcontextprotocol.io/clients) supports Streamable HTTP transport before using this mode.
:::

:::warning
This mode does not include authorization support.
:::

### Usage

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

### MCP Client Configuration

```json
{
  "mcpServers": {
    "couchbase-http": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

### When to Use

- When you need **multiple clients** to connect to the same server instance.
- When the server runs as a **standalone service** (e.g., in a container).
- When the MCP client supports HTTP transport (e.g., Cursor).

---

## SSE (Deprecated)

:::warning Deprecated
SSE mode has been [deprecated](https://modelcontextprotocol.io/docs/concepts/transports#server-sent-events-sse-deprecated) by the MCP specification. Use [Streamable HTTP](#streamable-http) instead.
:::

The MCP Server can be run in [Server-Sent Events (SSE)](https://modelcontextprotocol.io/specification/2024-11-05/basic/transports#http-with-sse) transport mode.

### Usage

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

### MCP Client Configuration

```json
{
  "mcpServers": {
    "couchbase-sse": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```
