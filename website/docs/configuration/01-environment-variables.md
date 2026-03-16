---
sidebar_position: 1
title: Environment Variables
---

# Environment Variables

The MCP server can be configured using environment variables or command line arguments.

## Configuration Reference

| Environment Variable | CLI Argument | Description | Default |
|---|---|---|---|
| `CB_CONNECTION_STRING` | `--connection-string` | Connection string to the Couchbase cluster | **Required** |
| `CB_USERNAME` | `--username` | Username for basic authentication | **Required (or mTLS)** |
| `CB_PASSWORD` | `--password` | Password for basic authentication | **Required (or mTLS)** |
| `CB_CLIENT_CERT_PATH` | `--client-cert-path` | Path to client certificate for mTLS | **Required if using mTLS** |
| `CB_CLIENT_KEY_PATH` | `--client-key-path` | Path to client key for mTLS | **Required if using mTLS** |
| `CB_CA_CERT_PATH` | `--ca-cert-path` | Path to server root certificate for TLS (self-signed/untrusted certs). Not required for Capella. | |
| `CB_MCP_READ_ONLY_MODE` | `--read-only-mode` | Prevent all data modifications (KV and Query) | `true` |
| `CB_MCP_READ_ONLY_QUERY_MODE` | `--read-only-query-mode` | **[DEPRECATED]** Prevent queries that modify data. Use `CB_MCP_READ_ONLY_MODE` instead. | `true` |
| `CB_MCP_TRANSPORT` | `--transport` | Transport mode: `stdio`, `http`, `sse` | `stdio` |
| `CB_MCP_HOST` | `--host` | Host for HTTP/SSE transport modes | `127.0.0.1` |
| `CB_MCP_PORT` | `--port` | Port for HTTP/SSE transport modes | `8000` |
| `CB_MCP_DISABLED_TOOLS` | `--disabled-tools` | Tools to disable (see [Disabling Tools](/configuration/disabling-tools)) | None |

## Authentication Priority

For authentication, you need **either**:
- Username and Password (basic authentication), **or**
- Client Certificate and Key paths (mTLS authentication)

If both are specified, client certificates take priority.

Optionally, you can specify a CA root certificate path to validate server certificates (useful for self-signed certificates).

## Checking the Version

```bash
uvx couchbase-mcp-server --version
```

---

## Transport Modes

The MCP server supports three transport modes: **stdio** (default), **Streamable HTTP**, and **SSE** (deprecated).

### stdio (Default)

The default transport mode. The MCP client launches the server as a subprocess and communicates via standard input/output (stdin/stdout).

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

**When to use:** Recommended for most use cases — simplest setup, no network configuration needed. The server runs as a child process of the MCP client with one server instance per client session.

To explicitly set the transport mode:

```bash
uvx couchbase-mcp-server --transport=stdio
```

Or via environment variable:

```bash
CB_MCP_TRANSPORT=stdio
```

### Streamable HTTP

The MCP Server can be run in [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports#streamable-http) transport mode, which allows multiple clients to connect to the same server instance via HTTP.

:::note
Check if your [MCP client](https://modelcontextprotocol.io/clients) supports Streamable HTTP transport before using this mode.
:::

:::warning
This mode does not include authorization support.
:::

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

**Client configuration:**

```json
{
  "mcpServers": {
    "couchbase-http": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

**When to use:**
- When you need **multiple clients** to connect to the same server instance.
- When the server runs as a **standalone service** (e.g., in a container).
- When the MCP client supports HTTP transport (e.g., Cursor).

### SSE (Deprecated)

:::warning Deprecated
SSE mode has been [deprecated](https://modelcontextprotocol.io/docs/concepts/transports#server-sent-events-sse-deprecated) by the MCP specification. Use [Streamable HTTP](#streamable-http) instead.
:::

The MCP Server can be run in [Server-Sent Events (SSE)](https://modelcontextprotocol.io/specification/2024-11-05/basic/transports#http-with-sse) transport mode.

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

**Client configuration:**

```json
{
  "mcpServers": {
    "couchbase-sse": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```
