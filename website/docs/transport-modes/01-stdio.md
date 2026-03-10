---
sidebar_position: 1
title: stdio (Default)
---

# stdio Transport Mode

The default transport mode. The MCP client launches the server as a subprocess and communicates via standard input/output (stdin/stdout).

## Usage

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

## When to Use

- **Recommended for most use cases** — simplest setup, no network configuration needed.
- The server runs as a child process of the MCP client.
- One server instance per client session.

## Explicitly Setting Transport

You can explicitly set the transport mode if needed:

```bash
uvx couchbase-mcp-server --transport=stdio
```

Or via environment variable:

```bash
CB_MCP_TRANSPORT=stdio
```
