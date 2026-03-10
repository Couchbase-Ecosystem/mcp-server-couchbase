---
sidebar_position: 4
title: Quick Start
---

# Quick Start

Get the Couchbase MCP Server running in under 5 minutes.

## 1. Ensure Prerequisites

- Python 3.10+ installed
- [uv](https://docs.astral.sh/uv/) installed
- A Couchbase cluster running (e.g., [Capella free tier](https://docs.couchbase.com/cloud/get-started/create-account.html))
- An MCP client installed (e.g., [Claude Desktop](https://claude.ai/download))

## 2. Configure Your MCP Client

Add the following to your MCP client's configuration file:

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "your-username",
        "CB_PASSWORD": "your-password"
      }
    }
  }
}
```

Replace the environment variable values with your actual Couchbase cluster credentials.

For client-specific configuration file locations, see the [MCP Client Guides](/docs/client-guides/claude-desktop).

## 3. Start Using It

Restart your MCP client and start asking questions like:

- "List all buckets in my cluster"
- "Show me the schema of the `airline` collection in `travel-sample`"
- "Run a SQL++ query to find all airlines from the United States"
- "Check my cluster health"
- "What are the longest running queries?"

## Default Security

By default, the server runs in **read-only mode** (`CB_MCP_READ_ONLY_MODE=true`). This means:

- All KV write tools (upsert, insert, replace, delete) are **not loaded**
- SQL++ queries that modify data are **blocked**

To enable write operations, set `CB_MCP_READ_ONLY_MODE=false` in your configuration. See [Read-Only Mode](/docs/configuration/read-only-mode) for details.
