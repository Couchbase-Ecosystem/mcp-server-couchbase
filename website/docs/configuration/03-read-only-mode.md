---
sidebar_position: 3
title: Read-Only Mode
---

# Read-Only Mode

The MCP server provides configuration options for controlling write operations, ensuring safe interaction between LLMs and your database.

## `CB_MCP_READ_ONLY_MODE` (Recommended)

This is the primary security control:

- **When `true` (default)**: All write operations are disabled. KV write tools (upsert, insert, replace, delete) are **not loaded** and will not be available to the LLM. SQL++ queries that modify data are also blocked.
- **When `false`**: KV write tools are loaded and available. SQL++ write queries are allowed (unless blocked by `CB_MCP_READ_ONLY_QUERY_MODE`).

## `CB_MCP_READ_ONLY_QUERY_MODE` (Deprecated)

:::warning Deprecated
This option only controls SQL++ query-based writes but does not prevent KV write operations. Use `CB_MCP_READ_ONLY_MODE` instead for comprehensive protection.
:::

## Mode Behavior Truth Table

| `READ_ONLY_MODE` | `READ_ONLY_QUERY_MODE` | Result |
|---|---|---|
| `true` | `true` | Read-only KV and Query operations. All writes disabled. |
| `true` | `false` | Read-only KV and Query operations. All writes disabled. |
| `false` | `true` | Only Query writes disabled. KV writes allowed. |
| `false` | `false` | All KV and Query operations allowed. |

:::important
When `READ_ONLY_MODE` is `true`, it takes precedence and disables all write operations regardless of `READ_ONLY_QUERY_MODE` setting. This is the recommended safe default to prevent inadvertent data modifications by LLMs.
:::

## Configuration Example

To enable write operations:

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "username",
        "CB_PASSWORD": "password",
        "CB_MCP_READ_ONLY_MODE": "false"
      }
    }
  }
}
```
