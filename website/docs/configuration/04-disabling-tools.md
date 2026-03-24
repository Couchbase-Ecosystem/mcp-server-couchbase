---
sidebar_position: 4
title: Disabling Tools
---

# Disabling Tools

You can disable specific tools to prevent them from being loaded and exposed to the MCP client; disabled tools will not appear in tool discovery, cannot be invoked by the LLM, and help cut noise and unnecessary token consumption.

## Supported Formats

### Comma-Separated List

```bash
# Environment variable
CB_MCP_DISABLED_TOOLS="upsert_document_by_id, delete_document_by_id"

# Command line
uvx couchbase-mcp-server --disabled-tools "upsert_document_by_id, delete_document_by_id"
```

### File Path (One Tool Per Line)

```bash
# Environment variable
CB_MCP_DISABLED_TOOLS=disabled_tools.txt

# Command line
uvx couchbase-mcp-server --disabled-tools disabled_tools.txt
```

File format example (`disabled_tools.txt`):

```text
# Write operations
upsert_document_by_id
delete_document_by_id

# Index advisor
get_index_advisor_recommendations
```

Lines starting with `#` are treated as comments and ignored.

## MCP Client Configuration Examples

**Using comma-separated list:**

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
        "CB_MCP_DISABLED_TOOLS": "upsert_document_by_id,delete_document_by_id"
      }
    }
  }
}
```

**Using file path (recommended for many tools):**

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
        "CB_MCP_DISABLED_TOOLS": "/path/to/disabled_tools.txt"
      }
    }
  }
}
```

## Important Security Note

:::warning
Disabling tools alone does not guarantee that certain operations cannot be performed.
:::

The underlying database user's RBAC (Role-Based Access Control) permissions are the authoritative security control. For example, even if you disable `upsert_document_by_id` and `delete_document_by_id`, data modifications can still occur via the `run_sql_plus_plus_query` tool using SQL++ DML statements (INSERT, UPDATE, DELETE, MERGE) unless:
- `CB_MCP_READ_ONLY_MODE` is set to `true` (default), **OR**
- The database user lacks the necessary RBAC permissions

:::tip Best Practice
Always configure appropriate RBAC permissions on your Couchbase user credentials as the primary security measure. See [RBAC for Couchbase Server](https://docs.couchbase.com/server/current/manage/manage-security/manage-users-and-roles.html) or [RBAC for Capella](https://docs.couchbase.com/cloud/clusters/manage-database-users.html). Use tool disabling as an additional layer to guide LLM behavior and reduce the attack surface, not as the sole security control.
:::
