# Read-Only Mode

The MCP server provides configuration options for controlling write operations, ensuring safe interaction between LLMs and your database. Use this mode to start in a safe default that prevents data mutations by not loading write-capable tools; see the [Security](../06-security.md) page for best practices. This mode is enabled by default.

## Affected Tools

When read-only mode is enabled, the following tools are affected:

| Tool | Description |
| ---- | ----------- |
| `upsert_document_by_id` | Insert or update a document by ID |
| `insert_document_by_id` | Insert a new document by ID |
| `replace_document_by_id` | Replace an existing document by ID |
| `delete_document_by_id` | Delete a document by ID |
| `run_sql_plus_plus_query` | Run SQL++ queries that modify data |


## Read-Only Mode (Recommended)

This is the primary security control (`CB_MCP_READ_ONLY_MODE`):

- **When `true` (default)**: All write operations are disabled. KV write tools (upsert, insert, replace, delete) are **not loaded** and will not be available to the LLM. SQL++ queries that modify data are also blocked.

- **When `false`**: KV write tools are loaded and available. SQL++ write queries are allowed (unless blocked by `CB_MCP_READ_ONLY_QUERY_MODE`).

## Read-Only Query Mode (Deprecated)

:::warning Deprecated
`CB_MCP_READ_ONLY_QUERY_MODE` only controls SQL++ query-based writes but does not prevent KV write operations. Use `CB_MCP_READ_ONLY_MODE` instead for comprehensive protection.
:::

## Mode Behavior Truth Table

| `READ_ONLY_MODE` | `READ_ONLY_QUERY_MODE` | Result |
| --- | --- | --- |
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

## Security Guidelines

- Read-only mode is a **defense-in-depth feature**, not the primary security boundary.

- The authoritative control is **Couchbase RBAC**: You should configure database user permissions so that the credentials used by the MCP server simply do not have data modification privileges if you want strong guarantees. See [RBAC for Couchbase Server](https://docs.couchbase.com/server/current/manage/manage-security/manage-users-and-roles.html) or [RBAC for Capella](https://docs.couchbase.com/cloud/clusters/manage-database-users.html).
