---
sidebar_position: 6
title: Confirmation Required
---

# Confirmation Required

The `CB_MCP_CONFIRMATION_REQUIRED` environment variable enables user confirmation prompts for tools marked as requiring confirmation. This allows users to double-check before the LLM executes important actions.

## How It Works

When a tool requires confirmation, the server sends an [elicitation](https://modelcontextprotocol.io/docs/concepts/elicitation) request to the client.

**Clients with elicitation support:**

1. Prompt the user for confirmation.
2. Send the user's response back to the server.

**Clients without elicitation support:** The tool executes **without confirmation**.

:::important
Full functionality requires client support for [elicitation](https://modelcontextprotocol.io/docs/concepts/elicitation).
:::

## Configuration

| Environment Variable | CLI Argument | Description | Default |
|---|---|---|---|
| `CB_MCP_CONFIRMATION_REQUIRED` | `--confirmation-required` | Comma-separated list of tool names that require user confirmation before execution | None |

### Example

```json
{
  "env": {
    "CB_MCP_CONFIRMATION_REQUIRED": "upsert_document_by_id,delete_document_by_id"
  }
}
```

## Supported Formats

### Comma-Separated List

```bash
# Environment variable
CB_MCP_CONFIRMATION_REQUIRED="delete_document_by_id, upsert_document_by_id"

# Command line
uvx couchbase-mcp-server --confirmation-required "delete_document_by_id, upsert_document_by_id"
```

### File Path (One Tool Per Line)

```bash
# Environment variable
CB_MCP_CONFIRMATION_REQUIRED=confirmation_required_tools.txt

# Command line
uvx couchbase-mcp-server --confirmation-required confirmation_required_tools.txt
```

File format example (`confirmation_required_tools.txt`):

```text
# Write operations
upsert_document_by_id
delete_document_by_id

# Replace operations
replace_document_by_id
```

Lines starting with `#` are treated as comments and ignored.

## Important Limitations

- Setting `CB_MCP_CONFIRMATION_REQUIRED` for a tool that **didn't load** has no impact, as no confirmation is needed for unloaded tools. A tool doesn't load if it is explicitly listed under the `disabled_tools` configuration or if **READ_ONLY** mode is enabled and the tool is not a **READ_ONLY** tool.

:::warning
The confirmation_required setting applies explicitly to tools, not to individual actions (such as read, update, or delete operations).

For example, if confirmation_required is enabled for the `delete_document_by_id` tool, the MCP server prompts for confirmation only when the MCP client selects that specific tool. No confirmation is requested if the client selects a different tool, such as `run_sql_plus_plus_query`.
:::
