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

## Important Limitations

- Setting `CB_MCP_CONFIRMATION_REQUIRED` for a tool that **didn't load** has no impact, as no confirmation is needed for unloaded tools.
- Tools fail to load when:
  - Added to the `CB_MCP_DISABLED_TOOLS` configuration, or
  - Not included in the loaded tools when `CB_MCP_READ_ONLY_MODE` is enabled.

:::warning
The confirmation_required setting applies **specifically to tools**, not to individual actions (read/update/delete, etc.). Tool execution via SQL++ or the `run_sql_plus_plus_query` tool bypasses confirmation, even if confirmation_required is enabled for that tool.
:::
