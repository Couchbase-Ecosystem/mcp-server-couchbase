# Elicitation / Confirmation for Tool Calls

The `CB_MCP_CONFIRMATION_REQUIRED` environment variable enables user confirmation prompts for tools marked as requiring confirmation. This allows users to double-check the tool call before the LLM executes the actions.

## How It Works

When a tool requires confirmation, the server sends an [elicitation](https://modelcontextprotocol.io/docs/concepts/elicitation) request to the client.

**Clients with elicitation support:**

1. Prompt the user for confirmation.

2. Send the user's response back to the server.

**Clients without elicitation support:** The tool executes **without confirmation**.

:::important
Full functionality requires client support for [elicitation](https://modelcontextprotocol.io/clients).
:::

## Configuration

| Environment Variable | CLI Argument | Description | Default |
| --- | --- | --- | --- |
| `CB_MCP_CONFIRMATION_REQUIRED` | `--confirmation-required` | Comma-separated list or file path of tool names that require elicitation/user confirmation before execution | None |

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
CB_MCP_CONFIRMATION_REQUIRED=/path/to/confirmation_required_tools.txt

# Command line
uvx couchbase-mcp-server --confirmation-required /path/to/confirmation_required_tools.txt
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
        "CB_MCP_CONFIRMATION_REQUIRED": "upsert_document_by_id,delete_document_by_id"
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
        "CB_MCP_CONFIRMATION_REQUIRED": "/path/to/confirmation_required_tools.txt"
      }
    }
  }
}
```

## Important Limitations

- Setting `CB_MCP_CONFIRMATION_REQUIRED` for a tool that **did not load** has no impact, as the tool is not available. A tool does not load if it is explicitly listed under the `disabled_tools` configuration or if **READ_ONLY** mode is enabled and the tool is not a **READ_ONLY** tool.

:::warning
The confirmation_required setting applies explicitly to tools, not to individual actions (such as read, update, or delete operations).

For example, if confirmation_required is enabled for the `delete_document_by_id` tool, the MCP server prompts for confirmation only when the MCP client selects that specific tool. No confirmation is requested if the client selects a different tool, such as `run_sql_plus_plus_query` to delete documents.
:::
