---
sidebar_position: 2
title: Claude Desktop
---

# Claude Desktop

Follow these steps to use the Couchbase MCP Server with [Claude Desktop](https://claude.ai/download).

## Setup

1. Open the Claude Desktop configuration file:
   - **Mac**: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

   For detailed instructions, see the [MCP quickstart guide](https://modelcontextprotocol.io/quickstart/user).

2. Add the Couchbase MCP Server [configuration](/docs/installation/uv):

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

3. **Restart Claude Desktop** to apply the changes.

4. You can now use the server to run queries on your Couchbase cluster using natural language and perform CRUD operations on documents.

## Logs

Claude Desktop logs can be found at:

- **macOS**: `~/Library/Logs/Claude`
- **Windows**: `%APPDATA%\Claude\Logs`

The logs can help diagnose connection issues or other problems with your MCP server configuration. For more details, refer to the [official documentation](https://modelcontextprotocol.io/quickstart/user#troubleshooting).
