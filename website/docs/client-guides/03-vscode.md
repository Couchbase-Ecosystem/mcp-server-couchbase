---
sidebar_position: 3
title: VS Code
---

# VS Code

Follow these steps to use the Couchbase MCP Server with [VS Code](https://code.visualstudio.com/).

## Setup

1. Install [VS Code](https://code.visualstudio.com/).

2. Configure the MCP server using one of these methods:

   **Workspace configuration:**
   - Create a new file at `.vscode/mcp.json` in your workspace.
   - Add the configuration and save.

   **Global configuration:**
   - Run **MCP: Open User Configuration** in the Command Palette (`Ctrl+Shift+P` or `Cmd+Shift+P`).
   - Add the configuration and save.

3. Add the Couchbase MCP Server configuration:

   ```json
   {
     "servers": {
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

   :::note
   VS Code uses `servers` as the top-level JSON property, while other clients like Cursor use `mcpServers`. Check the [VS Code MCP documentation](https://code.visualstudio.com/docs/copilot/customization/mcp-servers) for details.
   :::

4. Once saved, the server starts and a small action list appears with **Running | Stop | n Tools | More..**.

5. Click on the options to **Start**/**Stop**/manage the server.

## Logs

In the Command Palette (`Ctrl+Shift+P` or `Cmd+Shift+P`):
- Run **MCP: List Servers** and pick the couchbase server.
- Choose **Show Output** to see its logs in the Output tab.
