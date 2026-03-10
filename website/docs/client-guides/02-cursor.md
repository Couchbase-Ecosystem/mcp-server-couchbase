---
sidebar_position: 2
title: Cursor
---

# Cursor

Follow these steps to use the Couchbase MCP Server with [Cursor](https://cursor.sh/).

## Setup

1. Install [Cursor](https://cursor.sh/) on your machine.

2. Go to **Cursor > Cursor Settings > Tools & Integrations > MCP Tools**. See the [Cursor MCP documentation](https://cursor.com/docs/mcp) for details.

3. Add the Couchbase MCP Server [configuration](/docs/getting-started/installation). You may need to add the server configuration under a parent key of `mcpServers`.

4. Save the configuration.

5. You will see **couchbase** listed in the MCP servers list. Refresh to confirm the server is enabled.

6. You can now use the Couchbase MCP Server in Cursor to query your Couchbase cluster using natural language and perform CRUD operations on documents.

## Logs

In the bottom panel of Cursor, click on **Output** and select **Cursor MCP** from the dropdown menu to view server logs. This can help diagnose connection issues or other problems with your MCP server configuration.

For more details, refer to the [official Cursor MCP documentation](https://cursor.com/docs/mcp).
