---
sidebar_position: 5
title: JetBrains IDEs
---

# JetBrains IDEs

Follow these steps to use the Couchbase MCP Server with [JetBrains IDEs](https://www.jetbrains.com/) (IntelliJ IDEA, PyCharm, WebStorm, etc.).

## Setup

1. Install any [JetBrains IDE](https://www.jetbrains.com/).

2. Install one of the JetBrains plugins:
   - [AI Assistant](https://www.jetbrains.com/help/ai-assistant/getting-started-with-ai-assistant.html)
   - [Junie](https://www.jetbrains.com/help/junie/get-started-with-junie.html)

3. Navigate to **Settings > Tools > AI Assistant or Junie > MCP Server**.

4. Click **"+"** to add the Couchbase MCP [configuration](/docs/installation/uv) and click **Save**.

5. You will see the Couchbase MCP Server in the list of servers. Click **Apply** to start the server. Hovering over the status shows all available tools.

6. You can now use the Couchbase MCP Server in JetBrains IDEs to query your Couchbase cluster using natural language and perform CRUD operations on documents.

## Logs

The log file can be found at **Help > Show Log in Finder (Explorer) > mcp > couchbase**.
