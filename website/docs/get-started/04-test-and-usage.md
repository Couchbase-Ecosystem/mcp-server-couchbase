---
sidebar_position: 4
title: Test & Usage
---

# Test & Usage

After configuring the Couchbase MCP Server, verify that everything is working correctly.

## Step 1: Check Server Status

Ask your AI assistant:

> "What is the status of the Couchbase MCP server?"

The assistant will call `get_server_configuration_status` and return the server's configuration, including:
- Server version and build info
- Transport mode
- Read-only mode status
- Loaded tools and any disabled tools
- Connection string (with credentials redacted)

If the tool is not available, the server did not start correctly. Check the [Troubleshooting](/docs/get-started/troubleshooting) guide.

## Step 2: Test Cluster Connection

> "Test the connection to my Couchbase cluster."

The assistant will call `test_cluster_connection`, which attempts to connect to the cluster using the configured credentials. It returns:
- Connection status (success or failure)
- Cluster services and their status
- Bucket accessibility information

If the connection fails, verify your `CB_CONNECTION_STRING`, `CB_USERNAME`, and `CB_PASSWORD` values.

## Step 3: Try Basic Operations

Once connected, try these operations to confirm full functionality:

> "List all buckets in my cluster."

This calls `get_buckets_in_cluster` and confirms the server can communicate with the cluster's management API.

> "Show me the scopes and collections in the `travel-sample` bucket."

This calls `get_scopes_and_collections_in_bucket` and confirms bucket-level access.

> "Run a query: SELECT COUNT(*) FROM `travel-sample`.inventory.airline"

This calls `run_sql_plus_plus_query` and confirms the Query service is accessible.

## Checking Logs

If the server isn't working as expected, check the MCP client logs for errors:

| Client | Log Location |
|--------|-------------|
| **Claude Desktop** | `~/Library/Logs/Claude` (macOS), `%APPDATA%\Claude\Logs` (Windows) |
| **Cursor** | Bottom panel > Output > "Cursor MCP" |
| **VS Code** | Command Palette > "MCP: List Servers" > Show Output |
| **Windsurf** | Check Windsurf output panel |
| **JetBrains** | Help > Show Log in Finder/Explorer > mcp > couchbase |

## Next Steps

- Explore the [Tutorials](/docs/home/tutorials) for guided walkthroughs
- Review the [Tool Reference](/docs/tools/cluster-health) for all available tools
- Configure [Read-Only Mode](/docs/configuration/read-only-mode) or [Disable Tools](/docs/configuration/disabling-tools) for your security needs
