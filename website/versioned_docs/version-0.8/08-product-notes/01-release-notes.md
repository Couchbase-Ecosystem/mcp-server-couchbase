# Release Notes

Full release details are published on the [GitHub Releases](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/releases) page.

## Version History

### v0.8.0 (27 May 2026)

- **Python 3.14 Support** - The Couchbase MCP Server is now compatible with Python 3.14, allowing users to take advantage of the latest features and improvements in the Python ecosystem.

- **List Indexes Tool** - List indexes tool will be powered by SQL++ for Couchbase Server versions 8.0 and above. Users can set `return_raw_index_stats=true` to return the unprocessed index information.

- **Migrate to FastMCP SDK** - The server has been updated to use the native [FastMCP SDK](https://gofastmcp.com/getting-started/welcome) instead of the native [MCP SDK](https://py.sdk.modelcontextprotocol.io/).

- **Docker Base Image Update** - The base images for the prebuilt Docker images have been updated to `python:3.13-slim-trixie` for security and performance improvements.

### v0.7.1 (9 April 2026)

- **Fix for test_cluster_connection Tool** - Resolved an issue where the `test_cluster_connection` tool could cause an exception with the latest Couchbase SDK (4.6.0). The tool now accurately reflects the connection status in its response.
- **Update Development Dependencies** - Updated development dependencies for pytest and pytest-asyncio to latest versions.

### v0.7.0 (1 April 2026)

- **Explain Query Tool** - New `explain_sql_plus_plus_query` tool returns query execution plans for LLM analysis and optimization.

- **Elicitation for Tool Calls** - New `CB_MCP_CONFIRMATION_REQUIRED` setting enables user confirmation prompts for specified tools before execution.

**Note:** The tool call for `test_cluster_connection` has a [bug](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues/127) with the Couchbase Python SDK 4.6.0. The solution is to downgrade the SDK version in the MCP server to 4.5.0 or upgrade the MCP server to version 0.7.1.

### v0.6.1 (6 February 2026)

- **Read-Only Mode** - New `CB_MCP_READ_ONLY_MODE` setting disables all write operations (KV write tools not loaded, SQL++ write queries blocked). Enabled by default for safety.

- **Tool Disabling** - Disable individual tools via `CB_MCP_DISABLED_TOOLS` (comma-separated list or file path).

- **Expanded CRUD Support** - Added `insert_document_by_id`, `replace_document_by_id`, and `delete_document_by_id` tools in addition to existing get and upsert operations.

- **IDE Support** - Added support for VS Code and JetBrains IDEs (AI Assistant and Junie plugins).

### v0.5.3 (10 December 2025)

- **Query Performance Analysis** - Added 7 tools for identifying slow-running queries, frequently executed queries, primary index usage, non-covering indexes, non-selective queries, large response sizes, and large result counts.

### v0.5.2 (13 November 2025)

- **MCP Registry Support** - MCP server added to the [MCP Registry](https://modelcontextprotocol.io/registry) for easier discovery and installation by clients.

### v0.5.1 (3 November 2025)

- **List Indexes** - New `list_indexes` tool with optional filtering by bucket, scope, collection, and index name.

- **Index Recommendations** - New `get_index_advisor_recommendations` tool leveraging the Couchbase Index Advisor.

- **Cluster Health** - New `get_cluster_health_and_services` tool for monitoring cluster status and service latency.

## Upcoming Features

- **Search-Based Tools** - Tools for Full Text Search (FTS).

## Checking Your Version

```bash
uvx couchbase-mcp-server --version
```

## Installation Channels

| Channel | Update Method |
| ------- | ------------ |
| **PyPI** | `uvx couchbase-mcp-server` always runs the latest version |
| **Docker Hub** | Pull the latest tag: `docker pull couchbaseecosystem/mcp-server-couchbase:latest` |
| **Source** | `git pull` and `uv sync` |
