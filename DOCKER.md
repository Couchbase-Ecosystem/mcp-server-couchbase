# Couchbase MCP Server

Pre-built images for the [Couchbase](https://www.couchbase.com/) MCP Server.

A Model Context Protocol (MCP) server that allows AI agents to interact with Couchbase databases.

Github Repo: https://github.com/Couchbase-Ecosystem/mcp-server-couchbase

Dockerfile: https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/Dockerfile

## Features/Tools
### Cluster setup & health tools
| Tool Name | Description |
|-----------|-------------|
| `get_server_configuration_status` | Get the status of the MCP server |
| `test_cluster_connection` | Check the cluster credentials by connecting to the cluster |
| `get_cluster_health_and_services` | Get cluster health status and list of all running services |

### Data model & schema discovery tools
| Tool Name | Description |
|-----------|-------------|
| `get_buckets_in_cluster` | Get a list of all the buckets in the cluster |
| `get_scopes_in_bucket` | Get a list of all the scopes in the specified bucket |
| `get_collections_in_scope` | Get a list of all the collections in a specified scope and bucket. Note that this tool requires the cluster to have Query service. |
| `get_scopes_and_collections_in_bucket` | Get a list of all the scopes and collections in the specified bucket |
| `get_schema_for_collection` | Get the structure for a collection |

### Document KV operations tools
| Tool Name | Description |
|-----------|-------------|
| `get_document_by_id` | Get a document by ID from a specified scope and collection |
| `upsert_document_by_id` | Upsert a document by ID to a specified scope and collection |
| `delete_document_by_id` | Delete a document by ID from a specified scope and collection |

### Query and indexing tools
| Tool Name | Description |
|-----------|-------------|
| `list_indexes` | List all indexes in the cluster with their definitions, with optional filtering by bucket, scope, collection and index name. |
| `get_index_advisor_recommendations` | Get index recommendations from Couchbase Index Advisor for a given SQL++ query to optimize query performance |
| `run_sql_plus_plus_query` | Run a [SQL++ query](https://www.couchbase.com/sqlplusplus/) on a specified scope.<br><br>Queries are automatically scoped to the specified bucket and scope, so use collection names directly (e.g., `SELECT * FROM users` instead of `SELECT * FROM bucket.scope.users`).<br><br>`CB_MCP_READ_ONLY_QUERY_MODE` config is true by default, which means that queries that modify data are disabled by default. |

### Query performance analysis tools
| Tool Name | Description |
|-----------|-------------|
| `get_longest_running_queries` | Get longest running queries by average service time |
| `get_most_frequent_queries` | Get most frequently executed queries |
| `get_queries_with_largest_response_sizes` | Get queries with the largest response sizes |
| `get_queries_with_large_result_count` | Get queries with the largest result counts |
| `get_queries_using_primary_index` | Get queries that use a primary index (potential performance concern) |
| `get_queries_not_using_covering_index` | Get queries that don't use a covering index |
| `get_queries_not_selective` | Get queries that are not selective (index scans return many more documents than final result) |

## Usage

The Docker images can be used in the supported MCP clients such as Claude Desktop, Cursor, Windsurf, etc in combination with Docker.

### Configuration

Add the configuration specified below to the MCP configuration in your MCP client.

- Claude Desktop: https://modelcontextprotocol.io/quickstart/user
- Cursor: https://docs.cursor.com/context/model-context-protocol#configuring-mcp-servers
- Windsurf: https://docs.windsurf.com/windsurf/cascade/mcp#adding-a-new-mcp-plugin

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "-e",
        "CB_CONNECTION_STRING=<couchbase_connection_string>",
        "-e",
        "CB_USERNAME=<database_username>",
        "-e",
        "CB_PASSWORD=<database_password>",
        "couchbaseecosystem/mcp-server-couchbase:latest"
      ]
    }
  }
}
```

### Environment Variables

The detailed explanation for the environment variables can be found on the [Github Repo](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase?tab=readme-ov-file#additional-configuration-for-mcp-server).

| Variable                      | Description                                                                                               | Default                                                        |
| ----------------------------- | --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `CB_CONNECTION_STRING`        | Couchbase Connection string                                                                               | **Required**                                                   |
| `CB_USERNAME`                 | Database username                                                                                         | **Required (or Client Certificate and Key needed for mTLS)**   |
| `CB_PASSWORD`                 | Database password                                                                                         | **Required (or Client Certificate and Key needed for mTLS)**   |
| `CB_CLIENT_CERT_PATH`         | Path to the client certificate file for mTLS authentication                                               | **Required if using mTLS (or Username and Password required)** |
| `CB_CLIENT_KEY_PATH`          | Path to the client key file for mTLS authentication                                                       | **Required if using mTLS (or Username and Password required)** |
| `CB_CA_CERT_PATH`             | Path to server root certificate for TLS if server is configured with a self-signed/untrusted certificate. |                                                                |
| `CB_MCP_READ_ONLY_QUERY_MODE` | Prevent queries that modify data. Note that data modification would still be possible via document operations tools                                                              | `true`                                                         |
| `CB_MCP_TRANSPORT`            | Transport mode (stdio/http/sse)                                                                           | `stdio`                                                        |
| `CB_MCP_HOST`                 | Server host (HTTP/SSE modes)                                                                              | `127.0.0.1`                                                    |
| `CB_MCP_PORT`                 | Server port (HTTP/SSE modes)                                                                              | `8000`                                                         |
| `CB_MCP_DISABLED_TOOLS`           | Tools to disable                                              |                                                                |
