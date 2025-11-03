# Couchbase MCP Server

Pre-built images for the [Couchbase](https://www.couchbase.com/) MCP Server.

A Model Context Protocol (MCP) server that allows AI agents to interact with Couchbase databases.

Github Repo: https://github.com/Couchbase-Ecosystem/mcp-server-couchbase

Dockerfile: https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/Dockerfile

## Features

- Get a list of all the buckets in the cluster
- Get a list of all the scopes and collections in the specified bucket
- Get a list of all the scopes in the specified bucket
- Get a list of all the collections in a specified scope and bucket. Note that this tool requires the cluster to have Query service.
- Get the structure for a collection
- Get a document by ID from a specified scope and collection
- Upsert a document by ID to a specified scope and collection
- Delete a document by ID from a specified scope and collection
- Run a [SQL++ query](https://www.couchbase.com/sqlplusplus/) on a specified scope
  - There is an option in the MCP server, `CB_MCP_READ_ONLY_QUERY_MODE` that is set to true by default to disable running SQL++ queries that change the data or the underlying collection structure. Note that the documents can still be updated by ID.
- Get the status of the MCP server
- Check the cluster credentials by connecting to the cluster
- List all indexes in the cluster with their definitions, with optional filtering by bucket, scope, collection and index name.
- Get index recommendations from Couchbase Index Advisor for a given SQL++ query to optimize query performance
- Get cluster health status and list of all running services

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
| `CB_MCP_READ_ONLY_QUERY_MODE` | Prevent data modification queries via SQL++                                                               | `true`                                                         |
| `CB_MCP_TRANSPORT`            | Transport mode (stdio/http/sse)                                                                           | `stdio`                                                        |
| `CB_MCP_HOST`                 | Server host (HTTP/SSE modes)                                                                              | `127.0.0.1`                                                    |
| `CB_MCP_PORT`                 | Server port (HTTP/SSE modes)                                                                              | `8000`                                                         |
