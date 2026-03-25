---
title: Customize MCP Server
---

# Customize MCP Server

If you want to customise the Couchbase MCP server with your own tools, resources or configurations, you may clone the repository and make the changes as you wish. If you would like to contribute towards the official Couchbase MCP server, follow the [Contributing](/product-notes/contributing) guidelines.

## Prerequisites

- **Python 3.10+** installed
- **[uv](https://docs.astral.sh/uv/)** installed
- **Git** installed

## Clone the Repository

```bash
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
cd mcp-server-couchbase
```

## MCP Client Configuration

When configuring an MCP client, use this command format:

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uv",
      "args": [
        "--directory",
        "path/to/cloned/repo/mcp-server-couchbase/",
        "run",
        "src/mcp_server.py"
      ],
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
`path/to/cloned/repo/mcp-server-couchbase/` should be the absolute path to the cloned repository on your local machine. Don't forget the trailing slash.
:::

:::tip
If you have other MCP servers configured, add the `couchbase` entry to the existing `mcpServers` object.
:::

## Dockerize from Source

You can also build and run the server as a Docker container from the cloned repository.

### Build the Image

```bash
docker build -t mcp/couchbase .
```

To include build metadata (git commit hash and build timestamp):

```bash
docker build --build-arg GIT_COMMIT_HASH=$(git rev-parse HEAD) \
  --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
  -t mcp/couchbase .
```

Or use the provided build script:

```bash
./build.sh
```

The script automatically generates git commit hash and build timestamp, creates multiple tags (`latest`, `<short-commit>`), and shows build results.

### Verify Image Labels

```bash
# View git commit hash
docker inspect --format='{{index .Config.Labels "org.opencontainers.image.revision"}}' mcp/couchbase:latest

# View all metadata labels
docker inspect --format='{{json .Config.Labels}}' mcp/couchbase:latest
```

## Next Steps

See the [Quick Start](/get-started/quickstart) for client-specific configuration instructions.
