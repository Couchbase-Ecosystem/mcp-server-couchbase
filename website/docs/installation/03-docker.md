---
sidebar_position: 3
title: Docker
---

# Docker

The MCP server can be built and run as a Docker container. Pre-built images are available on [Docker Hub](https://hub.docker.com/r/couchbaseecosystem/mcp-server-couchbase) and the [Docker MCP Catalog](https://hub.docker.com/mcp/server/couchbase/overview).

## Building the Image

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

**Verify image labels:**

```bash
# View git commit hash
docker inspect --format='{{index .Config.Labels "org.opencontainers.image.revision"}}' mcp/couchbase:latest

# View all metadata labels
docker inspect --format='{{json .Config.Labels}}' mcp/couchbase:latest
```

## Running as Independent Container

```bash
docker run --rm -i \
  -e CB_CONNECTION_STRING='<couchbase_connection_string>' \
  -e CB_USERNAME='<database_user>' \
  -e CB_PASSWORD='<database_password>' \
  -e CB_MCP_TRANSPORT='<http|sse|stdio>' \
  -e CB_MCP_READ_ONLY_MODE='true' \
  -e CB_MCP_PORT=9001 \
  -p 9001:9001 \
  mcp/couchbase
```

The `CB_MCP_PORT` environment variable and port mapping (`-p`) are only needed for HTTP/SSE transport modes.

## MCP Client Configuration (stdio)

The Docker image can be used in `stdio` transport mode with MCP clients:

```json
{
  "mcpServers": {
    "couchbase-mcp-docker": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "-e",
        "CB_CONNECTION_STRING=<couchbase_connection_string>",
        "-e",
        "CB_USERNAME=<database_user>",
        "-e",
        "CB_PASSWORD=<database_password>",
        "mcp/couchbase"
      ]
    }
  }
}
```

## Networking Notes

- If your Couchbase server is running on the host machine, the connection string would typically be `couchbase://host.docker.internal`. See the [Docker networking documentation](https://docs.docker.com/desktop/features/networking/#i-want-to-connect-from-a-container-to-a-service-on-the-host).
- You can specify the container's networking with `--network=<your_network>`. The default is `bridge`. See [Docker network drivers](https://docs.docker.com/engine/network/drivers/).
