---
sidebar_position: 3
title: Docker
---

# Docker

Pre-built images are available on [Docker Hub](https://hub.docker.com/r/couchbaseecosystem/mcp-server-couchbase) and the [Docker MCP Catalog](https://hub.docker.com/mcp/server/couchbase/overview). A Docker image can also be built from source.

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
        "couchbaseecosystem/mcp-server-couchbase"
      ]
    }
  }
}
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
  couchbaseecosystem/mcp-server-couchbase
```

The `CB_MCP_PORT` environment variable and port mapping (`-p`) are only needed for HTTP/SSE transport modes.

:::note
By default, the server binds to `127.0.0.1` (localhost only). When running in a Docker container with HTTP/SSE transport, set `CB_MCP_HOST=0.0.0.0` so the server is accessible outside the container network.
:::

## Networking Notes

- If your Couchbase server is running on the host machine, the connection string would typically be `couchbase://host.docker.internal`. See the [Docker networking documentation](https://docs.docker.com/desktop/features/networking/#i-want-to-connect-from-a-container-to-a-service-on-the-host).
- You can specify the container's networking with `--network=<your_network>`. The default is `bridge`. See [Docker network drivers](https://docs.docker.com/engine/network/drivers/).

## Building from Source

Clone the repository first:

```bash
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
cd mcp-server-couchbase
```

Then build the image:

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
