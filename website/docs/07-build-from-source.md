# Build from Source

This page provides a step-by-step guide to building the Couchbase MCP Server from source when you want to run it locally, test the latest changes, or extend it directly from the GitHub repository. It covers cloning the repository, configuring your MCP client, and optionally building a Docker image from source.

If you would like to contribute towards the official Couchbase MCP server, follow the [Contributing](./08-product-notes/02-contributing.md) guidelines.

## Prerequisites

- **Python 3.10+** installed
- **[uv](https://docs.astral.sh/uv/)** installed
- **Git** installed

## Clone the Repository

```bash
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
cd mcp-server-couchbase
```

## Running the Source Code Directly

You can run the MCP server directly from the source using uv.

### Source: MCP Client Configuration

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
docker build -t mcp/couchbase-src .
```

To include build metadata (git commit hash and build timestamp):

```bash
docker build --build-arg GIT_COMMIT_HASH=$(git rev-parse HEAD) \
  --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
  -t mcp/couchbase-src .
```

Or use the provided [build script](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/build.sh):

#### Build with default image name (mcp/couchbase-src)

```bash
./build.sh
```

#### Build with custom image name

```bash
./build.sh my-custom/image-name
```

The script does the following:

- Accepts an optional image name parameter (defaults to `mcp/couchbase-src`)
- Generates git commit hash and build timestamp
- Creates multiple useful tags (`latest`, `<short-commit>`)
- Shows build information and results
- Uses the same arguments as CI/CD builds

### Verify Image Labels

```bash
# View git commit hash
docker inspect --format='{{index .Config.Labels "org.opencontainers.image.revision"}}' mcp/couchbase-src:latest

# View all metadata labels
docker inspect --format='{{json .Config.Labels}}' mcp/couchbase-src:latest
```

### Docker: MCP Client Configuration

Once the image is built, configure your MCP client to use it:

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "CB_CONNECTION_STRING=couchbases://your-connection-string",
        "-e", "CB_USERNAME=your-username",
        "-e", "CB_PASSWORD=your-password",
        "mcp/couchbase-src"
      ]
    }
  }
}
```

## Next Steps

See the [Quick Start](./02-get-started/02-quickstart.md) for client-specific configuration instructions.
