# Couchbase MCP Server

An [MCP](https://modelcontextprotocol.io/) server implementation of Couchbase that allows LLMs to directly interact with Couchbase clusters.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/) [![PyPI version](https://badge.fury.io/py/couchbase-mcp-server.svg)](https://pypi.org/project/couchbase-mcp-server/) [![Verified on MseeP](https://mseep.ai/badge.svg)](https://mseep.ai/app/13fce476-0e74-4b1e-ab82-1df2a3204809)

<a href="https://glama.ai/mcp/servers/@Couchbase-Ecosystem/mcp-server-couchbase">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@Couchbase-Ecosystem/mcp-server-couchbase/badge" alt="Couchbase Server MCP server" />
</a>

## Features

- Get a list of all the scopes and collections in the specified bucket
- Get the structure for a collection
- Get a document by ID from a specified scope and collection
- Upsert a document by ID to a specified scope and collection
- Delete a document by ID from a specified scope and collection
- Run a [SQL++ query](https://www.couchbase.com/sqlplusplus/) on a specified scope
  - There is an option in the MCP server, `CB_MCP_READ_ONLY_QUERY_MODE` that is set to true by default to disable running SQL++ queries that change the data or the underlying collection structure. Note that the documents can still be updated by ID.
- Get the status of the MCP server
- Check the cluster credentials by connecting to the cluster

## Prerequisites

- Python 3.10 or higher.
- A running Couchbase cluster. The easiest way to get started is to use [Capella](https://docs.couchbase.com/cloud/get-started/create-account.html#getting-started) free tier, which is fully managed version of Couchbase server. You can follow [instructions](https://docs.couchbase.com/cloud/clusters/data-service/import-data-documents.html#import-sample-data) to import one of the sample datasets or import your own.
- [uv](https://docs.astral.sh/uv/) installed to run the server.
- An [MCP client](https://modelcontextprotocol.io/clients) such as [Claude Desktop](https://claude.ai/download) installed to connect the server to Claude. The instructions are provided for Claude Desktop and Cursor. Other MCP clients could be used as well.

## Configuration

The MCP server can be run either from the pre built PyPI package or the source using uv.

### Running from PyPI

We publish a pre built [PyPI package](https://pypi.org/project/couchbase-mcp-server/) for the MCP server.

#### Server Configuration using Pre built Package for MCP Clients

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://connection-string",
        "CB_USERNAME": "username",
        "CB_PASSWORD": "password",
        "CB_BUCKET_NAME": "bucket_name"
      }
    }
  }
}
```

> Note: If you have other MCP servers in use in the client, you can add it to the existing `mcpServers` object.

### Running from Source

The MCP server can be run from the source using this repository.

#### Clone the repository to your local machine.

```bash
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
```

#### Server Configuration using Source for MCP Clients

This is the common configuration for the MCP clients such as Claude Desktop, Cursor, Windsurf Editor.

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
        "CB_CONNECTION_STRING": "couchbases://connection-string",
        "CB_USERNAME": "username",
        "CB_PASSWORD": "password",
        "CB_BUCKET_NAME": "bucket_name"
      }
    }
  }
}
```

> Note: `path/to/cloned/repo/mcp-server-couchbase/` should be the path to the cloned repository on your local machine. Don't forget the trailing slash at the end!

> Note: If you have other MCP servers in use in the client, you can add it to the existing `mcpServers` object.

### Additional Configuration for MCP Server

The server can be configured using environment variables or command line arguments:

| Environment Variable          | CLI Argument             | Description                                | Default      |
| ----------------------------- | ------------------------ | ------------------------------------------ | ------------ |
| `CB_CONNECTION_STRING`        | `--connection-string`    | Connection string to the Couchbase cluster | **Required** |
| `CB_USERNAME`                 | `--username`             | Username with bucket access                | **Required** |
| `CB_PASSWORD`                 | `--password`             | Password for authentication                | **Required** |
| `CB_BUCKET_NAME`              | `--bucket-name`          | Name of the bucket to access               | **Required** |
| `CB_MCP_READ_ONLY_QUERY_MODE` | `--read-only-query-mode` | Prevent data modification queries          | `true`       |
| `CB_MCP_TRANSPORT`            | `--transport`            | Transport mode: `stdio`, `http`, `sse`     | `stdio`      |
| `CB_MCP_HOST`                 | `--host`                 | Host for HTTP/SSE transport modes          | `127.0.0.1`  |
| `CB_MCP_PORT`                 | `--port`                 | Port for HTTP/SSE transport modes          | `8000`       |

You can also check the version of the server using:

```bash
uvx couchbase-mcp-server --version
```

#### Client Specific Configuration

<details>
<summary>Claude Desktop</summary>

Follow the steps below to use Couchbase MCP server with Claude Desktop MCP client

1. The MCP server can now be added to Claude Desktop by editing the configuration file. More detailed instructions can be found on the [MCP quickstart guide](https://modelcontextprotocol.io/quickstart/user).

   - On Mac, the configuration file is located at `~/Library/Application Support/Claude/claude_desktop_config.json`
   - On Windows, the configuration file is located at `%APPDATA%\Claude\claude_desktop_config.json`

   Open the configuration file and add the [configuration](#configuration) to the `mcpServers` section.

2. Restart Claude Desktop to apply the changes.

3. You can now use the server in Claude Desktop to run queries on the Couchbase cluster using natural language and perform CRUD operations on documents.

Logs

The logs for Claude Desktop can be found in the following locations:

- MacOS: ~/Library/Logs/Claude
- Windows: %APPDATA%\Claude\Logs

The logs can be used to diagnose connection issues or other problems with your MCP server configuration. For more details, refer to the [official documentation](https://modelcontextprotocol.io/quickstart/user#troubleshooting).

</details>

<details>
<summary>Cursor</summary>

Follow steps below to use Couchbase MCP server with Cursor:

1. Install [Cursor](https://cursor.sh/) on your machine.

2. In Cursor, go to Cursor > Cursor Settings > Tools & Integrations > MCP Tools. Also, checkout the docs on [setting up MCP server configuration](https://docs.cursor.com/en/context/mcp#configuring-mcp-servers) from Cursor.

3. Specify the same [configuration](#configuration). You may need to add the server configuration under a parent key of mcpServers.

4. Save the configuration.

5. You will see couchbase as an added server in MCP servers list. Refresh to see if server is enabled.

6. You can now use the Couchbase MCP server in Cursor to query your Couchbase cluster using natural language and perform CRUD operations on documents.

For more details about MCP integration with Cursor, refer to the [official Cursor MCP documentation](https://docs.cursor.com/en/context/mcp).

Logs

In the bottom panel of Cursor, click on "Output" and select "Cursor MCP" from the dropdown menu to view server logs. This can help diagnose connection issues or other problems with your MCP server configuration.

</details>

<details>
<summary>Windsurf Editor</summary>

Follow the steps below to use the Couchbase MCP server with [Windsurf Editor](https://windsurf.com/).

1. Install [Windsurf Editor](https://windsurf.com/download) on your machine.

2. In Windsurf Editor, navigate to Command Palette > Windsurf MCP Configuration Panel or Windsurf - Settings > Advanced > Cascade > Model Context Protocol (MCP) Servers. For more details on the configuration, please refer to the [official documentation](https://docs.windsurf.com/windsurf/cascade/mcp#adding-a-new-mcp-plugin).

3. Click on Add Server and then Add custom server. On the configuration that opens in the editor, add the Couchbase MCP Server [configuration](#configuration) from above.

4. Save the configuration.

5. You will see couchbase as an added server in MCP Servers list under Advanced Settings. Refresh to see if server is enabled.

6. You can now use the Couchbase MCP server in Windsurf Editor to query your Couchbase cluster using natural language and perform CRUD operations on documents.

For more details about MCP integration with Windsurf Editor, refer to the official [Windsurf MCP documentation](https://docs.windsurf.com/windsurf/cascade/mcp).

</details>

## Streamable HTTP Transport Mode

The MCP Server can be run in [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports#streamable-http) transport mode which allows multiple clients to connect to the same server instance via HTTP.
Check if your [MCP client](https://modelcontextprotocol.io/clients) supports streamable http transport before attempting to connect to MCP server in this mode.

> Note: This mode does not include authorization support.

### Usage

By default, the MCP server will run on port 8000 but this can be configured using the `--port` or `CB_MCP_PORT` environment variable.

```bash
uvx couchbase-mcp-server \
  --connection-string='<couchbase_connection_string>' \
  --username='<database_username>' \
  --password='<database_password>' \
  --bucket-name='<couchbase_bucket_to_use>' \
  --read-only-query-mode=true \
  --transport=http
```

The server will be available on http://localhost:8000/mcp. This can be used in MCP clients supporting streamable http transport mode such as Cursor.

### MCP Client Configuration

```json
{
  "mcpServers": {
    "couchbase-http": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

## SSE Transport Mode

There is an option to run the MCP server in [Server-Sent Events (SSE)](https://modelcontextprotocol.io/specification/2024-11-05/basic/transports#http-with-sse) transport mode.

> Note: SSE mode has been [deprecated](https://modelcontextprotocol.io/docs/concepts/transports#server-sent-events-sse-deprecated) by MCP. We have support for [Streamable HTTP](#streamable-http-transport-mode).

### Usage

By default, the MCP server will run on port 8000 but this can be configured using the `--port` or `CB_MCP_PORT` environment variable.

```bash
uvx couchbase-mcp-server \
  --connection-string='<couchbase_connection_string>' \
  --username='<database_username>' \
  --password='<database_password>' \
  --bucket-name='<couchbase_bucket_to_use>' \
  --read-only-query-mode=true \
  --transport=sse
```

The server will be available on http://localhost:8000/sse. This can be used in MCP clients supporting SSE transport mode such as Cursor.

### MCP Client Configuration

```json
{
  "mcpServers": {
    "couchbase-sse": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

## Docker Image

The MCP server can also be built and run as a Docker container. Prebuilt images can be found on [DockerHub](https://hub.docker.com/r/couchbaseecosystem/mcp-server-couchbase).

Alternatively, we are part of the [Docker MCP Catalog](https://hub.docker.com/mcp/server/couchbase/overview).

### Building Image

```bash
docker build -t mcp/couchbase .
```

<details>
<summary>Building with Arguments</summary>
If you want to build with the build arguments for commit hash and the build time, you can build using:

```bash
docker build --build-arg GIT_COMMIT_HASH=$(git rev-parse HEAD) \
  --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
  -t mcp/couchbase .
```

**Alternatively, use the provided build script:**

```bash
./build.sh
```

This script automatically:

- Generates git commit hash and build timestamp
- Creates multiple useful tags (`latest`, `<short-commit>`)
- Shows build information and results
- Uses the same arguments as CI/CD builds

**Verify image labels:**

```bash
# View git commit hash in image
docker inspect --format='{{index .Config.Labels "org.opencontainers.image.revision"}}' mcp/couchbase:latest

# View all metadata labels
docker inspect --format='{{json .Config.Labels}}' mcp/couchbase:latest
```

</details>

### Running

The MCP server can be run with the environment variables being used to configure the Couchbase settings. The environment variables are the same as described in the [Configuration section](#server-configuration-for-mcp-clients).

#### Independent Docker Container

```bash
docker run --rm -i \
  -e CB_CONNECTION_STRING='<couchbase_connection_string>' \
  -e CB_USERNAME='<database_user>' \
  -e CB_PASSWORD='<database_password>' \
  -e CB_BUCKET_NAME='<bucket_name>' \
  -e CB_MCP_TRANSPORT='<http|sse|stdio>' \
  -e CB_MCP_READ_ONLY_QUERY_MODE='<true|false>' \
  -e CB_MCP_PORT=9001 \
  -p 9001:9001 \
  mcp/couchbase
```

The `CB_MCP_PORT` environment variable is only applicable in the case of HTTP transport modes like http and sse.

#### MCP Client Configuration

The Docker image can be used in `stdio` transport mode with the following configuration.

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
        "-e",
        "CB_BUCKET_NAME=<bucket_name>",
        "mcp/couchbase"
      ]
    }
  }
}
```

Notes

- The `couchbase_connection_string` value depends on whether the Couchbase server is running on the same host machine, in another Docker container, or on a remote host. If your Couchbase server is running on your host machine, your connection string would likely be of the form `couchbase://host.docker.internal`. For details refer to the [docker documentation](https://docs.docker.com/desktop/features/networking/#i-want-to-connect-from-a-container-to-a-service-on-the-host).
- You can specify the container's networking using the `--network=<your_network>` option. The network you choose depends on your environment; the default is `bridge`. For details, refer to [network drivers in docker](https://docs.docker.com/engine/network/drivers/).

### Risks Associated with LLMs

- The use of large language models and similar technology involves risks, including the potential for inaccurate or harmful outputs.
- Couchbase does not review or evaluate the quality or accuracy of such outputs, and such outputs may not reflect Couchbase's views.
- You are solely responsible for determining whether to use large language models and related technology, and for complying with any license terms, terms of use, and your organization's policies governing your use of the same.

### Managed MCP Server

The Couchbase MCP server can also be used as a managed server in your agentic applications via [Smithery.ai](https://smithery.ai/server/@Couchbase-Ecosystem/mcp-server-couchbase).

## Troubleshooting Tips

- Ensure the path to your MCP server repository is correct in the configuration if running from source.
- Verify that your Couchbase connection string, database username, password and bucket name are correct.
- If using Couchbase Capella, ensure that the cluster is [accessible](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html) from the machine where the MCP server is running.
- Check that the database user has proper permissions to access the specified bucket.
- Confirm that the `uv` package manager is properly installed and accessible. You may need to provide absolute path to `uv`/`uvx` in the `command` field in the configuration.
- Check the logs for any errors or warnings that may indicate issues with the MCP server. The location of the logs depend on your MCP client.
- If you are observing issues running your MCP server from source after updating your local MCP server repository, try running `uv sync` to update the [dependencies](https://docs.astral.sh/uv/concepts/projects/sync/#syncing-the-environment).

---

## 👩‍💻 Contributing

We welcome contributions from the community! Whether you want to fix bugs, add features, or improve documentation, your help is appreciated.

If you need help, have found a bug, or want to contribute improvements, the best place to do that is right here — by [opening a GitHub issue](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues).

### For Developers

If you're interested in contributing code or setting up a development environment:

📖 **See [CONTRIBUTING.md](CONTRIBUTING.md)** for comprehensive developer setup instructions, including:

- Development environment setup with `uv`
- Code linting and formatting with Ruff
- Pre-commit hooks installation
- Project structure overview
- Development workflow and practices

### Quick Start for Contributors

```bash
# Clone and setup
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
cd mcp-server-couchbase

# Install with development dependencies
uv sync --extra dev

# Install pre-commit hooks
uv run pre-commit install

# Run linting
./scripts/lint.sh
```

---

## 📢 Support Policy

We truly appreciate your interest in this project!
This project is **Couchbase community-maintained**, which means it's **not officially supported** by our support team. However, our engineers are actively monitoring and maintaining this repo and will try to resolve issues on a best-effort basis.

Our support portal is unable to assist with requests related to this project, so we kindly ask that all inquiries stay within GitHub.

Your collaboration helps us all move forward together — thank you!
