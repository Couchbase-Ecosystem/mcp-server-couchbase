---
sidebar_position: 3
title: Installation
---

# Installation

The Couchbase MCP Server can be installed from PyPI or run from source.

## Install from PyPI (Recommended)

The pre-built [PyPI package](https://pypi.org/project/couchbase-mcp-server/) is the easiest way to get started.

No manual installation is needed — `uvx` runs the package directly:

```bash
uvx couchbase-mcp-server --version
```

When configuring an MCP client, use this command format:

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "username",
        "CB_PASSWORD": "password"
      }
    }
  }
}
```

## Install from Source

Clone the repository and run directly with `uv`:

```bash
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
cd mcp-server-couchbase
```

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
