---
sidebar_position: 2
title: Source
---

# Install from Source

Clone the repository and run directly with `uv`.

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

## Next Steps

See the [Setup](/docs/get-started/setup) page for client-specific configuration instructions, or jump to the [Quick Start](/docs/get-started/quickstart) to start using the server.
