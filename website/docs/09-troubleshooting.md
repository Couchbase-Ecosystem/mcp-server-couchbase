---
sidebar_position: 9
title: Troubleshooting
---

# Troubleshooting

Common issues and solutions when using the Couchbase MCP Server.

## Connection Issues

- **Verify the repository path** is correct in your MCP client configuration (if running from source).
- **Check credentials** — Ensure your connection string, username, password, or certificate paths are correct.
- **Capella accessibility** — If using Couchbase Capella, ensure the cluster is [accessible](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html) from the machine running the MCP server.
- **Bucket permissions** — Check that the database user has proper permissions to access at least one bucket.

## uv / uvx Issues

- **Confirm `uv` is installed** and accessible. You may need to provide the absolute path to `uv`/`uvx` in the `command` field of your MCP client configuration.
- **After updating the repo**, run `uv sync` to update [dependencies](https://docs.astral.sh/uv/concepts/projects/sync/#syncing-the-environment).

## Transport Mode Issues

- **stdio** — Ensure the MCP client is configured to launch the server as a subprocess. Check that no other process is already using stdin/stdout.
- **HTTP/SSE** — Check that the configured port is not in use. Verify the URL matches the transport mode (`/mcp` for HTTP, `/sse` for SSE).

## Read-Only Mode

- If write operations fail unexpectedly, check whether `CB_MCP_READ_ONLY_MODE=true` (the default). See [Read-Only Mode](/docs/configuration/read-only-mode).

## Logs

Check the MCP client logs for errors or warnings. Log locations vary by client:

- **Claude Desktop**: `~/Library/Logs/Claude` (macOS), `%APPDATA%\Claude\Logs` (Windows)
- **Cursor**: Bottom panel > Output > "Cursor MCP"
- **VS Code**: Command Palette > "MCP: List Servers" > Show Output
- **JetBrains**: Help > Show Log in Finder/Explorer > mcp > couchbase
