---
title: Troubleshooting
---

# Troubleshooting

Common issues and solutions when using the Couchbase MCP Server.

## uv / uvx Issues

- **Confirm `uv` is installed** and accessible. You may need to provide the absolute path to `uv`/`uvx` in the `command` field of your MCP client configuration. Run `which uv` (macOS/Linux) or `where uv` (Windows) to find the path.
- **After updating the repo**, run `uv sync` to update [dependencies](https://docs.astral.sh/uv/concepts/projects/sync/#syncing-the-environment).
- **`uvx` not found**: Install uv following the [official instructions](https://docs.astral.sh/uv/getting-started/installation/). If installed via a package manager, ensure it's on your system PATH.

## Connection Issues

- **Verify the repository path** is correct in your MCP client configuration (if running from source).
- **Check credentials** — Ensure your connection string, username, password, or certificate paths are correct.
- **Capella accessibility** — If using Couchbase Capella, ensure the cluster is [accessible](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html) from the machine running the MCP server.
- **Bucket permissions** — Check that the database user has proper permissions to access at least one bucket.
- **Connection string format** — Use `couchbases://` for Capella and TLS-enabled clusters, `couchbase://` for unencrypted local connections.

## Transport Mode Issues

- **stdio** — Ensure the MCP client is configured to launch the server as a subprocess. Check that no other process is already using stdin/stdout.
- **HTTP/SSE** — Check that the configured port is not in use. Verify the URL matches the transport mode (`/mcp` for HTTP, `/sse` for SSE).
- **Port conflicts** — If the default port 8000 is in use, set a different port with `CB_MCP_PORT` or `--port`.

## Read-Only Mode Issues

- If write operations fail unexpectedly, check whether `CB_MCP_READ_ONLY_MODE=true` (the default).
- When `CB_MCP_READ_ONLY_MODE=true`, KV write tools are not loaded and SQL++ write queries are blocked — regardless of `CB_MCP_READ_ONLY_QUERY_MODE`.
- See [Read-Only Mode](/configuration/read-only-mode) for the full behavior truth table.

## Tool Disabling Issues

- Verify tool names are spelled exactly as listed in the [Tools](/tools/cluster-health) reference.
- If using a file path for `CB_MCP_DISABLED_TOOLS`, ensure the file exists and is readable by the server process.
- Remember that disabling tools alone does not prevent operations — RBAC is the authoritative security control. See [Security](/security).

## Environment Variable Issues

- **Variables not taking effect** — Ensure variables are set in the `env` block of your MCP client configuration, not as system environment variables (unless your client supports that).
- **Deprecated variables** — `CB_MCP_READ_ONLY_QUERY_MODE` is deprecated. Use `CB_MCP_READ_ONLY_MODE` instead.
- See [Environment Variables](/configuration/environment-variables) for the full reference.

## Checking Logs

Check the MCP client logs for errors or warnings:

| Client | Log Location |
|--------|-------------|
| **Claude Desktop** | `~/Library/Logs/Claude` (macOS), `%APPDATA%\Claude\Logs` (Windows) |
| **Cursor** | Bottom panel > Output > "Cursor MCP" |
| **VS Code** | Command Palette > "MCP: List Servers" > Show Output |
| **Windsurf** | Check Windsurf output panel |
| **JetBrains** | Help > Show Log in Finder/Explorer > mcp > couchbase |
