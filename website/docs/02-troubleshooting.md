---
title: Troubleshooting
---

# Troubleshooting

Common issues and solutions when using the Couchbase MCP Server.

## uv / uvx Issues

- **`uvx` not found**: Ensure `uv` is on your system PATH. Install uv following the [official instructions](https://docs.astral.sh/uv/getting-started/installation/). If installed via a package manager, verify it's on your PATH.
  - Run `which uv` (macOS/Linux) or `where uv` (Windows) to find the path. You may need to provide the absolute path to `uv`/`uvx` in the `command` field of your MCP client configuration.
- **After updating source code**, run `uv sync` to update [dependencies](https://docs.astral.sh/uv/concepts/projects/sync/#syncing-the-environment). This is only required when running from source after pulling new changes.

## Connection Issues

- **Check credentials** — Ensure your connection string, username, password, or certificate paths are correct.
- **Cluster accessibility** — Ensure the cluster is accessible from the machine running the MCP server. If using Couchbase Capella, ensure the machine's IP is [allowed](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html) in the cluster settings.
- **Bucket permissions** — Check that the database user has proper permissions to access at least one bucket.
- **Connection string format** — Use `couchbases://` for Capella and TLS-enabled clusters, `couchbase://` for unencrypted local connections.
  - Use `couchbase://` for unencrypted connections as you do not need to encrypt the connection in trusted environments.

## Transport Mode Issues

- **stdio** — Ensure the MCP client is configured to launch the server as a subprocess. Check that no other process is already using stdin/stdout.
- **HTTP/SSE** — Check that the configured port is not in use. Verify the URL matches the transport mode (`/mcp` for HTTP, `/sse` for SSE).
- **Port conflicts** — If the default port 8000 is in use, set a different port with `CB_MCP_PORT` or `--port`.
- **Host binding** — By default, the server binds to `127.0.0.1` (localhost only). To allow external connections, set `CB_MCP_HOST=0.0.0.0`.

## Read-Only Mode Issues

- If write operations fail unexpectedly, check whether `CB_MCP_READ_ONLY_MODE=true` (the default).
- When `CB_MCP_READ_ONLY_MODE=true`, KV write tools are not loaded and SQL++ write queries are blocked — regardless of `CB_MCP_READ_ONLY_QUERY_MODE`.
- See [Read-Only Mode](/configuration/read-only-mode) for the full behavior truth table.

## Tool Disabling Issues

- Verify tool names are spelled exactly as listed in the [Tools](/tools/cluster-health) reference.
- If using a file path for `CB_MCP_DISABLED_TOOLS`, ensure the file exists and is readable by the server process.
- Remember that disabling tools alone does not prevent operations — RBAC is the authoritative security control. See [Security](/security).

## Tools Requiring Confirmation

- Verify tool names are spelled exactly as listed in the [Tools](/tools/cluster-health) reference.
- If using a file path for `CB_MCP_CONFIRMATION_REQUIRED`, ensure the file exists and is readable by the server process.
- Ensure that the MCP client supports [Elicitation](https://modelcontextprotocol.io/docs/concepts/elicitation). If the client does not support it, the tools will be executed without requiring confirmation.
- See [Elicitation/Confirmation for Tool Calls](/configuration/confirmation-required) for configuration details.

## Environment Variable Issues

- **Variables not taking effect** — Ensure variables are set in the `env` block of your MCP client configuration, not as system environment variables (unless your client supports that).
- **CLI vs environment variable conflicts** — Command line arguments take priority over environment variables. If a setting isn't behaving as expected, check if it's being overridden by a CLI argument.
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
