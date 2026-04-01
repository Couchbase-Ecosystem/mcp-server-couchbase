# Troubleshooting

Common issues that you might run into while using the Couchbase MCP Server and their solutions.

## uv / uvx Issues

If you see errors related to `uv` or `uvx`, check the following:

- **`uvx` not found**: Ensure `uv` is on your system PATH. Install uv following the [official instructions](https://docs.astral.sh/uv/getting-started/installation/). If installed via a package manager, verify it's on your PATH.

  - Run `which uv` (macOS/Linux) or `where uv` (Windows) to find the path. You may need to provide the absolute path to `uv`/`uvx` in the `command` field of your MCP client configuration.

- **After updating source code**, run `uv sync` to update [dependencies](https://docs.astral.sh/uv/concepts/projects/sync/#syncing-the-environment). This is only required when running from source after pulling new changes.

## Connection Issues

If you see errors related to connecting to the Couchbase cluster, check the following:

- **Check credentials** - Ensure your connection string, username, password, or certificate paths are correct.

- **Cluster accessibility** - Ensure the cluster is accessible from the machine running the MCP server. If using Couchbase Capella, ensure the machine's IP is [allowed](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html) in the cluster settings.

- **Bucket permissions** - Check that the database user has proper permissions to access at least one bucket.

- **Connection string format** - Use `couchbases://` for Capella and TLS-enabled clusters, `couchbase://` for unencrypted local connections.

  - Use `couchbase://` for unencrypted connections

- **Certificates for TLS** - If you are connecting with TLS enabled on non-Capella clusters, ensure that the certificate is configured correctly via `CB_CA_CERT_PATH`.

## Transport Mode Issues

If the MCP client is unable to communicate with the server, check the following based on your configured transport mode:

- **stdio** - Ensure the MCP client is configured to launch the server as a subprocess. Check the client's server configuration for the correct command and arguments to start the server.

- **HTTP** - Check that the configured port is not in use. Verify that the URL matches the transport mode (`/mcp` for HTTP, `/sse` for SSE). Also check if the [client configuration](./configuration/05-streamable-http.md) is set correctly.

- **Port conflicts** - If the default port 8000 is in use, set a different port with `CB_MCP_PORT` or `--port`.

- **Host binding** - By default, the server binds to `127.0.0.1` (localhost only). To allow external connections, set `CB_MCP_HOST=0.0.0.0` or `--host`.

## Read-Only Mode Issues

If you are trying to perform write operations but they are not working as expected, check the following:

- Check whether `CB_MCP_READ_ONLY_MODE=true` (the default).

- When `CB_MCP_READ_ONLY_MODE=true`, KV write tools are not loaded and SQL++ write queries are blocked — regardless of `CB_MCP_READ_ONLY_QUERY_MODE`.

- See [Read-Only Mode](./configuration/03-read-only-mode.md) for the full behavior truth table.

## Tool Disabling Issues

If a tool you expect to be disabled is still executing, check the following:

- Verify tool names are spelled exactly as listed in the [Tools](./tools/01-cluster-health.md) reference.

- If using a file path for `CB_MCP_DISABLED_TOOLS`, ensure the file exists and is readable by the server process.

- Remember that disabling tools alone does not prevent operations - RBAC is the authoritative security control. See [Security](./security.md).

## Elicitation/Tools Requiring Confirmation

If the tool you expect to require confirmation is executing without it, check the following:

- Ensure that the MCP client supports [Elicitation](https://modelcontextprotocol.io/docs/concepts/elicitation). If the client does not support it, the tools will be executed without requiring confirmation.

- Verify tool names are spelled exactly as listed in the [Tools](./tools/01-cluster-health.md) reference.

- If using a file path for `CB_MCP_CONFIRMATION_REQUIRED`, ensure the file exists and is readable by the server process.

## Environment Variable Issues

If you are setting environment variables but they do not seem to be taking effect, check the following:

- **Variables not taking effect** - Ensure variables are set in the `env` block of your MCP client configuration, not as system environment variables (unless your client supports that).

- **CLI vs environment variable conflicts** - Command line arguments take priority over environment variables. If a setting is not behaving as expected, check if it is being overridden by a CLI argument.

- **Deprecated variables** - `CB_MCP_READ_ONLY_QUERY_MODE` is deprecated. Use `CB_MCP_READ_ONLY_MODE` instead.

- See [Environment Variables](./configuration/01-environment-variables.md) for the full reference.

## Checking Logs

Check the MCP client logs for errors or warnings:

| Client | Log Location |
| ------ | ----------- |
| **Claude Desktop** | `~/Library/Logs/Claude` (macOS), `%APPDATA%\Claude\Logs` (Windows) |
| **Cursor** | Bottom panel > Output > "Cursor MCP" |
| **VS Code** | Command Palette > "MCP: List Servers" > Show Output |
| **Windsurf** | Check Windsurf output panel |
| **JetBrains** | Help > Show Log in Finder/Explorer > mcp > couchbase |
