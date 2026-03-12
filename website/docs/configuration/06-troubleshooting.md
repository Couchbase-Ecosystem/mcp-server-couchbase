---
sidebar_position: 6
title: Troubleshooting
---

# Configuration Troubleshooting

Troubleshooting guide for configuration-specific issues with the Couchbase MCP Server.

## Transport Mode Issues

- **stdio** — Ensure the MCP client is configured to launch the server as a subprocess. Check that no other process is already using stdin/stdout.
- **HTTP/SSE** — Check that the configured port is not in use. Verify the URL matches the transport mode (`/mcp` for HTTP, `/sse` for SSE).
- **Port conflicts** — If the default port 8000 is in use, set a different port with `CB_MCP_PORT` or `--port`.

## Read-Only Mode Issues

- If write operations fail unexpectedly, check whether `CB_MCP_READ_ONLY_MODE=true` (the default).
- When `CB_MCP_READ_ONLY_MODE=true`, KV write tools are not loaded and SQL++ write queries are blocked — regardless of `CB_MCP_READ_ONLY_QUERY_MODE`.
- See [Read-Only Mode](/docs/configuration/read-only-mode) for the full behavior truth table.

## Tool Disabling Issues

- Verify tool names are spelled exactly as listed in the [Tool Reference](/docs/tools/cluster-health).
- If using a file path for `CB_MCP_DISABLED_TOOLS`, ensure the file exists and is readable by the server process.
- Remember that disabling tools alone does not prevent operations — RBAC is the authoritative security control. See [Security](/docs/security).

## Environment Variable Issues

- **Variables not taking effect** — Ensure variables are set in the `env` block of your MCP client configuration, not as system environment variables (unless your client supports that).
- **Deprecated variables** — `CB_MCP_READ_ONLY_QUERY_MODE` is deprecated. Use `CB_MCP_READ_ONLY_MODE` instead.
- See [Environment Variables](/docs/configuration/environment-variables) for the full reference.
