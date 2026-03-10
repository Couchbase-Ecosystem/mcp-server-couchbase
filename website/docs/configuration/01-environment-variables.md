---
sidebar_position: 1
title: Environment Variables
---

# Environment Variables

The MCP server can be configured using environment variables or command line arguments.

## Configuration Reference

| Environment Variable | CLI Argument | Description | Default |
|---|---|---|---|
| `CB_CONNECTION_STRING` | `--connection-string` | Connection string to the Couchbase cluster | **Required** |
| `CB_USERNAME` | `--username` | Username for basic authentication | **Required (or mTLS)** |
| `CB_PASSWORD` | `--password` | Password for basic authentication | **Required (or mTLS)** |
| `CB_CLIENT_CERT_PATH` | `--client-cert-path` | Path to client certificate for mTLS | **Required if using mTLS** |
| `CB_CLIENT_KEY_PATH` | `--client-key-path` | Path to client key for mTLS | **Required if using mTLS** |
| `CB_CA_CERT_PATH` | `--ca-cert-path` | Path to server root certificate for TLS (self-signed/untrusted certs). Not required for Capella. | |
| `CB_MCP_READ_ONLY_MODE` | `--read-only-mode` | Prevent all data modifications (KV and Query) | `true` |
| `CB_MCP_READ_ONLY_QUERY_MODE` | `--read-only-query-mode` | **[DEPRECATED]** Prevent queries that modify data. Use `CB_MCP_READ_ONLY_MODE` instead. | `true` |
| `CB_MCP_TRANSPORT` | `--transport` | Transport mode: `stdio`, `http`, `sse` | `stdio` |
| `CB_MCP_HOST` | `--host` | Host for HTTP/SSE transport modes | `127.0.0.1` |
| `CB_MCP_PORT` | `--port` | Port for HTTP/SSE transport modes | `8000` |
| `CB_MCP_DISABLED_TOOLS` | `--disabled-tools` | Tools to disable (see [Disabling Tools](/docs/configuration/disabling-tools)) | None |

## Authentication Priority

For authentication, you need **either**:
- Username and Password (basic authentication), **or**
- Client Certificate and Key paths (mTLS authentication)

If both are specified, client certificates take priority.

Optionally, you can specify a CA root certificate path to validate server certificates (useful for self-signed certificates).

## Checking the Version

```bash
uvx couchbase-mcp-server --version
```
