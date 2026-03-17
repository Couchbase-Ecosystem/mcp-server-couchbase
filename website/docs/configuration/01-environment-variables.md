---
sidebar_position: 1
title: Environment Variables & Command Line Arguments
---

# Environment Variables & Command Line Arguments

The MCP server can be configured using environment variables or command line arguments. If both are specified, command line arguments take priority over environment variables.

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
| `CB_MCP_TRANSPORT` | `--transport` | Transport mode: `stdio` (default — client launches server as subprocess), `http` ([Streamable HTTP](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports#streamable-http) — multiple clients, serves at `/mcp`), `sse` ([deprecated](https://modelcontextprotocol.io/docs/concepts/transports#server-sent-events-sse-deprecated) — use `http` instead) | `stdio` |
| `CB_MCP_HOST` | `--host` | Host for HTTP/SSE transport modes | `127.0.0.1` |
| `CB_MCP_PORT` | `--port` | Port for HTTP/SSE transport modes | `8000` |
| `CB_MCP_DISABLED_TOOLS` | `--disabled-tools` | Tools to disable (see [Disabling Tools](/configuration/disabling-tools)) | None |

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

---

## Example Configurations

### Basic Authentication (Username and Password)

Provide a Couchbase database username and password:

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

### mTLS (Mutual TLS)

For environments requiring certificate-based authentication:

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_CLIENT_CERT_PATH": "/path/to/client-certificate.pem",
        "CB_CLIENT_KEY_PATH": "/path/to/client.key"
      }
    }
  }
}
```

### Couchbase Capella

- **Connection string**: Use `couchbases://` (with `s`) — TLS is always enabled.
- **TLS certificates**: The bundled Capella root CA is used automatically. You do not need to set `CB_CA_CERT_PATH`.
- **IP allowlisting**: Ensure the machine running the MCP server has its IP [allowed](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html) in the Capella cluster settings.

```json
{
  "env": {
    "CB_CONNECTION_STRING": "couchbases://cb.your-capella-endpoint.cloud.couchbase.com",
    "CB_USERNAME": "username",
    "CB_PASSWORD": "password"
  }
}
```

### Self-Managed Couchbase Server

- **Connection string**: Use `couchbase://` for unencrypted connections or `couchbases://` for TLS.
- **TLS certificates**: If using TLS with self-signed or untrusted certificates, set `CB_CA_CERT_PATH` to your CA root certificate.
- **mTLS**: For certificate-based authentication, use `CB_CLIENT_CERT_PATH` and `CB_CLIENT_KEY_PATH` instead of username/password.

**Basic auth with custom CA:**

```json
{
  "env": {
    "CB_CONNECTION_STRING": "couchbases://your-server-hostname",
    "CB_USERNAME": "username",
    "CB_PASSWORD": "password",
    "CB_CA_CERT_PATH": "/path/to/ca-certificate.pem"
  }
}
```

**mTLS (no username/password):**

```json
{
  "env": {
    "CB_CONNECTION_STRING": "couchbases://your-server-hostname",
    "CB_CLIENT_CERT_PATH": "/path/to/client-certificate.pem",
    "CB_CLIENT_KEY_PATH": "/path/to/client.key",
    "CB_CA_CERT_PATH": "/path/to/ca-certificate.pem"
  }
}
```

:::note
For Capella connections using the Index Service REST API (e.g. `list_indexes`), the bundled Capella root CA is applied automatically. For the main SDK connection, Capella's public certificates are typically trusted by the system trust store. If you encounter TLS errors, set `CB_CA_CERT_PATH` explicitly.
:::
