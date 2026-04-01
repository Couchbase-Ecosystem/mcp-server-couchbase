# Environment Variables & Command Line Arguments

The MCP server can be configured using environment variables or command line arguments. If both are specified, command line arguments take priority over environment variables.

## Configuration Reference

| Environment Variable | CLI Argument | Description | Default |
| --- | --- | --- | --- |
| `CB_CONNECTION_STRING` | `--connection-string` | Connection string to the Couchbase cluster | **Required** |
| `CB_USERNAME` | `--username` | Username for basic authentication. See [Configuring Authentication](#configuring-authentication). | **Required (or mTLS)** |
| `CB_PASSWORD` | `--password` | Password for basic authentication. See [Configuring Authentication](#configuring-authentication). | **Required (or mTLS)** |
| `CB_CLIENT_CERT_PATH` | `--client-cert-path` | Path to client certificate for mTLS. See [Configuring Authentication](#configuring-authentication). | **Required if using mTLS** |
| `CB_CLIENT_KEY_PATH` | `--client-key-path` | Path to client key for mTLS. See [Configuring Authentication](#configuring-authentication). | **Required if using mTLS** |
| `CB_CA_CERT_PATH` | `--ca-cert-path` | Path to server root certificate for TLS (self-signed/untrusted certs). Not required for Capella. | |
| `CB_MCP_READ_ONLY_MODE` | `--read-only-mode` | Prevent all data modifications (KV and Query). See [Read-Only Mode](/configuration/02-read-only-mode). | `true` |
| `CB_MCP_READ_ONLY_QUERY_MODE` | `--read-only-query-mode` | **[DEPRECATED]** Prevent queries that modify data. Use `CB_MCP_READ_ONLY_MODE` instead. | `true` |
| `CB_MCP_TRANSPORT` | `--transport` | Transport mode: `stdio` (default — client launches server as subprocess), `http` ([Streamable HTTP](./03-streamable-http.md) - multiple clients, serves at `/mcp`), `sse` ([deprecated](https://modelcontextprotocol.io/docs/concepts/transports#server-sent-events-sse-deprecated) — use `http` instead) | `stdio` |
| `CB_MCP_HOST` | `--host` | Host for HTTP/SSE transport modes | `127.0.0.1` |
| `CB_MCP_PORT` | `--port` | Port for HTTP/SSE transport modes | `8000` |
| `CB_MCP_DISABLED_TOOLS` | `--disabled-tools` | Tools to disable (see [Disabling Tools](/configuration/disabling-tools)) | None |
| `CB_MCP_CONFIRMATION_REQUIRED` | `--confirmation-required` | Tools requiring user confirmation before execution (see [Elicitation/Confirmation for Tool Calls](/configuration/05-elicitation-for-tools)) | None |

## Checking the MCP Server Version

```bash
uvx couchbase-mcp-server --version
```

## Configuring Authentication

For authentication, you need **either**:

- Username and Password ([basic authentication](#how-to-basic-auth))

  **or**

- Client Certificate and Key paths ([mTLS authentication](#how-to-mtls-based-auth))

If both are specified, mTLS takes priority.

Optionally, you can specify a CA root certificate path to validate server certificates (useful for self-signed certificates).

---

## Example Configurations

:::note
All examples below use `uvx` to run the server. These can be replaced with the corresponding `docker run` commands - see [Streamable HTTP](./03-streamable-http.md) for the Docker HTTP configuration.
:::

### How to: Basic Auth

Provide a Couchbase database username and password. For Basic Authentication setup, see [Manage Database Credentials](https://docs.couchbase.com/cloud/clusters/manage-database-users.html) (Capella) or [Manage Users and Roles](https://docs.couchbase.com/server/current/manage/manage-security/manage-users-and-roles.html) (self-managed).

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

### How to: Connect to Capella

- **Connection string**: Use `couchbases://` (with `s`) — TLS is always enabled.

- **TLS certificates**: The bundled Capella root CA is used automatically. You do not need to set `CB_CA_CERT_PATH`.

- **IP allowlisting**: Ensure the machine running the MCP server has its IP [allowed](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html) in the Capella cluster settings.

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://cb.your-capella-endpoint.cloud.couchbase.com",
        "CB_USERNAME": "username",
        "CB_PASSWORD": "password"
      }
    }
  }
}
```

### How to: Connect to Self-Managed Server with Certificates

- **Connection string**: Use `couchbase://` for unencrypted connections or `couchbases://` for TLS.

- **TLS certificates**: If using TLS with self-signed or untrusted certificates, set `CB_CA_CERT_PATH` to your CA root certificate.

- **mTLS**: For certificate-based authentication, use `CB_CLIENT_CERT_PATH` and `CB_CLIENT_KEY_PATH` instead of username/password.

**Basic auth with custom CA:**

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-server-hostname",
        "CB_USERNAME": "username",
        "CB_PASSWORD": "password",
        "CB_CA_CERT_PATH": "/path/to/ca-certificate.pem"
      }
    }
  }
}
```

**mTLS (no username/password):**

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-server-hostname",
        "CB_CLIENT_CERT_PATH": "/path/to/client-certificate.pem",
        "CB_CLIENT_KEY_PATH": "/path/to/client.key",
        "CB_CA_CERT_PATH": "/path/to/ca-certificate.pem"
      }
    }
  }
}
```

### How to: mTLS Based Auth

For environments requiring certificate-based authentication. For mTLS setup, see [Configure Client Certificate Authentication](https://docs.couchbase.com/server/current/manage/manage-security/configure-client-certificates.html).

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
