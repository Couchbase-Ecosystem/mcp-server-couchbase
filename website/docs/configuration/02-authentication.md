---
sidebar_position: 2
title: Authentication
---

# Authentication

The Couchbase MCP Server supports two authentication methods.

## Basic Authentication (Username & Password)

The simplest method. Provide a Couchbase username and password:

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

## mTLS (Mutual TLS)

For environments requiring certificate-based authentication. Note that **username and password are still required** even when using mTLS, because some tools (e.g. `list_indexes`) communicate with Couchbase REST APIs using HTTP Basic Auth:

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "username",
        "CB_PASSWORD": "password",
        "CB_CLIENT_CERT_PATH": "/path/to/client-certificate.pem",
        "CB_CLIENT_KEY_PATH": "/path/to/client.key"
      }
    }
  }
}
```

## Capella vs Self-Managed Server

The authentication setup differs depending on your Couchbase deployment type.

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
- **mTLS**: Optionally configure mutual TLS with `CB_CLIENT_CERT_PATH` and `CB_CLIENT_KEY_PATH`.

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

## Custom CA Certificate

If your Couchbase server uses a self-signed or untrusted certificate, provide the CA root certificate:

```json
{
  "env": {
    "CB_CONNECTION_STRING": "couchbases://your-connection-string",
    "CB_USERNAME": "username",
    "CB_PASSWORD": "password",
    "CB_CA_CERT_PATH": "/path/to/ca-certificate.pem"
  }
}
```

:::note
For Capella connections using the Index Service REST API (e.g. `list_indexes`), the bundled Capella root CA is applied automatically. For the main SDK connection, Capella's public certificates are typically trusted by the system trust store. If you encounter TLS errors, set `CB_CA_CERT_PATH` explicitly.
:::

## Priority

If both username/password **and** client certificate/key are provided, the client certificates are used for authentication.
