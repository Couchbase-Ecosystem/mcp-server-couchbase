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
Capella connections do not require a custom CA certificate — the bundled Capella root CA is used automatically.
:::

## Priority

If both username/password **and** client certificate/key are provided, the client certificates are used for authentication.
