---
title: Security
---

# Security

The Couchbase MCP Server provides multiple layers of security to protect your data when used with LLMs.

## Best Practices Quick Reference

- **Always configure RBAC** — Create a dedicated database user with least-privilege permissions.
- **Keep read-only mode enabled** — `CB_MCP_READ_ONLY_MODE=true` (default) blocks all write operations.
- **Use TLS** — Use `couchbases://` connection strings for encrypted connections.
- **Disable unnecessary tools** — Reduce the attack surface by removing tools you don't need.
- **Don't rely on a single layer** — Combine RBAC, read-only mode, tool disabling, and TLS for defense in depth.

## Read-Only Mode (Default)

By default, `CB_MCP_READ_ONLY_MODE=true`. This:

- **Prevents KV write tools** from being loaded — they won't appear in tool discovery.
- **Blocks SQL++ write queries** — INSERT, UPDATE, DELETE, MERGE, and DDL statements are rejected.

See [Read-Only Mode](/docs/configuration/read-only-mode) for the full configuration reference.

## RBAC Best Practices

:::important
Database RBAC (Role-Based Access Control) permissions are the **authoritative security control**. Always configure appropriate RBAC permissions on your Couchbase user credentials as the primary security measure.
:::

Recommendations:

- **Create a dedicated database user** for the MCP server with only the permissions it needs.
- **Grant read-only roles** if write operations are not needed (e.g., `Data Reader`, `Query Select`).
- **Scope permissions to specific buckets** rather than granting cluster-wide access.
- Don't rely solely on `CB_MCP_READ_ONLY_MODE` or tool disabling — these guide LLM behavior but RBAC is the enforcement layer.

## Tool Disabling

You can [disable specific tools](/docs/configuration/disabling-tools) to reduce the attack surface. However:

:::warning
Disabling tools alone does not guarantee operations cannot be performed. For example, even with KV write tools disabled, data modifications can still occur via `run_sql_plus_plus_query` using SQL++ DML statements — unless `CB_MCP_READ_ONLY_MODE=true` or the database user lacks RBAC permissions.
:::

## TLS / mTLS

The server supports:

- **TLS connections** — Use `couchbases://` (with `s`) in your connection string for encrypted connections.
- **Custom CA certificates** — Set `CB_CA_CERT_PATH` for self-signed or untrusted server certificates.
- **mTLS (mutual TLS)** — Set `CB_CLIENT_CERT_PATH` and `CB_CLIENT_KEY_PATH` for certificate-based authentication.

For Capella connections, TLS is always enabled and the bundled Capella root CA is used automatically.

## Defense in Depth

For maximum security, layer these controls:

1. **RBAC** — Least-privilege database user permissions (primary control).
2. **Read-Only Mode** — `CB_MCP_READ_ONLY_MODE=true` (default) blocks all write operations.
3. **Tool Disabling** — Remove unnecessary tools from LLM discovery.
4. **TLS/mTLS** — Encrypt all network traffic.

## Risks Associated with LLMs

- The use of large language models involves risks, including the potential for inaccurate or harmful outputs.
- Couchbase does not review or evaluate the quality or accuracy of LLM outputs, and such outputs may not reflect Couchbase's views.
- You are solely responsible for determining whether to use LLMs and for complying with your organization's policies.
