---
title: Registries
---

# Registries

The Couchbase MCP Server is listed on several MCP server registries. These registries make it easy to discover, install, and configure the server.

## Smithery.ai

[Smithery](https://smithery.ai/server/@Couchbase-Ecosystem/mcp-server-couchbase) is a managed MCP server registry. It can automatically configure and run the Couchbase MCP Server for you.

- **Install:** Follow the instructions on the [Smithery server page](https://smithery.ai/server/@Couchbase-Ecosystem/mcp-server-couchbase).
- **Transport:** stdio
- **Configuration:** Smithery prompts for `CB_CONNECTION_STRING`, `CB_USERNAME`, `CB_PASSWORD`, and `CB_BUCKET_NAME`.

## Docker MCP Catalog

The [Docker MCP Catalog](https://hub.docker.com/mcp/server/couchbase/overview) provides a curated listing of MCP servers available as Docker images.

- **Image:** [`couchbaseecosystem/mcp-server-couchbase`](https://hub.docker.com/r/couchbaseecosystem/mcp-server-couchbase)
- **Transport:** stdio (default), HTTP, SSE
- See [Docker Installation](/docs/installation/docker) for full usage instructions.

## MCP Registry

The [MCP Registry](https://registry.modelcontextprotocol.io/) is the official registry for MCP servers maintained by the MCP specification authors.

- **Package ID:** `io.github.Couchbase-Ecosystem/mcp-server-couchbase`
- **Packages:** PyPI (`couchbase-mcp-server`) and OCI (`docker.io/couchbaseecosystem/mcp-server-couchbase`)
- **Configuration:** Defined in [`server.json`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/server.json) at the repository root.

## Glama.ai

[Glama](https://glama.ai/mcp/servers/@Couchbase-Ecosystem/mcp-server-couchbase) provides an MCP server directory with automated quality analysis.

- **Listing:** [Couchbase MCP Server on Glama](https://glama.ai/mcp/servers/@Couchbase-Ecosystem/mcp-server-couchbase)
- **Configuration:** Defined in [`glama.json`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/glama.json) at the repository root.

## MseeP

[MseeP](https://mseep.ai/app/13fce476-0e74-4b1e-ab82-1df2a3204809) provides MCP server verification and trust scoring.

- **Listing:** [Couchbase MCP Server on MseeP](https://mseep.ai/app/13fce476-0e74-4b1e-ab82-1df2a3204809)
- **Badge:** [![Verified on MseeP](https://mseep.ai/badge.svg)](https://mseep.ai/app/13fce476-0e74-4b1e-ab82-1df2a3204809)

## Archestra.ai

[Archestra](https://archestra.ai/mcp-catalog/couchbase-ecosystem__mcp-server-couchbase) provides an MCP catalog with quality trust scores.

- **Listing:** [Couchbase MCP Server on Archestra](https://archestra.ai/mcp-catalog/couchbase-ecosystem__mcp-server-couchbase)
- **Badge:** [![Trust Score](https://archestra.ai/mcp-catalog/api/badge/quality/Couchbase-Ecosystem/mcp-server-couchbase)](https://archestra.ai/mcp-catalog/couchbase-ecosystem__mcp-server-couchbase)
