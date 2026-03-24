---
sidebar_position: 1
title: Prerequisites
---

# Prerequisites

Before using the Couchbase MCP Server, ensure you have the following:

## Required

- **Python 3.10 or higher** — The server requires Python 3.10+ (supports 3.10, 3.11, 3.12, 3.13).
- **Couchbase Server 7.2 or higher** — The MCP Server is compatible with Couchbase Server 7.2+.
- **A running Couchbase cluster** — Either:
  - [Couchbase Capella](https://docs.couchbase.com/cloud/get-started/create-account.html#getting-started) (free tier available) — fully managed cloud version
  - A self-hosted Couchbase Server instance
- **[uv](https://docs.astral.sh/uv/) or [Docker](https://www.docker.com/)** — uv is the recommended way to run the server. Docker is an alternative if you prefer containerized deployments.
- **An MCP client** — Such as [Claude Desktop](https://claude.ai/download), [Cursor](https://cursor.sh/), [VS Code](https://code.visualstudio.com/docs/copilot/chat/mcp-servers), [Windsurf](https://docs.windsurf.com/windsurf/cascade/mcp), or any [MCP-compatible client](https://modelcontextprotocol.io/clients).

## Setup Couchbase Server

If you don't already have a Couchbase cluster, you can set one up using either option:

- **Couchbase Capella** (recommended) — [Create a free-tier account](https://docs.couchbase.com/cloud/get-started/create-account.html#getting-started) for a fully managed cloud deployment.
- **Self-Managed Couchbase Server** — [Install Couchbase Server](https://docs.couchbase.com/server/current/install/install-intro.html) on your own infrastructure.

## Getting Sample Data (Optional)

- **Couchbase Capella** — [Import sample datasets](https://docs.couchbase.com/cloud/clusters/data-service/import-data-documents.html#import-sample-data) like `travel-sample` or import your own data.
- **Self-Managed Couchbase Server** — [Install sample buckets](https://docs.couchbase.com/server/current/manage/manage-settings/install-sample-buckets.html) like `travel-sample` from the Couchbase Web Console.

## Setup Authentication

Configure your Couchbase cluster with one of the following authentication methods:

- **Basic Authentication**: A username and password with access to the required buckets.
- **mTLS Authentication**: A client certificate and key for mutual TLS authentication.

For Basic Authentication setup, see [Manage Database Credentials](https://docs.couchbase.com/cloud/clusters/manage-database-users.html) (Capella) or [Manage Users and Roles](https://docs.couchbase.com/server/current/manage/manage-security/manage-users-and-roles.html) (self-managed). For mTLS setup, see [Configure Client Certificate Authentication](https://docs.couchbase.com/server/current/manage/manage-security/configure-client-certificates.html).

Ensure that:
- The cluster is accessible from the machine running the MCP server.
- If using Capella, the machine's IP address is [allowed](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html).
- The database user has proper permissions to access at least one bucket.
