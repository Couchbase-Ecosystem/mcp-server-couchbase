---
sidebar_position: 1
title: Prerequisites
---

# Prerequisites

Before using the Couchbase MCP Server, ensure you have the following:

## Required

- **Python 3.10 or higher** — The server requires Python 3.10+ (supports 3.10, 3.11, 3.12, 3.13).
- **A running Couchbase cluster** — Either:
  - [Couchbase Capella](https://docs.couchbase.com/cloud/get-started/create-account.html#getting-started) (free tier available) — fully managed cloud version
  - A self-hosted Couchbase Server instance
- **[uv](https://docs.astral.sh/uv/)** — Fast Python package manager, used to run the server.
- **An MCP client** — Such as [Claude Desktop](https://claude.ai/download), [Cursor](https://cursor.sh/), VS Code, Windsurf Editor, or any [MCP-compatible client](https://modelcontextprotocol.io/clients).

## Getting Sample Data

The easiest way to get started is with Couchbase Capella's free tier. You can [import sample datasets](https://docs.couchbase.com/cloud/clusters/data-service/import-data-documents.html#import-sample-data) like `travel-sample` or import your own data.

## Couchbase Cluster Access

You'll need one of the following authentication methods:

- **Basic Authentication**: A username and password with access to the required buckets.
- **mTLS Authentication**: A client certificate and key for mutual TLS authentication.

Ensure that:
- The cluster is accessible from the machine running the MCP server.
- If using Capella, the machine's IP address is [allowed](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html).
- The database user has proper permissions to access at least one bucket.
