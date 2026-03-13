---
sidebar_position: 1
title: Overview
slug: /
---

# Couchbase MCP Server

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/) [![PyPI version](https://badge.fury.io/py/couchbase-mcp-server.svg)](https://pypi.org/project/couchbase-mcp-server/)

Couchbase MCP Server is a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server implementation that lets LLMs directly interact with data stored in Couchbase clusters through a rich set of tools.

It exposes capabilities like cluster health checks, schema discovery, document key-value operations, SQL++ querying, and query performance analysis in a controlled way that is safe by default via read-only modes and fine-grained tool disabling. The server supports multiple transport modes, including stdio and Streamable HTTP, so it can be run locally or shared across teams.

## Use Cases

- **Database exploration** — Ask an LLM to list buckets, discover schemas, and understand your data model without writing queries.
- **Query assistance** — Have an AI assistant write and run SQL++ queries against your cluster using natural language.
- **Performance analysis** — Identify slow queries, missing indexes, and optimization opportunities through conversation.
- **Cluster monitoring** — Check cluster health, running services, and connection status through your AI assistant.
- **Document operations** — Read, insert, update, and delete documents using natural language (when write mode is enabled).

## Tools

The server exposes 23 tools across 5 categories. See the [Tools](/tools/cluster-health) page for full details.

- **Cluster Setup & Health**: `get_server_configuration_status`, `test_cluster_connection`, `get_cluster_health_and_services`
- **Data Model & Schema Discovery**: `get_buckets_in_cluster`, `get_scopes_in_bucket`, `get_collections_in_scope`, `get_scopes_and_collections_in_bucket`, `get_schema_for_collection`
- **Document KV Operations**: `get_document_by_id`, `upsert_document_by_id`, `insert_document_by_id`, `replace_document_by_id`, `delete_document_by_id`
- **Query and Indexing**: `list_indexes`, `get_index_advisor_recommendations`, `run_sql_plus_plus_query`
- **Query Performance Analysis**: `get_longest_running_queries`, `get_most_frequent_queries`, `get_queries_with_largest_response_sizes`, `get_queries_with_large_result_count`, `get_queries_using_primary_index`, `get_queries_not_using_covering_index`, `get_queries_not_selective`

## Tutorials

Browse MCP tutorials on the [Couchbase Developer Portal](https://developer.couchbase.com/tutorials/?search=model%20context%20protocol).

## Releases

The latest release is available on [PyPI](https://pypi.org/project/couchbase-mcp-server/) and [Docker Hub](https://hub.docker.com/r/couchbaseecosystem/mcp-server-couchbase). See the [Release Notes](/release-notes) for version history and details.

Upcoming features include:

- **Explain Queries** — New tool to retrieve query execution plans for LLM analysis
- **Search-Based Tools** — Tools for Search queries, indexes, and performance analysis

## Support Policy

This project is **Couchbase community-maintained** — not officially supported by Couchbase support. Engineers actively monitor and maintain this repo and will resolve issues on a best-effort basis.

- **Bug reports**: [Open a GitHub issue](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues)
- **Feature requests**: [Open a GitHub issue](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues) with the "enhancement" label
- **Questions**: [Open a GitHub issue](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues)

This is **not** covered by Couchbase Enterprise Support agreements. Pull requests and contributions from the community are welcome — see [Contributing](/contributing).
