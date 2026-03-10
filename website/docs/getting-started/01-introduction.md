---
sidebar_position: 1
title: Introduction
---

# Couchbase MCP Server

An [MCP](https://modelcontextprotocol.io/) server implementation that allows Large Language Models (LLMs) to directly interact with Couchbase clusters through the Model Context Protocol standard.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/) [![PyPI version](https://badge.fury.io/py/couchbase-mcp-server.svg)](https://pypi.org/project/couchbase-mcp-server/)

## What is this?

The Couchbase MCP Server bridges the gap between LLMs and your Couchbase database. It exposes 23 tools across 5 categories that let AI assistants query, explore, and manage your Couchbase clusters using natural language.

## Features

### Cluster Setup & Health Tools

| Tool Name | Description |
|-----------|-------------|
| `get_server_configuration_status` | Get the status of the MCP server |
| `test_cluster_connection` | Check the cluster credentials by connecting to the cluster |
| `get_cluster_health_and_services` | Get cluster health status and list of all running services |

### Data Model & Schema Discovery Tools

| Tool Name | Description |
|-----------|-------------|
| `get_buckets_in_cluster` | Get a list of all the buckets in the cluster |
| `get_scopes_in_bucket` | Get a list of all the scopes in the specified bucket |
| `get_collections_in_scope` | Get a list of all the collections in a specified scope and bucket |
| `get_scopes_and_collections_in_bucket` | Get a list of all the scopes and collections in the specified bucket |
| `get_schema_for_collection` | Get the structure for a collection |

### Document KV Operations Tools

| Tool Name | Description |
|-----------|-------------|
| `get_document_by_id` | Get a document by ID from a specified scope and collection |
| `upsert_document_by_id` | Upsert a document by ID. **Disabled by default when `CB_MCP_READ_ONLY_MODE=true`.** |
| `insert_document_by_id` | Insert a new document by ID (fails if document exists). **Disabled by default when `CB_MCP_READ_ONLY_MODE=true`.** |
| `replace_document_by_id` | Replace an existing document by ID (fails if document doesn't exist). **Disabled by default when `CB_MCP_READ_ONLY_MODE=true`.** |
| `delete_document_by_id` | Delete a document by ID. **Disabled by default when `CB_MCP_READ_ONLY_MODE=true`.** |

### Query and Indexing Tools

| Tool Name | Description |
|-----------|-------------|
| `list_indexes` | List all indexes in the cluster with their definitions, with optional filtering |
| `get_index_advisor_recommendations` | Get index recommendations from Couchbase Index Advisor for a given SQL++ query |
| `run_sql_plus_plus_query` | Run a [SQL++ query](https://www.couchbase.com/sqlplusplus/) on a specified scope |

### Query Performance Analysis Tools

| Tool Name | Description |
|-----------|-------------|
| `get_longest_running_queries` | Get longest running queries by average service time |
| `get_most_frequent_queries` | Get most frequently executed queries |
| `get_queries_with_largest_response_sizes` | Get queries with the largest response sizes |
| `get_queries_with_large_result_count` | Get queries with the largest result counts |
| `get_queries_using_primary_index` | Get queries that use a primary index (potential performance concern) |
| `get_queries_not_using_covering_index` | Get queries that don't use a covering index |
| `get_queries_not_selective` | Get queries that are not selective |

## Support Policy

This project is **Couchbase community-maintained** — not officially supported by Couchbase support. Engineers actively monitor and maintain this repo and will resolve issues on a best-effort basis.

For help or bug reports, [open a GitHub issue](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues).
