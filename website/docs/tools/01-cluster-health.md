---
sidebar_position: 1
title: Tools
---

# Tools

The Couchbase MCP Server exposes 23 tools across 5 categories. Each tool is available to LLMs through the MCP protocol.

| Category | Tool | Description |
|----------|------|-------------|
| **Cluster Setup & Health** | `get_server_configuration_status` | Get the status and configuration of the MCP server |
| | `test_cluster_connection` | Check the cluster credentials by connecting to the cluster |
| | `get_cluster_health_and_services` | Get cluster health status and list of all running services |
| **Data Model & Schema Discovery** | `get_buckets_in_cluster` | Get a list of all the buckets in the cluster |
| | `get_scopes_in_bucket` | Get a list of all the scopes in the specified bucket |
| | `get_collections_in_scope` | Get a list of all the collections in a specified scope and bucket |
| | `get_scopes_and_collections_in_bucket` | Get a list of all the scopes and collections in the specified bucket |
| | `get_schema_for_collection` | Infer the document structure for a collection |
| **Document KV Operations** | `get_document_by_id` | Get a document by ID from a specified scope and collection |
| | `upsert_document_by_id` | Insert or update a document by ID |
| | `insert_document_by_id` | Insert a new document by ID (fails if document exists) |
| | `replace_document_by_id` | Replace an existing document by ID (fails if document doesn't exist) |
| | `delete_document_by_id` | Delete a document by ID |
| **Query and Indexing** | `run_sql_plus_plus_query` | Run a [SQL++ query](https://www.couchbase.com/sqlplusplus/) on a specified scope |
| | `list_indexes` | List all indexes in the cluster with their definitions, with optional filtering |
| | `get_index_advisor_recommendations` | Get index recommendations from Couchbase Index Advisor for a given SQL++ query |
| **Query Performance Analysis** | `get_longest_running_queries` | Get longest running queries by average service time |
| | `get_most_frequent_queries` | Get most frequently executed queries |
| | `get_queries_with_largest_response_sizes` | Get queries with the largest response sizes |
| | `get_queries_with_large_result_count` | Get queries with the largest result counts |
| | `get_queries_using_primary_index` | Get queries that use a primary index (potential performance concern) |
| | `get_queries_not_using_covering_index` | Get queries that don't use a covering index |
| | `get_queries_not_selective` | Get queries that are not selective |

:::note
- Document KV write tools are disabled by default when `CB_MCP_READ_ONLY_MODE=true`.
- Query Performance Analysis tools query `system:completed_requests`.
:::

Source code: [server.py](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/server.py) | [kv.py](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/kv.py) | [query.py](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/query.py) | [index.py](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/index.py)
