---
sidebar_position: 4
title: Query & Indexing Tools
---

# Query & Indexing Tools

Tools for executing SQL++ queries and managing indexes.

**Source:** [`src/tools/query.py`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/query.py), [`src/tools/index.py`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/index.py)

---

## `run_sql_plus_plus_query`

Run a [SQL++ query](https://www.couchbase.com/sqlplusplus/) on a scope and return the results as a list of JSON objects.

The query runs on the specified scope in the specified bucket. Use collection names directly without bucket/scope prefixes — the scope context is set automatically.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket name |
| `scope_name` | `str` | Yes | The scope to run the query on |
| `query` | `str` | Yes | The SQL++ query to execute |

**Returns:** A list of dictionaries representing the query results.

**Example:**
```sql
-- Correct: Use collection names directly
SELECT * FROM airline WHERE country = 'United States' LIMIT 5

-- Incorrect: Don't use fully qualified names
SELECT * FROM `travel-sample`.inventory.airline WHERE country = 'United States'
```

:::info Read-Only Mode and Queries
When `CB_MCP_READ_ONLY_MODE=true` (default) or `CB_MCP_READ_ONLY_QUERY_MODE=true`, the server parses queries using the `lark-sqlpp` library and blocks:
- **Data modification queries**: INSERT, UPDATE, DELETE, MERGE
- **Structure modification queries**: CREATE, DROP, ALTER
:::

---

## `list_indexes`

List all indexes in the cluster with their definitions, with optional filtering. Uses the Index Service REST API (`/getIndexStatus`) to retrieve index information directly.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | No | Filter indexes by bucket name |
| `scope_name` | `str` | No | Filter by scope name (requires `bucket_name`) |
| `collection_name` | `str` | No | Filter by collection name (requires `bucket_name` and `scope_name`) |
| `index_name` | `str` | No | Filter by index name (requires all above) |
| `include_raw_index_stats` | `bool` | No | Include raw index stats from the API. Default: `false` |

**Returns:** A list of dictionaries, each containing:
- `name` — Index name
- `definition` — Cleaned-up CREATE INDEX statement
- `status` — Current status (e.g., `"Ready"`, `"Building"`, `"Deferred"`)
- `isPrimary` — Whether this is a primary index
- `bucket` — Bucket name
- `scope` — Scope name
- `collection` — Collection name
- `raw_index_stats` — Complete raw API data (only if `include_raw_index_stats=true`)

---

## `get_index_advisor_recommendations`

Get index recommendations from the Couchbase Index Advisor for a given SQL++ query. The advisor analyzes the query and suggests optimal indexes. Works with SELECT, UPDATE, DELETE, or MERGE queries.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket name |
| `scope_name` | `str` | Yes | The scope name |
| `query` | `str` | Yes | The SQL++ query to analyze |

**Returns:** A dictionary containing:
- `current_used_indexes` — Array of currently used indexes
- `recommended_indexes` — Array of recommended secondary indexes
- `recommended_covering_indexes` — Array of recommended covering indexes
- `summary` — Counts of current and recommended indexes, and whether any recommendations exist

Each index object contains:
- `index` — The `CREATE INDEX` SQL++ command
- `statements` — Array of statement objects with the query and run count
