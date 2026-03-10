---
sidebar_position: 5
title: Performance Analysis Tools
---

# Query Performance Analysis Tools

Tools for analyzing query performance using data from the Couchbase `system:completed_requests` catalog. These tools help identify performance bottlenecks and optimization opportunities.

**Source:** [`src/tools/query.py`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/query.py)

:::note
All performance analysis tools query `system:completed_requests`. If no completed queries are available, they return a message indicating no data was found.

**Filtering varies by tool:**
- `get_longest_running_queries`, `get_queries_with_largest_response_sizes`, `get_queries_with_large_result_count` — exclude INFER, CREATE INDEX, and system keyspace queries
- `get_most_frequent_queries` — excludes INFER, CREATE INDEX, EXPLAIN, ADVISE, and system keyspace queries
- `get_queries_using_primary_index`, `get_queries_not_using_covering_index` — exclude system keyspace queries only
- `get_queries_not_selective` — no INFER/CREATE INDEX/system keyspace filtering; selects only queries where `phaseCounts.indexScan > resultCount`
:::

---

## `get_longest_running_queries`

Get the N longest running queries by average service time, grouped by statement.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | `int` | No | `10` | Number of queries to return |

**Returns:** List of queries with `statement`, `avgServiceTime` (formatted duration string), and `queries` (execution count).

---

## `get_most_frequent_queries`

Get the N most frequently executed queries, grouped by statement.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | `int` | No | `10` | Number of queries to return |

**Returns:** List of queries with `statement` and `queries` (frequency count).

---

## `get_queries_with_largest_response_sizes`

Get queries with the largest response sizes, useful for identifying queries that return excessive data.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | `int` | No | `10` | Number of queries to return |

**Returns:** List of queries with `statement`, `avgResultSizeBytes`, `avgResultSizeKB`, `avgResultSizeMB`, and `queries` (execution count).

---

## `get_queries_with_large_result_count`

Get queries with the largest result counts.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | `int` | No | `10` | Number of queries to return |

**Returns:** List of queries with `statement`, `avgResultCount`, and `queries` (execution count).

---

## `get_queries_using_primary_index`

Get queries that use a primary index. Primary index usage is a potential performance concern — these queries typically perform full scans.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | `int` | No | `10` | Number of queries to return |

**Returns:** List of full query records from `system:completed_requests` where `phaseCounts.primaryScan` exists, ordered by result count.

---

## `get_queries_not_using_covering_index`

Get queries that perform index scans but also require document fetches — indicating they don't use a covering index. Adding a covering index for these queries could improve performance by eliminating the fetch phase.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | `int` | No | `10` | Number of queries to return |

**Returns:** List of full query records from `system:completed_requests` where both `phaseCounts.indexScan` and `phaseCounts.fetch` exist, ordered by result count.

---

## `get_queries_not_selective`

Get queries that are not very selective — where index scans return significantly more documents than the final result. This indicates the index is scanning many documents that get filtered out, suggesting a more selective index could help.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | `int` | No | `10` | Number of queries to return |

**Returns:** List of queries with `statement` and `diff` (average difference between `phaseCounts.indexScan` and `resultCount`), ordered by the difference.
