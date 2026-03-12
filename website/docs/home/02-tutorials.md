---
sidebar_position: 2
title: Tutorials
---

# Tutorials

Hands-on tutorials to help you get the most out of the Couchbase MCP Server. These tutorials assume you have completed the [Quick Start](/docs/get-started/quickstart) and have a working MCP client connected to a Couchbase cluster with the `travel-sample` dataset.

---

## Tutorial 1: Explore Your Cluster with an LLM

Learn how to use natural language to discover and understand your Couchbase data model.

### What you'll learn

- How to navigate buckets, scopes, and collections
- How to infer document schemas
- How to run SQL++ queries through your AI assistant

### Steps

**Step 1: Discover your buckets**

Ask your AI assistant:

> "What buckets are in my cluster?"

The assistant will call `get_buckets_in_cluster` and return a list of bucket names (e.g., `travel-sample`).

**Step 2: Explore scopes and collections**

> "Show me all scopes and collections in the travel-sample bucket."

The assistant will call `get_scopes_and_collections_in_bucket` with `bucket_name: "travel-sample"` and return the full hierarchy, for example:

```
inventory: airline, airport, hotel, landmark, route
tenant_agent_00: users, bookings
```

**Step 3: Understand a collection's schema**

> "What does a document in the airline collection look like?"

The assistant will call `get_schema_for_collection` with `bucket_name: "travel-sample"`, `scope_name: "inventory"`, `collection_name: "airline"` and return the inferred field names, types, and sample sizes.

**Step 4: Query your data**

> "Find all airlines based in the United States."

The assistant will call `run_sql_plus_plus_query` with a SQL++ query like:

```sql
SELECT a.* FROM `travel-sample`.inventory.airline a WHERE a.country = 'United States' LIMIT 10;
```

**Step 5: Inspect a specific document**

> "Get the document with ID 'airline_10' from the airline collection."

The assistant will call `get_document_by_id` with `bucket_name: "travel-sample"`, `scope_name: "inventory"`, `collection_name: "airline"`, `document_id: "airline_10"`.

### Summary

In this tutorial, you used five tools through natural language:

| Tool | Purpose |
|------|---------|
| `get_buckets_in_cluster` | Listed all buckets |
| `get_scopes_and_collections_in_bucket` | Explored the data hierarchy |
| `get_schema_for_collection` | Inferred document structure |
| `run_sql_plus_plus_query` | Queried data with SQL++ |
| `get_document_by_id` | Retrieved a specific document |

---

## Tutorial 2: Monitor Query Performance

Learn how to use the performance analysis tools to identify slow queries and optimization opportunities.

### What you'll learn

- How to find slow and frequent queries
- How to identify queries using primary indexes
- How to spot queries that could benefit from better indexes

### Prerequisites

Your cluster needs completed query history in `system:completed_requests`. Run a few queries against your cluster first (the previous tutorial is a good starting point).

### Steps

**Step 1: Find your slowest queries**

> "What are the longest running queries on my cluster?"

The assistant will call `get_longest_running_queries` with `limit: 10` and return queries ranked by average service time.

**Step 2: Check for frequently executed queries**

> "Show me the most frequently executed queries."

The assistant will call `get_most_frequent_queries` with `limit: 10` and return queries ranked by execution count.

**Step 3: Identify primary index usage**

> "Are any queries using a primary index?"

The assistant will call `get_queries_using_primary_index`. Primary index usage often indicates a full collection scan — a significant performance concern.

**Step 4: Find queries without covering indexes**

> "Which queries aren't using covering indexes?"

The assistant will call `get_queries_not_using_covering_index` to find queries that perform index scans but also require document fetches. Adding a covering index for these queries eliminates the fetch phase.

**Step 5: Check for non-selective queries**

> "Are there any queries that aren't selective?"

The assistant will call `get_queries_not_selective` to find queries where the index scan returns far more documents than the final result count, indicating the index is scanning many documents that get filtered out.

**Step 6: Check for large response sizes**

> "Which queries return the most data?"

The assistant will call `get_queries_with_largest_response_sizes` to identify queries returning excessive data.

**Step 7: Get index recommendations**

> "What indexes would improve the query: SELECT * FROM `travel-sample`.inventory.airline WHERE country = 'United States'?"

The assistant will call `get_index_advisor_recommendations` with your query and return Couchbase Index Advisor suggestions.

### Summary

In this tutorial, you used seven tools to analyze query performance:

| Tool | Purpose |
|------|---------|
| `get_longest_running_queries` | Found slowest queries |
| `get_most_frequent_queries` | Found most-executed queries |
| `get_queries_using_primary_index` | Identified full scans |
| `get_queries_not_using_covering_index` | Found fetch-heavy queries |
| `get_queries_not_selective` | Found poorly selective queries |
| `get_queries_with_largest_response_sizes` | Found data-heavy queries |
| `get_index_advisor_recommendations` | Got index optimization advice |
