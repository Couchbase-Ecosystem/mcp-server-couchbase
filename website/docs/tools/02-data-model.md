---
sidebar_position: 2
title: Data Model & Schema Tools
---

# Data Model & Schema Discovery Tools

Tools for exploring buckets, scopes, collections, and schemas in your Couchbase cluster.

**Source:** [`src/tools/server.py`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/server.py), [`src/tools/query.py`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/query.py)

---

## `get_buckets_in_cluster`

Get the names of all accessible buckets in the cluster.

**Parameters:** None

**Returns:** A list of bucket name strings.

---

## `get_scopes_in_bucket`

Get the names of all scopes in the given bucket.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket to list scopes for |

**Returns:** A list of scope name strings.

---

## `get_collections_in_scope`

Get the names of all collections in the given scope and bucket.

:::note
This tool requires the cluster to have the Query service running.
:::

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket name |
| `scope_name` | `str` | Yes | The scope to list collections for |

**Returns:** A list of collection name strings.

---

## `get_scopes_and_collections_in_bucket`

Get all scopes and their collections in the specified bucket in a single call.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket to list scopes and collections for |

**Returns:** A dictionary with scope names as keys and lists of collection names as values.

**Example response:**
```json
{
  "inventory": ["airline", "airport", "hotel", "route"],
  "_default": ["_default"]
}
```

---

## `get_schema_for_collection`

Get the schema for a collection by running a Couchbase `INFER` query. Returns the inferred document structure of the collection.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket name |
| `scope_name` | `str` | Yes | The scope name |
| `collection_name` | `str` | Yes | The collection to infer the schema for |

**Returns:** A dictionary containing:
- `collection_name` — The collection name
- `schema` — Inferred schema details (field names, types, sample sizes)
