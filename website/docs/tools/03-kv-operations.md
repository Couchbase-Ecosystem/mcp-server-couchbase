---
sidebar_position: 3
title: Document KV Operations
---

# Document KV Operations

Tools for document CRUD operations using Couchbase's Key-Value (KV) service. These provide direct document access by ID — the fastest way to read and write individual documents.

**Source:** [`src/tools/kv.py`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/src/tools/kv.py)

:::info Write Tools and Read-Only Mode
The write tools (`upsert`, `insert`, `replace`, `delete`) are **not loaded** when `CB_MCP_READ_ONLY_MODE=true` (the default). They will not appear in tool discovery and cannot be invoked. Set `CB_MCP_READ_ONLY_MODE=false` to enable them.
:::

---

## `get_document_by_id`

Retrieve a document by its ID from the specified scope and collection. Raises an exception if the document is not found.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket name |
| `scope_name` | `str` | Yes | The scope name |
| `collection_name` | `str` | Yes | The collection name |
| `document_id` | `str` | Yes | The document ID to retrieve |

**Returns:** The document content as a dictionary.

---

## `upsert_document_by_id`

Insert or update a document by its ID. Creates the document if it doesn't exist, updates it if it does.

:::warning
Only use this tool when the user explicitly requests an "upsert" operation or states they want to "insert or update" a document. This tool should not be used as a fallback when `insert_document_by_id` or `replace_document_by_id` fails.
:::

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket name |
| `scope_name` | `str` | Yes | The scope name |
| `collection_name` | `str` | Yes | The collection name |
| `document_id` | `str` | Yes | The document ID |
| `document_content` | `dict` | Yes | The document content to upsert |

**Returns:** `true` on success, `false` on failure.

**Availability:** Disabled when `CB_MCP_READ_ONLY_MODE=true` (default).

---

## `insert_document_by_id`

Insert a new document by its ID. This operation **fails if the document already exists**.

:::warning
If this operation fails because the document exists, do not automatically retry with `replace` or `upsert`. Report the failure to the user so they can decide.
:::

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket name |
| `scope_name` | `str` | Yes | The scope name |
| `collection_name` | `str` | Yes | The collection name |
| `document_id` | `str` | Yes | The document ID |
| `document_content` | `dict` | Yes | The document content to insert |

**Returns:** `true` on success, `false` on failure (including if document already exists).

**Availability:** Disabled when `CB_MCP_READ_ONLY_MODE=true` (default).

---

## `replace_document_by_id`

Replace an existing document by its ID. This operation **fails if the document does not exist**.

:::warning
If this operation fails because the document doesn't exist, do not automatically retry with `insert` or `upsert`. Report the failure to the user so they can decide.
:::

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket name |
| `scope_name` | `str` | Yes | The scope name |
| `collection_name` | `str` | Yes | The collection name |
| `document_id` | `str` | Yes | The document ID |
| `document_content` | `dict` | Yes | The replacement document content |

**Returns:** `true` on success, `false` on failure (including if document does not exist).

**Availability:** Disabled when `CB_MCP_READ_ONLY_MODE=true` (default).

---

## `delete_document_by_id`

Delete a document by its ID from the specified scope and collection.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bucket_name` | `str` | Yes | The bucket name |
| `scope_name` | `str` | Yes | The scope name |
| `collection_name` | `str` | Yes | The collection name |
| `document_id` | `str` | Yes | The document ID to delete |

**Returns:** `true` on success, `false` on failure.

**Availability:** Disabled when `CB_MCP_READ_ONLY_MODE=true` (default).
