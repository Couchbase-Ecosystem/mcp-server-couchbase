# couchbase_utils

`couchbase_utils.cb_utils.CB` provides minimal Couchbase helpers used by demo and tests.

## Main capabilities

1. Connect to cluster
	- `connect_to_cluster(connection_string, username, password, timeout_seconds=30)`
2. List namespaces
	- `get_all_namespaces(timeout_seconds=30)`
	- Returns `[[bucket, scope, collection], ...]`
3. Run `INFER` for collection schema variants
	- `get_inferred_schema(bucket_name, scope_name, collection_name, timeout_seconds=60)`
4. Scope operations
	- `create_scope(...)`
	- `delete_scope(...)`
5. Collection operations
	- `create_collection(...)`
	- `delete_collection(...)`
6. KV operations
	- `upsert_document(...)`
	- `remove_document(...)`

## Notes

- SQL++ identifiers are safely quoted.
- `get_all_namespaces()` uses management APIs and normalizes bucket-name extraction across return shapes.
- `get_inferred_schema()` normalizes both direct dict rows and nested list row shapes.
