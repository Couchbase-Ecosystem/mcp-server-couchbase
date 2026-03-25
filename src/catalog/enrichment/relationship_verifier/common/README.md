# common

Shared data models and helpers used across schema extraction, relationship inference, and verification.

## Relationship models

`common.relationships` exposes three relationship dataclasses:

- `PrimaryKeyRelationship(table, columns)`
- `ForeignKeyRelationship(child_table, child_columns, parent_table, parent_columns)`
- `InferredRelationship(kind, table1, table2, foreign_key_table, from_columns, to_columns, connecting_table)`

Use `AnyRelationship` when accepting a mixed list.

### `META().id` sentinel for FKs

Use `META_ID_SENTINEL` (`"$meta_id"`) in `ForeignKeyRelationship.parent_columns` when the parent side should reference `META(parent_doc).id` instead of a normal field path.

Helpers:

- `uses_meta_id(columns)`
- `relationship_from_dict(data)`

## Path parsing for nested and array fields

`common.path_utils` provides SQL++ path parsing utilities:

- `ParsedPath(unnest_clauses, column_ref)`
- `parse_column_path(root_alias, path, quote_fn=...)`

Path notation:

- `a.b.c` → nested object access
- `a.[].c` → unnest `a`, then access `c` from array element
- `a.[].c.[].d` → multiple `UNNEST` stages

`[]` must be a standalone segment and must follow a field segment.
