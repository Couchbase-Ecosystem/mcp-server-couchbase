# verification_using_data

Data-backed verifier for relationship candidates using SQL++ against Couchbase collections.

## Main entry point

Use `RelationshipVerifier` from `relationship_verifier.py`.

Inputs are lists of `AnyRelationship` from `common.relationships`:

- `PrimaryKeyRelationship`
- `ForeignKeyRelationship`
- `InferredRelationship` (`OO` and `OM` are currently evaluated)

Constructor configuration:

- `keyspace_map: dict[str, str]` (optional)
	- Maps normalized collection names to `scope.collection`.
	- Required when relationship input uses unqualified collection names.
- `index_map: dict[str, list[list[str]]]` (optional)
	- Lists index keys per normalized collection name.
	- Used to decide whether a query can scan fully or should be capped.

## Verification pipeline

`RelationshipVerifier.verify()` runs five stages:

1. `devise_operations` — plan task objects per relationship.
2. `collect_unique_operations` — deduplicate tasks by `task_id`.
3. `convert_operations_to_queries` — map each task to one SQL++ query.
4. `run_task_queries` — execute SQL++ and collect first-row outputs.
5. `coalesce_results` — evaluate outputs into `VerificationResult` per input relationship.

If a task query fails, the verifier stores `{"error": "..."}` for that task and marks the relationship invalid with a `*_check_unavailable` failure reason.

## Primary-key evaluation (`PK`)

For `PrimaryKeyRelationship(table, columns)`, all checks below must pass:

1. Column exists in sampled data (`exists_count > 0`) for each column.
2. Column values are scalar (`nested_count == 0`) for each column.
3. Column values are never null/missing (`null_count == 0`) for each column.
4. Composite key tuple is unique (`duplicate_groups == 0`) for `columns`.

Failure reasons are emitted with `pk_*` prefixes.

## Foreign-key evaluation (`FK`)

For `ForeignKeyRelationship(child_table, child_columns, parent_table, parent_columns)`, checks are:

1. Child columns observed in sampled data (`exists_count > 0`).
2. Parent columns observed in sampled data (`exists_count > 0`), except `$meta_id`.
3. Referential inclusion (`missing_count == 0`).
4. Child/parent type compatibility (`type_mismatch_count == 0`) per column pair.
5. Parent tuple uniqueness (`duplicate_groups == 0`) unless parent is `$meta_id`.
6. Parent columns non-null (`null_count == 0`) unless parent is `$meta_id`.

Failure reasons are emitted with `fk_*` prefixes.

## Inferred relationship evaluation (`OO` and `OM`)

For inferred relationships, the verifier first applies FK-style checks using:
- value-set inclusion (`missing_count`),
- type compatibility (`type_mismatch_count`),
- parent tuple uniqueness (`duplicate_groups` on parent columns),
- parent-column not-null checks (`null_count`).

Then it applies multiplicity logic:
- `OO` passes only when FK-style checks pass **and** child-side duplicate groups on `from_columns` are exactly `0`.
- `OM` passes when FK-style checks pass; child-side uniqueness on `from_columns` is not required.

This makes OM lenient to sampled data that currently appears 1-to-1, while still rejecting broken referential mappings.

## Task types and expected counts

Defined in `tasks.py`:

- `ColumnExistsTask` → `exists_count`
- `ColumnNotObjectTask` → `nested_count`
- `ColumnNotNullTask` → `null_count`
- `ColumnUniquenessTask` → `duplicate_groups`
- `ValueSetInclusionTask` → `missing_count`
- `ColumnTypeCompatibilityTask` → `type_mismatch_count`

## Query limits and index-awareness

- `MAX_UNINDEXED_SCAN_ROWS = 50_000`.
- If `index_map` indicates index coverage for required columns, queries run without `LIMIT`.
- If no covering index is known, queries are generated with a capped scan (`LIMIT 50000`).

## Nested fields and arrays

Column paths support dotted and array traversal notation.

Examples:

- `a.b`
- `a.[].c`
- `a.[].c.[].d`

The verifier compiles these using `common.path_utils.parse_column_path()` and injects required `UNNEST` clauses into SQL++ `FROM` blocks.

## Special identifier: `$meta_id`

`$meta_id` is a sentinel for parent-side `META(alias).id` access.

- Supported via `common.relationships.META_ID_SENTINEL`.
- For FK/OO/OM checks, parent-column existence/type/non-null checks are skipped when parent column is `$meta_id`.
- Referential inclusion still applies using document IDs.

## Type compatibility nuance

`ColumnTypeCompatibilityTask` allows string/number interchangeability. A child value with type `"string"` or `"number"` is treated as compatible when parent data contains at least one non-null value of either type.

## Logging

- Logger code lives in `logger/`.
- Runtime log files are written to `logger/logs/`.
- Set `CB_VERIFIER_LOG_LEVEL` to control verbosity (default: `INFO`).
