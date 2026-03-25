"""Data-backed verifier for PK/FK/OO/OM relationships."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import chain
from typing import Any

from catalog.enrichment.relationship_verifier.common.path_utils import (
    ParsedPath,
    parse_column_path,
)
from catalog.enrichment.relationship_verifier.common.relationships import (
    META_ID_SENTINEL,
    AnyRelationship,
    ForeignKeyRelationship,
    InferredRelationship,
    PrimaryKeyRelationship,
    uses_meta_id,
)
from catalog.enrichment.relationship_verifier.couchbase_utils.cb_utils import CB
from catalog.enrichment.relationship_verifier.logger import get_verifier_logger
from catalog.enrichment.relationship_verifier.tasks import (
    AnyTask,
    ColumnExistsTask,
    ColumnNotNullTask,
    ColumnNotObjectTask,
    ColumnTypeCompatibilityTask,
    ColumnUniquenessTask,
    ValueSetInclusionTask,
)

__all__ = ["RelationshipVerifier", "VerificationResult"]


@dataclass(frozen=True, slots=True)
class VerificationResult:
    """Result of verifying one relationship candidate."""

    relationship: AnyRelationship
    is_valid: bool
    failure_reason: str | None = None


class RelationshipVerifier:
    """Plan, dedupe, execute, and evaluate data-backed relationship checks."""

    MAX_UNINDEXED_SCAN_ROWS = 50_000

    def __init__(
        self,
        cb: CB,
        bucket_name: str,
        keyspace_map: dict[str, str] | None = None,
        index_map: dict[str, list[list[str]]] | None = None,
    ) -> None:
        self.cb = cb
        self.bucket_name = bucket_name
        self.keyspace_map = keyspace_map or {}
        self.index_map = index_map or {}

    def verify(self, relationships: list[AnyRelationship]) -> list[VerificationResult]:
        """Verify relationship candidates against data in Couchbase."""
        logger = get_verifier_logger()
        logger.info(f"Starting verification for bucket '{self.bucket_name}'")
        logger.info(f"Total relationships to verify: {len(relationships)}")

        planned_operations = self.devise_operations(relationships)
        unique_operations = self.collect_unique_operations(planned_operations)
        task_queries = self.convert_operations_to_queries(unique_operations)
        task_outputs = self.run_task_queries(task_queries)
        results = self.coalesce_results(
            relationships=relationships,
            planned_operations=planned_operations,
            task_outputs=task_outputs,
        )

        # Log summary
        valid_count = sum(1 for r in results if r.is_valid)
        invalid_count = len(results) - valid_count
        logger.info(
            f"Verification complete: {valid_count} valid, {invalid_count} invalid"
        )

        return results

    def devise_operations(
        self, relationships: list[AnyRelationship]
    ) -> dict[int, list[AnyTask]]:
        """Build operation plans per input relationship index."""
        logger = get_verifier_logger()
        logger.info("Stage 1: Planning operations for each relationship")

        planned_operations: dict[int, list[AnyTask]] = {}
        for relationship_index, relationship in enumerate(relationships):
            planned_operations[relationship_index] = (
                self.devise_operations_for_relationship(relationship)
            )
            logger.debug(
                f"Relationship {relationship_index} ({relationship.kind}): "
                f"{len(planned_operations[relationship_index])} operations planned"
            )

        total_ops = sum(len(ops) for ops in planned_operations.values())
        logger.info(f"Total operations planned: {total_ops}")
        return planned_operations

    def devise_operations_for_relationship(
        self, relationship: AnyRelationship
    ) -> list[AnyTask]:
        """Build the per-relationship operation list."""
        if isinstance(relationship, PrimaryKeyRelationship):
            return self._plan_primary_key_operations(relationship)

        if isinstance(relationship, ForeignKeyRelationship):
            return self._plan_foreign_key_operations(relationship)

        if relationship.kind == "OO":
            return self._plan_one_to_one_operations(relationship)

        if relationship.kind == "OM":
            return self._plan_one_to_many_operations(relationship)

        raise ValueError(
            f"Unsupported inferred relationship kind for verification: {relationship.kind!r}"
        )

    def collect_unique_operations(
        self,
        planned_operations: dict[int, list[AnyTask]],
    ) -> dict[str, AnyTask]:
        """Deduplicate operations so repeated SQL++ queries are not sent."""
        logger = get_verifier_logger()
        logger.info("Stage 2: Deduplicating operations")

        unique_operations: dict[str, AnyTask] = {}
        for operation in chain.from_iterable(planned_operations.values()):
            unique_operations.setdefault(operation.task_id, operation)

        logger.info(f"Unique operations after deduplication: {len(unique_operations)}")
        return unique_operations

    def convert_operations_to_queries(
        self, operations: dict[str, AnyTask]
    ) -> dict[str, str]:
        """Convert each task into exactly one SQL++ query."""
        logger = get_verifier_logger()
        logger.info("Stage 3: Converting operations to SQL++ queries")

        queries = {
            task_id: self._operation_to_query(operation)
            for task_id, operation in operations.items()
        }

        logger.info(f"Generated {len(queries)} SQL++ queries")
        return queries

    def run_task_queries(self, task_queries: dict[str, str]) -> dict[str, Any]:
        """Run SQL++ task queries and return task-id keyed outputs."""
        logger = get_verifier_logger()
        logger.info("Stage 4: Executing SQL++ queries")

        task_outputs: dict[str, Any] = {}
        for task_id, query in task_queries.items():
            try:
                rows = self.cb.run_query(query)
                task_outputs[task_id] = rows[0] if rows else None
            except Exception as error:
                logger.warning(f"Query failed for task {task_id}: {error}")
                task_outputs[task_id] = {
                    "error": str(error),
                }

        logger.info(f"Query execution complete: {len(task_outputs)} results")
        return task_outputs

    def coalesce_results(
        self,
        *,
        relationships: list[AnyRelationship],
        planned_operations: dict[int, list[AnyTask]],
        task_outputs: dict[str, Any],
    ) -> list[VerificationResult]:
        """Coalesce outputs and return verification status per relationship."""
        logger = get_verifier_logger()
        logger.info("Stage 5: Coalescing results for each relationship")

        results: list[VerificationResult] = []
        for relationship_index, relationship in enumerate(relationships):
            operations = planned_operations[relationship_index]
            is_valid, failure_reason = self._evaluate_relationship_with_reason(
                relationship,
                operations,
                task_outputs,
            )
            result = VerificationResult(
                relationship=relationship,
                is_valid=is_valid,
                failure_reason=failure_reason,
            )
            results.append(result)

            if not is_valid:
                logger.warning(
                    f"Relationship {relationship_index} INVALID: {relationship.kind} - {failure_reason}"
                )
            else:
                logger.debug(
                    f"Relationship {relationship_index} VALID: {relationship.kind}"
                )

        return results

    def _plan_primary_key_operations(
        self, relationship: PrimaryKeyRelationship
    ) -> list[AnyTask]:
        """Plan existence, not-object, not-null, and uniqueness checks for a PK candidate."""
        operations: list[AnyTask] = []

        for column in relationship.columns:
            operations.append(
                ColumnExistsTask(
                    task_id=self._build_column_exists_task_id(
                        collection=relationship.table,
                        column=column,
                    ),
                    collection=relationship.table,
                    column=column,
                )
            )
            operations.append(
                ColumnNotObjectTask(
                    task_id=self._build_column_not_object_task_id(
                        collection=relationship.table,
                        column=column,
                    ),
                    collection=relationship.table,
                    column=column,
                )
            )
            operations.append(
                ColumnNotNullTask(
                    task_id=self._build_column_not_null_task_id(
                        collection=relationship.table,
                        column=column,
                    ),
                    collection=relationship.table,
                    column=column,
                )
            )

        operations.append(
            ColumnUniquenessTask(
                task_id=self._build_uniqueness_task_id(
                    collection=relationship.table,
                    columns=relationship.columns,
                ),
                collection=relationship.table,
                columns=relationship.columns,
            )
        )

        return operations

    def _plan_foreign_key_operations(
        self, relationship: ForeignKeyRelationship
    ) -> list[AnyTask]:
        """Plan inclusion, type, and parent key integrity checks for an FK candidate."""
        operations: list[AnyTask] = []

        # Check that all child columns exist
        for child_column in relationship.child_columns:
            operations.append(
                ColumnExistsTask(
                    task_id=self._build_column_exists_task_id(
                        collection=relationship.child_table,
                        column=child_column,
                    ),
                    collection=relationship.child_table,
                    column=child_column,
                )
            )

        # Check that all parent columns exist (except META_ID_SENTINEL)
        for parent_column in relationship.parent_columns:
            if parent_column == META_ID_SENTINEL:
                continue
            operations.append(
                ColumnExistsTask(
                    task_id=self._build_column_exists_task_id(
                        collection=relationship.parent_table,
                        column=parent_column,
                    ),
                    collection=relationship.parent_table,
                    column=parent_column,
                )
            )

        operations.append(
            ValueSetInclusionTask(
                task_id=self._build_inclusion_task_id(
                    child_collection=relationship.child_table,
                    child_columns=relationship.child_columns,
                    parent_collection=relationship.parent_table,
                    parent_columns=relationship.parent_columns,
                ),
                child_collection=relationship.child_table,
                child_columns=relationship.child_columns,
                parent_collection=relationship.parent_table,
                parent_columns=relationship.parent_columns,
            )
        )

        for child_column, parent_column in zip(
            relationship.child_columns,
            relationship.parent_columns,
            strict=True,
        ):
            if parent_column == META_ID_SENTINEL:
                continue
            operations.append(
                ColumnTypeCompatibilityTask(
                    task_id=self._build_type_compatibility_task_id(
                        child_collection=relationship.child_table,
                        child_column=child_column,
                        parent_collection=relationship.parent_table,
                        parent_column=parent_column,
                    ),
                    child_collection=relationship.child_table,
                    child_column=child_column,
                    parent_collection=relationship.parent_table,
                    parent_column=parent_column,
                )
            )

        if uses_meta_id(relationship.parent_columns):
            return operations

        operations.append(
            ColumnUniquenessTask(
                task_id=self._build_uniqueness_task_id(
                    collection=relationship.parent_table,
                    columns=relationship.parent_columns,
                ),
                collection=relationship.parent_table,
                columns=relationship.parent_columns,
            )
        )

        for parent_column in relationship.parent_columns:
            operations.append(
                ColumnNotNullTask(
                    task_id=self._build_column_not_null_task_id(
                        collection=relationship.parent_table,
                        column=parent_column,
                    ),
                    collection=relationship.parent_table,
                    column=parent_column,
                )
            )

        return operations

    def _plan_one_to_one_operations(
        self, relationship: InferredRelationship
    ) -> list[AnyTask]:
        """Plan FK-style validity plus child-side uniqueness for inferred 1-1 (`OO`)."""
        return self._plan_inferred_relationship_operations(relationship)

    def _plan_one_to_many_operations(
        self, relationship: InferredRelationship
    ) -> list[AnyTask]:
        """Plan FK-style validity plus a child-side multiplicity probe for inferred 1-many (`OM`)."""
        return self._plan_inferred_relationship_operations(relationship)

    def _plan_inferred_relationship_operations(
        self, relationship: InferredRelationship
    ) -> list[AnyTask]:
        operations = self._plan_foreign_key_operations(
            self._foreign_key_relationship_from_inferred(relationship)
        )
        operations.append(
            ColumnUniquenessTask(
                task_id=self._build_uniqueness_task_id(
                    collection=relationship.foreign_key_table,
                    columns=relationship.from_columns,
                ),
                collection=relationship.foreign_key_table,
                columns=relationship.from_columns,
            )
        )
        return operations

    def _operation_to_query(self, operation: AnyTask) -> str:
        """Translate one task model into a single SQL++ statement."""
        if isinstance(operation, ColumnExistsTask):
            return self._column_exists_query(operation)

        if isinstance(operation, ColumnNotObjectTask):
            return self._column_not_object_query(operation)

        if isinstance(operation, ColumnNotNullTask):
            return self._column_not_null_query(operation)

        if isinstance(operation, ColumnUniquenessTask):
            return self._column_uniqueness_query(operation)

        if isinstance(operation, ValueSetInclusionTask):
            return self._value_set_inclusion_query(operation)

        if isinstance(operation, ColumnTypeCompatibilityTask):
            return self._type_compatibility_query(operation)

        raise TypeError(f"Unsupported operation type: {type(operation).__name__}")

    def _column_not_object_query(self, task: ColumnNotObjectTask) -> str:
        """Count rows where the target column resolves to object/array."""
        keyspace = self._keyspace_expression(task.collection)
        column_path = self._parse_path("document", task.column)
        scan_limit = self._query_limit_for_collection(task.collection, (task.column,))
        from_clause = self._build_from_clause(
            keyspace,
            "document",
            column_path,
            limit_rows=scan_limit,
        )
        column_expression = column_path.column_ref
        return (
            "SELECT COUNT(*) AS nested_count "
            f"{from_clause} "
            f"WHERE IS_OBJECT({column_expression}) OR IS_ARRAY({column_expression});"
        )

    def _column_exists_query(self, task: ColumnExistsTask) -> str:
        """Count rows where the target column is present and non-null.

        Returns a count of documents where the column exists with a non-null value.
        If this count is 0, the column doesn't exist in any document.
        """
        keyspace = self._keyspace_expression(task.collection)
        column_path = self._parse_path("document", task.column)
        scan_limit = self._query_limit_for_collection(task.collection, (task.column,))
        from_clause = self._build_from_clause(
            keyspace,
            "document",
            column_path,
            limit_rows=scan_limit,
        )
        column_expression = column_path.column_ref
        return (
            "SELECT COUNT(*) AS exists_count "
            f"{from_clause} "
            f"WHERE {column_expression} IS NOT NULL AND {column_expression} IS NOT MISSING;"
        )

    def _column_not_null_query(self, task: ColumnNotNullTask) -> str:
        """Count rows where the target column is NULL or MISSING."""
        keyspace = self._keyspace_expression(task.collection)
        column_path = self._parse_path("document", task.column)
        scan_limit = self._query_limit_for_collection(task.collection, (task.column,))
        from_clause = self._build_from_clause(
            keyspace,
            "document",
            column_path,
            limit_rows=scan_limit,
        )
        column_expression = column_path.column_ref
        return (
            "SELECT COUNT(*) AS null_count "
            f"{from_clause} "
            f"WHERE {column_expression} IS NULL OR {column_expression} IS MISSING;"
        )

    def _column_uniqueness_query(self, task: ColumnUniquenessTask) -> str:
        """Count duplicate groups for an ordered tuple of columns."""
        keyspace = self._keyspace_expression(task.collection)
        column_paths = [
            self._parse_path("document", column_name) for column_name in task.columns
        ]
        scan_limit = self._query_limit_for_collection(task.collection, task.columns)
        from_clause = self._build_from_clause(
            keyspace,
            "document",
            *column_paths,
            limit_rows=scan_limit,
        )
        aliased_columns = [
            (column_path.column_ref, f"group_col_{index}")
            for index, column_path in enumerate(column_paths)
        ]
        projected_columns = ", ".join(
            f"{column_expression} AS {column_alias}"
            for column_expression, column_alias in aliased_columns
        )
        group_by_expressions = ", ".join(
            column_expression for column_expression, _ in aliased_columns
        )
        return (
            "SELECT COUNT(*) AS duplicate_groups "
            "FROM ("
            f"SELECT {projected_columns}, COUNT(*) AS grouped_count "
            f"{from_clause} "
            f"GROUP BY {group_by_expressions} "
            "HAVING COUNT(*) > 1"
            ") AS duplicate_rows;"
        )

    def _value_set_inclusion_query(self, task: ValueSetInclusionTask) -> str:
        """Count child rows whose FK tuple has no corresponding parent tuple."""
        child_keyspace = self._keyspace_expression(task.child_collection)
        parent_keyspace = self._keyspace_expression(task.parent_collection)

        child_paths = [
            self._parse_path("child_row", child_column)
            for child_column in task.child_columns
        ]
        parent_paths = [
            self._parse_path("parent_row", parent_column)
            for parent_column in task.parent_columns
        ]

        child_limit = self._query_limit_for_collection(
            task.child_collection, task.child_columns
        )
        parent_limit = self._query_limit_for_collection(
            task.parent_collection, task.parent_columns
        )

        child_from_clause = self._build_from_clause(
            child_keyspace,
            "child_row",
            *child_paths,
            limit_rows=child_limit,
        )
        parent_from_clause = self._build_from_clause(
            parent_keyspace,
            "parent_row",
            *parent_paths,
            limit_rows=parent_limit,
        )

        child_presence_filters = " AND ".join(
            (
                f"{child_path.column_ref} IS NOT NULL"
                f" AND {child_path.column_ref} IS NOT MISSING"
            )
            for child_path in child_paths
        )

        join_conditions = " AND ".join(
            (f"{parent_path.column_ref} = {child_path.column_ref}")
            for child_path, parent_path in zip(
                child_paths,
                parent_paths,
                strict=True,
            )
        )

        if not child_presence_filters:
            child_presence_filters = "TRUE"
        if not join_conditions:
            join_conditions = "TRUE"

        return (
            "SELECT COUNT(*) AS missing_count "
            f"{child_from_clause} "
            f"WHERE {child_presence_filters} "
            "AND NOT EXISTS ("
            "SELECT 1 "
            f"{parent_from_clause} "
            f"WHERE {join_conditions}"
            ");"
        )

    def _type_compatibility_query(self, task: ColumnTypeCompatibilityTask) -> str:
        """Count child rows whose type is incompatible with the parent column types.

        String/number values are treated as compatible when the parent side contains
        at least one non-null string or number value.
        """
        child_keyspace = self._keyspace_expression(task.child_collection)
        parent_keyspace = self._keyspace_expression(task.parent_collection)

        child_path = self._parse_path("child_row", task.child_column)
        parent_path = self._parse_path("parent_row", task.parent_column)

        child_limit = self._query_limit_for_collection(
            task.child_collection,
            (task.child_column,),
        )
        parent_limit = self._query_limit_for_collection(
            task.parent_collection,
            (task.parent_column,),
        )

        child_from_clause = self._build_from_clause(
            child_keyspace,
            "child_row",
            child_path,
            limit_rows=child_limit,
        )
        parent_from_clause = self._build_from_clause(
            parent_keyspace,
            "parent_row",
            parent_path,
            limit_rows=parent_limit,
        )

        child_expression = child_path.column_ref
        parent_expression = parent_path.column_ref

        return (
            "SELECT COUNT(*) AS type_mismatch_count "
            f"{child_from_clause} "
            f"WHERE {child_expression} IS NOT NULL AND {child_expression} IS NOT MISSING "
            "AND NOT ("
            f'TYPE({child_expression}) IN ["string", "number"] '
            "AND EXISTS ("
            "SELECT 1 "
            f"{parent_from_clause} "
            f"WHERE {parent_expression} IS NOT NULL AND {parent_expression} IS NOT MISSING "
            f'AND TYPE({parent_expression}) IN ["string", "number"]'
            ")"
            ") "
            f"AND TYPE({child_expression}) NOT IN ("
            "SELECT DISTINCT RAW TYPE(" + parent_expression + ") "
            f"{parent_from_clause} "
            f"WHERE {parent_expression} IS NOT NULL AND {parent_expression} IS NOT MISSING"
            ");"
        )

    def _evaluate_relationship_with_reason(  # noqa: PLR0911, PLR0912
        self,
        relationship: AnyRelationship,
        operations: list[AnyTask],
        task_outputs: dict[str, Any],
    ) -> tuple[bool, str | None]:
        if isinstance(relationship, PrimaryKeyRelationship):
            # First check that all columns exist
            exists_task_ids = [
                operation.task_id
                for operation in operations
                if isinstance(operation, ColumnExistsTask)
            ]
            for task_id, column in zip(
                exists_task_ids, relationship.columns, strict=True
            ):
                exists_count, error_reason = self._get_task_count(
                    task_outputs,
                    task_id,
                    "exists_count",
                )
                if exists_count is None:
                    return False, (
                        "pk_check_unavailable: "
                        f"could not read sampled evidence for {relationship.table}.{column}"
                        f" ({error_reason})"
                    )
                if exists_count == 0:
                    return False, (
                        "pk_column_not_observed_in_sample: "
                        f"{relationship.table}.{column} had no non-null values in sampled rows"
                    )

            nested_task_ids = [
                operation.task_id
                for operation in operations
                if isinstance(operation, ColumnNotObjectTask)
            ]
            for task_id, column in zip(
                nested_task_ids, relationship.columns, strict=True
            ):
                nested_count, error_reason = self._get_task_count(
                    task_outputs,
                    task_id,
                    "nested_count",
                )
                if nested_count is None:
                    return False, (
                        "pk_check_unavailable: "
                        f"could not read nested-value check for {relationship.table}.{column}"
                        f" ({error_reason})"
                    )
                if nested_count > 0:
                    return False, (
                        "pk_column_contains_nested_values: "
                        f"{relationship.table}.{column} has {nested_count} sampled row(s) "
                        "with object/array values"
                    )

            uniqueness_task_ids = [
                operation.task_id
                for operation in operations
                if isinstance(operation, ColumnUniquenessTask)
            ]
            for task_id in uniqueness_task_ids:
                duplicate_groups, error_reason = self._get_task_count(
                    task_outputs,
                    task_id,
                    "duplicate_groups",
                )
                if duplicate_groups is None:
                    return False, (
                        "pk_check_unavailable: "
                        f"could not read uniqueness check for {relationship.table}{relationship.columns}"
                        f" ({error_reason})"
                    )
                if duplicate_groups > 0:
                    return False, (
                        "pk_key_not_unique: "
                        f"{relationship.table}{relationship.columns} has {duplicate_groups} duplicate group(s)"
                    )

            not_null_task_ids = [
                operation.task_id
                for operation in operations
                if isinstance(operation, ColumnNotNullTask)
            ]
            for task_id, column in zip(
                not_null_task_ids, relationship.columns, strict=True
            ):
                null_count, error_reason = self._get_task_count(
                    task_outputs,
                    task_id,
                    "null_count",
                )
                if null_count is None:
                    return False, (
                        "pk_check_unavailable: "
                        f"could not read null check for {relationship.table}.{column}"
                        f" ({error_reason})"
                    )
                if null_count > 0:
                    return False, (
                        "pk_column_contains_null_or_missing: "
                        f"{relationship.table}.{column} has {null_count} sampled row(s) "
                        "with NULL/MISSING values"
                    )

            return True, None

        if isinstance(relationship, ForeignKeyRelationship):
            return self._evaluate_foreign_key_relationship_with_reason(
                relationship,
                task_outputs,
            )

        if relationship.kind == "OO":
            foreign_key_relationship = self._foreign_key_relationship_from_inferred(
                relationship
            )
            fk_valid, fk_failure_reason = (
                self._evaluate_foreign_key_relationship_with_reason(
                    foreign_key_relationship,
                    task_outputs,
                )
            )
            if not fk_valid:
                return False, fk_failure_reason

            child_uniqueness_task_id = self._build_uniqueness_task_id(
                collection=relationship.foreign_key_table,
                columns=relationship.from_columns,
            )
            duplicate_groups, error_reason = self._get_task_count(
                task_outputs,
                child_uniqueness_task_id,
                "duplicate_groups",
            )
            if duplicate_groups is None:
                return False, (
                    "oo_check_unavailable: "
                    f"could not read child-side uniqueness check for "
                    f"{relationship.foreign_key_table}{relationship.from_columns}"
                    f" ({error_reason})"
                )
            if duplicate_groups != 0:
                return False, (
                    "oo_child_key_not_unique: "
                    f"{relationship.foreign_key_table}{relationship.from_columns} "
                    f"has {duplicate_groups} duplicate group(s)"
                )
            return True, None

        if relationship.kind == "OM":
            foreign_key_relationship = self._foreign_key_relationship_from_inferred(
                relationship
            )
            return self._evaluate_foreign_key_relationship_with_reason(
                foreign_key_relationship,
                task_outputs,
            )

        raise ValueError(
            f"Unsupported inferred relationship kind for evaluation: {relationship.kind!r}"
        )

    def _evaluate_foreign_key_relationship_with_reason(  # noqa: PLR0911, PLR0912
        self,
        relationship: ForeignKeyRelationship,
        task_outputs: dict[str, Any],
    ) -> tuple[bool, str | None]:
        # First check that all child columns exist
        for child_column in relationship.child_columns:
            child_exists_task_id = self._build_column_exists_task_id(
                collection=relationship.child_table,
                column=child_column,
            )
            exists_count, error_reason = self._get_task_count(
                task_outputs,
                child_exists_task_id,
                "exists_count",
            )
            if exists_count is None:
                return False, (
                    "fk_check_unavailable: "
                    f"could not read child column evidence for "
                    f"{relationship.child_table}.{child_column} ({error_reason})"
                )
            if exists_count == 0:
                return False, (
                    "fk_child_column_not_observed_in_sample: "
                    f"{relationship.child_table}.{child_column} had no non-null values in sampled rows"
                )

        # Check that all parent columns exist (except META_ID_SENTINEL)
        for parent_column in relationship.parent_columns:
            if parent_column == META_ID_SENTINEL:
                continue
            parent_exists_task_id = self._build_column_exists_task_id(
                collection=relationship.parent_table,
                column=parent_column,
            )
            exists_count, error_reason = self._get_task_count(
                task_outputs,
                parent_exists_task_id,
                "exists_count",
            )
            if exists_count is None:
                return False, (
                    "fk_check_unavailable: "
                    f"could not read parent column evidence for "
                    f"{relationship.parent_table}.{parent_column} ({error_reason})"
                )
            if exists_count == 0:
                return False, (
                    "fk_parent_column_not_observed_in_sample: "
                    f"{relationship.parent_table}.{parent_column} had no non-null values in sampled rows"
                )

        inclusion_task_id = self._build_inclusion_task_id(
            child_collection=relationship.child_table,
            child_columns=relationship.child_columns,
            parent_collection=relationship.parent_table,
            parent_columns=relationship.parent_columns,
        )
        missing_count, error_reason = self._get_task_count(
            task_outputs,
            inclusion_task_id,
            "missing_count",
        )
        if missing_count is None:
            return False, (
                "fk_check_unavailable: "
                f"could not read referential inclusion check for "
                f"{relationship.child_table}{relationship.child_columns} -> "
                f"{relationship.parent_table}{relationship.parent_columns} ({error_reason})"
            )
        if missing_count > 0:
            return False, (
                "fk_referential_inclusion_failed: "
                f"{relationship.child_table}{relationship.child_columns} -> "
                f"{relationship.parent_table}{relationship.parent_columns} has "
                f"{missing_count} sampled child row(s) with no matching parent"
            )

        for child_column, parent_column in zip(
            relationship.child_columns,
            relationship.parent_columns,
            strict=True,
        ):
            if parent_column == META_ID_SENTINEL:
                continue

            type_check_task_id = self._build_type_compatibility_task_id(
                child_collection=relationship.child_table,
                child_column=child_column,
                parent_collection=relationship.parent_table,
                parent_column=parent_column,
            )
            mismatch_count, error_reason = self._get_task_count(
                task_outputs,
                type_check_task_id,
                "type_mismatch_count",
            )
            if mismatch_count is None:
                return False, (
                    "fk_check_unavailable: "
                    f"could not read type compatibility check for "
                    f"{relationship.child_table}.{child_column} -> "
                    f"{relationship.parent_table}.{parent_column} ({error_reason})"
                )
            if mismatch_count > 0:
                return False, (
                    "fk_type_mismatch: "
                    f"{relationship.child_table}.{child_column} -> "
                    f"{relationship.parent_table}.{parent_column} has "
                    f"{mismatch_count} incompatible sampled value(s)"
                )

        if uses_meta_id(relationship.parent_columns):
            return True, None

        parent_uniqueness_task_id = self._build_uniqueness_task_id(
            collection=relationship.parent_table,
            columns=relationship.parent_columns,
        )
        duplicate_groups, error_reason = self._get_task_count(
            task_outputs,
            parent_uniqueness_task_id,
            "duplicate_groups",
        )
        if duplicate_groups is None:
            return False, (
                "fk_check_unavailable: "
                f"could not read parent uniqueness check for "
                f"{relationship.parent_table}{relationship.parent_columns} ({error_reason})"
            )
        if duplicate_groups > 0:
            return False, (
                "fk_parent_key_not_unique: "
                f"{relationship.parent_table}{relationship.parent_columns} has "
                f"{duplicate_groups} duplicate group(s)"
            )

        for parent_column in relationship.parent_columns:
            parent_not_null_task_id = self._build_column_not_null_task_id(
                collection=relationship.parent_table,
                column=parent_column,
            )
            null_count, error_reason = self._get_task_count(
                task_outputs,
                parent_not_null_task_id,
                "null_count",
            )
            if null_count is None:
                return False, (
                    "fk_check_unavailable: "
                    f"could not read parent null check for "
                    f"{relationship.parent_table}.{parent_column} ({error_reason})"
                )
            if null_count > 0:
                return False, (
                    "fk_parent_key_contains_null_or_missing: "
                    f"{relationship.parent_table}.{parent_column} has {null_count} sampled row(s) "
                    "with NULL/MISSING values"
                )

        return True, None

    def _get_task_count(  # noqa: PLR0911
        self,
        task_outputs: dict[str, Any],
        task_id: str,
        count_key: str,
    ) -> tuple[int | None, str | None]:
        output_row = task_outputs.get(task_id)
        if output_row is None:
            return None, f"task output missing for task_id={task_id!r}"

        if not isinstance(output_row, dict):
            return (
                None,
                f"unexpected task output type={type(output_row).__name__} for task_id={task_id!r}",
            )

        if "error" in output_row:
            return (
                None,
                f"query execution failed for task_id={task_id!r}: {output_row['error']}",
            )

        if count_key not in output_row:
            return None, f"count key {count_key!r} not present for task_id={task_id!r}"

        raw_value = output_row.get(count_key)
        if raw_value is None:
            return None, f"count value {count_key!r} is NULL for task_id={task_id!r}"

        try:
            return int(raw_value), None
        except (TypeError, ValueError):
            return (
                None,
                f"count value {count_key!r} not int-castable for task_id={task_id!r}",
            )

    def _foreign_key_relationship_from_inferred(
        self,
        relationship: InferredRelationship,
    ) -> ForeignKeyRelationship:
        return ForeignKeyRelationship(
            child_table=relationship.foreign_key_table,
            child_columns=relationship.from_columns,
            parent_table=self._inferred_parent_table(relationship),
            parent_columns=relationship.to_columns,
        )

    def _inferred_parent_table(self, relationship: InferredRelationship) -> str:
        if (
            relationship.table1 == relationship.foreign_key_table
            and relationship.table2 != relationship.foreign_key_table
        ):
            return relationship.table2

        if (
            relationship.table2 == relationship.foreign_key_table
            and relationship.table1 != relationship.foreign_key_table
        ):
            return relationship.table1

        if relationship.table1 == relationship.table2:
            return relationship.table1

        raise ValueError(
            "Could not determine inferred parent table for relationship: "
            f"{relationship!r}"
        )

    def _build_column_exists_task_id(self, *, collection: str, column: str) -> str:
        return f"column_exists__{self._normalize_name(collection)}__{self._normalize_name(column)}"

    def _build_column_not_object_task_id(self, *, collection: str, column: str) -> str:
        return f"column_not_object__{self._normalize_name(collection)}__{self._normalize_name(column)}"

    def _build_column_not_null_task_id(self, *, collection: str, column: str) -> str:
        return f"column_not_null__{self._normalize_name(collection)}__{self._normalize_name(column)}"

    def _build_uniqueness_task_id(
        self, *, collection: str, columns: tuple[str, ...]
    ) -> str:
        normalized_columns = "__".join(
            self._normalize_name(column) for column in columns
        )
        return f"column_uniqueness__{self._normalize_name(collection)}__{normalized_columns}"

    def _build_inclusion_task_id(
        self,
        *,
        child_collection: str,
        child_columns: tuple[str, ...],
        parent_collection: str,
        parent_columns: tuple[str, ...],
    ) -> str:
        normalized_child_columns = "__".join(
            self._normalize_name(column) for column in child_columns
        )
        normalized_parent_columns = "__".join(
            self._normalize_name(column) for column in parent_columns
        )
        return (
            "value_set_inclusion"
            f"__{self._normalize_name(child_collection)}"
            f"__{normalized_child_columns}"
            f"__{self._normalize_name(parent_collection)}"
            f"__{normalized_parent_columns}"
        )

    def _build_type_compatibility_task_id(
        self,
        *,
        child_collection: str,
        child_column: str,
        parent_collection: str,
        parent_column: str,
    ) -> str:
        return (
            "column_type_compatibility"
            f"__{self._normalize_name(child_collection)}"
            f"__{self._normalize_name(child_column)}"
            f"__{self._normalize_name(parent_collection)}"
            f"__{self._normalize_name(parent_column)}"
        )

    def _keyspace_expression(self, collection_name: str) -> str:
        """Render `bucket.scope.collection` with escaped SQL++ identifiers.

        collection_name may be either:
        - unqualified collection token (e.g. `users`) resolved via keyspace_map
        - qualified `scope.collection` token
        """
        scope_name, resolved_collection = self._resolve_collection_keyspace(
            collection_name
        )
        return (
            f"{self._quote_identifier(self.bucket_name)}"
            f".{self._quote_identifier(scope_name)}"
            f".{self._quote_identifier(resolved_collection)}"
        )

    def _resolve_collection_keyspace(self, collection_name: str) -> tuple[str, str]:
        raw_name = collection_name.strip()
        if "." in raw_name:
            scope_name, resolved_collection = raw_name.split(".", maxsplit=1)
            scope_name = scope_name.strip()
            resolved_collection = resolved_collection.strip()
            if not scope_name or not resolved_collection:
                raise ValueError(
                    f"Invalid qualified collection identifier: {collection_name!r}"
                )
            return scope_name, resolved_collection

        normalized_name = self._normalize_name(raw_name)
        mapped_keyspace = self.keyspace_map.get(normalized_name)
        if mapped_keyspace is None:
            raise ValueError(
                "Collection keyspace not found in bucket keyspace map: "
                f"{collection_name!r}. Provide `scope.collection` for ambiguous names."
            )

        scope_name, resolved_collection = mapped_keyspace.split(".", maxsplit=1)
        return scope_name, resolved_collection

    def _parse_path(self, alias: str, column_name: str) -> ParsedPath:
        """Parse one column path into a SQL++ column expression and UNNEST clauses."""
        if column_name == META_ID_SENTINEL:
            return ParsedPath(unnest_clauses=(), column_ref=f"META({alias}).id")

        return parse_column_path(alias, column_name, quote_fn=self._quote_identifier)

    def _build_from_clause(
        self,
        keyspace: str,
        root_alias: str,
        *paths: ParsedPath,
        limit_rows: int | None = None,
    ) -> str:
        """Build a FROM clause and append unique UNNESTs required by parsed paths."""
        if limit_rows is None:
            from_clause = f"FROM {keyspace} AS {root_alias}"
        else:
            from_clause = (
                f"FROM (SELECT * FROM {keyspace} LIMIT {limit_rows}) AS {root_alias}"
            )

        unique_unnests: list[str] = []
        seen_unnests: set[str] = set()
        for path in paths:
            for unnest_clause in path.unnest_clauses:
                if unnest_clause in seen_unnests:
                    continue
                seen_unnests.add(unnest_clause)
                unique_unnests.append(unnest_clause)

        if unique_unnests:
            from_clause = f"{from_clause} {' '.join(unique_unnests)}"
        return from_clause

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        safe_identifier = identifier.replace("`", "``")
        return f"`{safe_identifier}`"

    @staticmethod
    def _normalize_name(name: str) -> str:
        return name.strip().lower()

    def _query_limit_for_collection(
        self, collection: str, columns: tuple[str, ...]
    ) -> int | None:
        if self._has_covering_index(collection, columns):
            return None
        return self.MAX_UNINDEXED_SCAN_ROWS

    def _has_covering_index(self, collection: str, columns: tuple[str, ...]) -> bool:
        required_columns = {
            self._normalize_index_token(column)
            for column in columns
            if self._normalize_index_token(column)
        }
        if not required_columns:
            return True

        normalized_collection = self._normalize_name(collection)
        index_keys = self.index_map.get(normalized_collection, [])
        for index_key in index_keys:
            if not index_key:
                return True

            indexed_columns = {
                self._normalize_index_token(index_component)
                for index_component in index_key
                if self._normalize_index_token(index_component)
            }
            if required_columns.issubset(indexed_columns):
                return True

        return False

    @staticmethod
    def _normalize_index_token(token: str) -> str:
        normalized = token.strip().lower()
        normalized = normalized.replace("`", "")
        normalized = normalized.replace("[]", "")
        normalized = normalized.replace("(", "")
        normalized = normalized.replace(")", "")
        normalized = normalized.replace(" ", "")
        return normalized
