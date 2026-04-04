"""Data-backed verifier for PK/FK/OO/OM relationships."""

from __future__ import annotations

import hashlib
import time
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
    PrimaryKeyAlternativeRelationship,
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
    MetaIdReferenceExistsTask,
    ValueSetInclusionTask,
)

__all__ = ["RelationshipVerifier", "VerificationResult"]


@dataclass(frozen=True, slots=True)
class VerificationResult:
    """Result of verifying one relationship candidate."""

    relationship: AnyRelationship
    is_valid: bool
    is_unable_to_verify: bool = False
    failure_reason: str | None = None


class RelationshipVerifier:
    """Plan, dedupe, execute, and evaluate data-backed relationship checks."""

    MAX_UNINDEXED_SCAN_ROWS = 10_000
    META_ID_TIMEOUT_SAMPLE_SIZE = 500
    META_ID_TIMEOUT_SAMPLE_SEED = 20260404
    VALUE_SET_TIMEOUT_SAMPLE_SIZE = 500
    VALUE_SET_TIMEOUT_SAMPLE_SEED = 20260402
    SMALL_COLLECTION_DOC_THRESHOLD = 30_000
    _UNAVAILABLE_QUERY_PREFIX = "__UNAVAILABLE__::"

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
        self._collection_doc_count_cache: dict[str, int | None] = {}
        self._latest_operations: dict[str, AnyTask] = {}
        self._sdk_operation_logs: list[dict[str, Any]] = []

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

    def _relationship_label(self, relationship: AnyRelationship) -> str:
        """Return a short relationship kind label for logging."""
        if isinstance(relationship, PrimaryKeyRelationship):
            return "PK"

        if isinstance(relationship, PrimaryKeyAlternativeRelationship):
            return "PKA"

        if isinstance(relationship, ForeignKeyRelationship):
            return "FK"

        return relationship.kind

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
                f"Relationship {relationship_index} ({self._relationship_label(relationship)}): "
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

        if isinstance(relationship, PrimaryKeyAlternativeRelationship):
            return self._plan_primary_key_alternative_operations(relationship)

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

        queries: dict[str, str] = {}
        self._latest_operations = dict(operations)
        for task_id, operation in operations.items():
            unavailable_reason = self._task_unavailable_reason(operation)
            if unavailable_reason is not None:
                queries[task_id] = (
                    f"{self._UNAVAILABLE_QUERY_PREFIX}{unavailable_reason}"
                )
                continue
            queries[task_id] = self._operation_to_query(operation)

        logger.info(f"Generated {len(queries)} SQL++ queries")
        return queries

    def run_task_queries(self, task_queries: dict[str, str]) -> dict[str, Any]:
        """Run SQL++ task queries and return task-id keyed outputs."""
        logger = get_verifier_logger()
        logger.info("Stage 4: Executing SQL++ queries")

        task_outputs: dict[str, Any] = {}
        for task_id, query in task_queries.items():
            task_output, execution_mode, error_reason, _ = self.execute_task_query(
                task_id,
                query,
            )
            if error_reason is not None:
                logger.warning(
                    "Task %s failed during %s execution: %s",
                    task_id,
                    execution_mode,
                    error_reason,
                )
            task_outputs[task_id] = task_output

        logger.info(f"Query execution complete: {len(task_outputs)} results")
        return task_outputs

    def execute_task_query(  # noqa: PLR0911
        self, task_id: str, query: str
    ) -> tuple[Any, str, str | None, dict[str, Any] | None]:
        """Execute one task using SQL++ or SDK fallback.

        Returns (task_output, execution_mode, error_reason, execution_metadata).
        """
        if query.startswith(self._UNAVAILABLE_QUERY_PREFIX):
            unavailable_reason = query.replace(self._UNAVAILABLE_QUERY_PREFIX, "", 1)
            operation = self._latest_operations.get(task_id)
            # Policy: ValueSet/Type checks use SDK fallback only when SQL++ was skipped
            # due to missing covering indexes. Indexed SQL++ query failures are not
            # re-routed to SDK and remain unable-to-verify.
            if isinstance(
                operation, ValueSetInclusionTask
            ) and unavailable_reason.startswith(
                "inclusion_check_requires_covering_index_on_either_side"
            ):
                started_ms = time.perf_counter() * 1000
                try:
                    fallback_output = self._sdk_fallback_value_set_inclusion(operation)
                    elapsed_ms = time.perf_counter() * 1000 - started_ms
                    self._sdk_operation_logs.append(
                        {
                            "task_id": task_id,
                            "operation_type": "value_set_inclusion",
                            "elapsed_ms": elapsed_ms,
                            "success": True,
                            "fallback_reason": unavailable_reason,
                            "error": None,
                        }
                    )
                    return fallback_output, "sdk_fallback", None, None
                except Exception as error:
                    elapsed_ms = time.perf_counter() * 1000 - started_ms
                    self._sdk_operation_logs.append(
                        {
                            "task_id": task_id,
                            "operation_type": "value_set_inclusion",
                            "elapsed_ms": elapsed_ms,
                            "success": False,
                            "fallback_reason": unavailable_reason,
                            "error": str(error),
                        }
                    )
                    error_text = str(error)
                    if self._is_timeout_error(error_text):
                        (
                            fallback_output,
                            fallback_error,
                            fallback_metadata,
                        ) = self._sampled_value_set_fallback(
                            operation,
                            fallback_stage="sdk_timeout",
                            first_error=error_text,
                        )
                        return (
                            fallback_output,
                            "sample_fallback",
                            fallback_error,
                            fallback_metadata,
                        )
                    return {"error": error_text}, "sdk_fallback", error_text, None

            if isinstance(
                operation, ColumnTypeCompatibilityTask
            ) and unavailable_reason.startswith(
                "type_check_requires_covering_index_on_either_side"
            ):
                started_ms = time.perf_counter() * 1000
                try:
                    fallback_output = self._sdk_fallback_type_compatibility(operation)
                    elapsed_ms = time.perf_counter() * 1000 - started_ms
                    self._sdk_operation_logs.append(
                        {
                            "task_id": task_id,
                            "operation_type": "column_type_compatibility",
                            "elapsed_ms": elapsed_ms,
                            "success": True,
                            "fallback_reason": unavailable_reason,
                            "error": None,
                        }
                    )
                    return fallback_output, "sdk_fallback", None, None
                except Exception as error:
                    elapsed_ms = time.perf_counter() * 1000 - started_ms
                    self._sdk_operation_logs.append(
                        {
                            "task_id": task_id,
                            "operation_type": "column_type_compatibility",
                            "elapsed_ms": elapsed_ms,
                            "success": False,
                            "fallback_reason": unavailable_reason,
                            "error": str(error),
                        }
                    )
                    return {"error": str(error)}, "sdk_fallback", str(error), None

            if isinstance(
                operation, MetaIdReferenceExistsTask
            ) and unavailable_reason.startswith("meta_id_check_requires_primary_index"):
                started_ms = time.perf_counter() * 1000
                try:
                    fallback_output = self._sdk_fallback_meta_id_reference_exists(
                        operation
                    )
                    elapsed_ms = time.perf_counter() * 1000 - started_ms
                    self._sdk_operation_logs.append(
                        {
                            "task_id": task_id,
                            "operation_type": "meta_id_reference_exists",
                            "elapsed_ms": elapsed_ms,
                            "success": True,
                            "fallback_reason": unavailable_reason,
                            "error": None,
                        }
                    )
                    return fallback_output, "sdk_fallback", None, None
                except Exception as error:
                    elapsed_ms = time.perf_counter() * 1000 - started_ms
                    self._sdk_operation_logs.append(
                        {
                            "task_id": task_id,
                            "operation_type": "meta_id_reference_exists",
                            "elapsed_ms": elapsed_ms,
                            "success": False,
                            "fallback_reason": unavailable_reason,
                            "error": str(error),
                        }
                    )
                    error_text = str(error)
                    if self._is_timeout_error(error_text):
                        (
                            fallback_output,
                            fallback_error,
                            fallback_metadata,
                        ) = self._sampled_meta_id_reference_fallback(
                            operation,
                            fallback_stage="sdk_timeout",
                            first_error=error_text,
                        )
                        return (
                            fallback_output,
                            "sample_fallback",
                            fallback_error,
                            fallback_metadata,
                        )
                    return {"error": error_text}, "sdk_fallback", error_text, None

            return {"unavailable": unavailable_reason}, "unavailable", None, None

        try:
            rows = self.cb.run_query(query)
            return (rows[0] if rows else None), "query", None, None
        except Exception as error:
            error_text = str(error)
            operation = self._latest_operations.get(task_id)
            if isinstance(operation, ValueSetInclusionTask) and self._is_timeout_error(
                error_text
            ):
                fallback_output, fallback_error, fallback_metadata = (
                    self._sampled_value_set_fallback(
                        operation,
                        fallback_stage="query_timeout",
                        first_error=error_text,
                    )
                )
                return (
                    fallback_output,
                    "sample_fallback",
                    fallback_error,
                    fallback_metadata,
                )
            if isinstance(
                operation, MetaIdReferenceExistsTask
            ) and self._is_timeout_error(error_text):
                fallback_output, fallback_error, fallback_metadata = (
                    self._sampled_meta_id_reference_fallback(
                        operation,
                        fallback_stage="query_timeout",
                        first_error=error_text,
                    )
                )
                return (
                    fallback_output,
                    "sample_fallback",
                    fallback_error,
                    fallback_metadata,
                )
            return {"error": error_text}, "query", error_text, None

    def get_sdk_operation_metrics(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._sdk_operation_logs]

    @staticmethod
    def _is_timeout_error(error_text: str) -> bool:
        normalized = error_text.strip().lower()
        timeout_markers = (
            "timeout",
            "timed out",
            "unambiguous_timeout",
            "timeout 29.5s exceeded",
            "seq_scan.timeout",
            "datastore.seq_scan.timeout",
            'code":1080',
        )
        return any(marker in normalized for marker in timeout_markers)

    def _sampled_value_set_fallback(
        self,
        task: ValueSetInclusionTask,
        *,
        fallback_stage: str,
        first_error: str,
    ) -> tuple[Any, str | None, dict[str, Any]]:
        sample_seed = self._stable_sample_seed(
            self.VALUE_SET_TIMEOUT_SAMPLE_SEED,
            task.task_id,
        )
        sampled_child_rows = self._sample_collection_rows_for_columns(
            task.child_collection,
            task.child_columns,
            sample_size=self.VALUE_SET_TIMEOUT_SAMPLE_SIZE,
            seed=sample_seed,
        )
        metadata: dict[str, Any] = {
            "fallback_stage": fallback_stage,
            "first_error": first_error,
            "sample_size": self.VALUE_SET_TIMEOUT_SAMPLE_SIZE,
            "sampled_query": "sdk_sampling(value_set_inclusion)",
        }
        try:
            output = self._sdk_fallback_value_set_inclusion(
                task,
                sampled_child_rows=sampled_child_rows,
            )
            return output, None, metadata
        except Exception as sampled_error:
            return {"error": str(sampled_error)}, str(sampled_error), metadata

    def _sampled_meta_id_reference_fallback(
        self,
        task: MetaIdReferenceExistsTask,
        *,
        fallback_stage: str,
        first_error: str,
    ) -> tuple[Any, str | None, dict[str, Any]]:
        sample_seed = self._stable_sample_seed(
            self.META_ID_TIMEOUT_SAMPLE_SEED,
            task.task_id,
        )
        sampled_child_rows = self._sample_collection_rows_for_columns(
            task.child_collection,
            (task.child_column,),
            sample_size=self.META_ID_TIMEOUT_SAMPLE_SIZE,
            seed=sample_seed,
        )
        metadata: dict[str, Any] = {
            "fallback_stage": fallback_stage,
            "first_error": first_error,
            "sample_size": self.META_ID_TIMEOUT_SAMPLE_SIZE,
            "sampled_query": "sdk_sampling(meta_id_reference_exists)",
        }
        try:
            output = self._sdk_fallback_meta_id_reference_exists(
                task,
                sampled_child_rows=sampled_child_rows,
            )
            return output, None, metadata
        except Exception as sampled_error:
            return {"error": str(sampled_error)}, str(sampled_error), metadata

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
            is_unable_to_verify = (
                failure_reason is not None and "_check_unavailable:" in failure_reason
            )
            result = VerificationResult(
                relationship=relationship,
                is_valid=is_valid,
                is_unable_to_verify=is_unable_to_verify,
                failure_reason=failure_reason,
            )
            results.append(result)

            if not is_valid:
                logger.warning(
                    f"Relationship {relationship_index} INVALID: {self._relationship_label(relationship)} - {failure_reason}"
                )
            else:
                logger.debug(
                    f"Relationship {relationship_index} VALID: {self._relationship_label(relationship)}"
                )

        return results

    def _plan_primary_key_operations(
        self, relationship: PrimaryKeyRelationship
    ) -> list[AnyTask]:
        """PK is strict META().id; no data-scan operations are needed."""
        return []

    def _plan_primary_key_alternative_operations(
        self, relationship: PrimaryKeyAlternativeRelationship
    ) -> list[AnyTask]:
        """Plan lightweight checks for logical PK alternatives (PKA)."""
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
        return operations

    def _plan_foreign_key_operations(
        self, relationship: ForeignKeyRelationship
    ) -> list[AnyTask]:
        """Plan child/parent existence, inclusion, and type checks for an FK candidate."""
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
                operations.append(
                    MetaIdReferenceExistsTask(
                        task_id=self._build_meta_id_reference_task_id(
                            child_collection=relationship.child_table,
                            child_column=child_column,
                            parent_collection=relationship.parent_table,
                        ),
                        child_collection=relationship.child_table,
                        child_column=child_column,
                        parent_collection=relationship.parent_table,
                    )
                )
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

        return operations

    def _plan_one_to_one_operations(
        self, relationship: InferredRelationship
    ) -> list[AnyTask]:
        """Plan FK-style validity checks for inferred 1-1 (`OO`)."""
        return self._plan_inferred_relationship_operations(relationship)

    def _plan_one_to_many_operations(
        self, relationship: InferredRelationship
    ) -> list[AnyTask]:
        """Plan FK-style validity plus a child-side multiplicity probe for inferred 1-many (`OM`)."""
        return self._plan_inferred_relationship_operations(relationship)

    def _plan_inferred_relationship_operations(
        self, relationship: InferredRelationship
    ) -> list[AnyTask]:
        return self._plan_foreign_key_operations(
            self._foreign_key_relationship_from_inferred(relationship)
        )

    def _operation_to_query(self, operation: AnyTask) -> str:  # noqa: PLR0911
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

        if isinstance(operation, MetaIdReferenceExistsTask):
            return self._meta_id_reference_exists_query(operation)

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
        """Return 1 when NULL/MISSING exists in sampled rows, else 0."""
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
            "SELECT CASE WHEN EXISTS ("
            "SELECT 1 "
            f"{from_clause} "
            f"WHERE {column_expression} IS NULL OR {column_expression} IS MISSING"
            ") THEN 1 ELSE 0 END AS null_count;"
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
        """Return 1 when any sampled child row has no matching parent tuple, else 0."""
        child_limit = self._query_limit_for_collection(
            task.child_collection, task.child_columns
        )
        parent_limit = self._query_limit_for_collection(
            task.parent_collection, task.parent_columns
        )
        return self._build_value_set_inclusion_query(
            task,
            child_limit=child_limit,
            parent_limit=parent_limit,
        )

    @staticmethod
    def _stable_sample_seed(base_seed: int, task_id: str) -> int:
        hash_source = f"{base_seed}:{task_id}".encode()
        return int(hashlib.md5(hash_source).hexdigest()[:8], 16)

    def _build_value_set_inclusion_query(
        self,
        task: ValueSetInclusionTask,
        *,
        child_limit: int | None,
        parent_limit: int | None,
        child_offset: int = 0,
    ) -> str:
        """Build ValueSetInclusion query with configurable child sampling."""
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

        child_from_clause = self._build_from_clause(
            child_keyspace,
            "child_row",
            *child_paths,
            limit_rows=child_limit,
            offset_rows=child_offset,
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
            "SELECT CASE WHEN EXISTS ("
            "SELECT 1 "
            f"{child_from_clause} "
            f"WHERE {child_presence_filters} "
            "AND NOT EXISTS ("
            "SELECT 1 "
            f"{parent_from_clause} "
            f"WHERE {join_conditions}"
            ")"
            ") THEN 1 ELSE 0 END AS missing_count;"
        )

    def _meta_id_reference_exists_query(self, task: MetaIdReferenceExistsTask) -> str:
        """Return 1 when any child META-ID reference has no matching parent document."""
        child_keyspace = self._keyspace_expression(task.child_collection)
        parent_keyspace = self._keyspace_expression(task.parent_collection)

        child_path = self._parse_path("child_row", task.child_column)
        child_limit = self._query_limit_for_collection(
            task.child_collection,
            (task.child_column,),
        )
        child_from_clause = self._build_from_clause(
            child_keyspace,
            "child_row",
            child_path,
            limit_rows=child_limit,
        )
        child_expression = child_path.column_ref

        return (
            "SELECT CASE WHEN EXISTS ("
            "SELECT 1 "
            f"{child_from_clause} "
            f"WHERE {child_expression} IS NOT NULL AND {child_expression} IS NOT MISSING "
            "AND NOT EXISTS ("
            "SELECT 1 "
            f"FROM {parent_keyspace} AS parent_row "
            f"WHERE META(parent_row).id = {child_expression}"
            ")"
            ") THEN 1 ELSE 0 END AS has_missing_reference;"
        )

    def _type_compatibility_query(self, task: ColumnTypeCompatibilityTask) -> str:
        """Return 1 when any sampled child value has incompatible parent-side type, else 0.

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
            "SELECT CASE WHEN EXISTS ("
            "SELECT 1 "
            f"{child_from_clause} "
            "WHERE NOT ("
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
            ")"
            ") THEN 1 ELSE 0 END AS type_mismatch_count;"
        )

    def _evaluate_relationship_with_reason(  # noqa: PLR0911, PLR0912
        self,
        relationship: AnyRelationship,
        operations: list[AnyTask],
        task_outputs: dict[str, Any],
    ) -> tuple[bool, str | None]:
        if isinstance(relationship, PrimaryKeyRelationship):
            if not uses_meta_id(relationship.columns):
                return False, (
                    "pk_must_be_meta_id: "
                    f"{relationship.table}{relationship.columns} must use ($meta_id)"
                )

            return True, None

        if isinstance(relationship, PrimaryKeyAlternativeRelationship):
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
                        "pka_check_unavailable: "
                        f"could not read sampled evidence for {relationship.table}.{column}"
                        f" ({error_reason})"
                    )
                if exists_count == 0:
                    return False, (
                        "pka_column_not_observed_in_sample: "
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
                        "pka_check_unavailable: "
                        f"could not read nested-value check for {relationship.table}.{column}"
                        f" ({error_reason})"
                    )
                if nested_count > 0:
                    return False, (
                        "pka_column_contains_nested_values: "
                        f"{relationship.table}.{column} has {nested_count} sampled row(s) "
                        "with object/array values"
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
            return False, (
                "oo_check_unavailable: "
                "child-side uniqueness verification is disabled for performance"
            )

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
                meta_id_task_id = self._build_meta_id_reference_task_id(
                    child_collection=relationship.child_table,
                    child_column=child_column,
                    parent_collection=relationship.parent_table,
                )
                missing_reference, error_reason = self._get_task_count(
                    task_outputs,
                    meta_id_task_id,
                    "has_missing_reference",
                )
                if missing_reference is None:
                    return False, (
                        "fk_check_unavailable: "
                        f"could not read META-ID reference check for "
                        f"{relationship.child_table}.{child_column} -> "
                        f"{relationship.parent_table}.$meta_id ({error_reason})"
                    )
                if missing_reference > 0:
                    return False, (
                        "fk_meta_id_reference_missing: "
                        f"{relationship.child_table}.{child_column} references "
                        f"document id(s) not present in {relationship.parent_table}"
                    )
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

        if "unavailable" in output_row:
            return (
                None,
                f"task unavailable for task_id={task_id!r}: {output_row['unavailable']}",
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

    def _build_meta_id_reference_task_id(
        self,
        *,
        child_collection: str,
        child_column: str,
        parent_collection: str,
    ) -> str:
        return (
            "meta_id_reference_exists"
            f"__{self._normalize_name(child_collection)}"
            f"__{self._normalize_name(child_column)}"
            f"__{self._normalize_name(parent_collection)}"
        )

    def _sdk_fallback_value_set_inclusion(
        self,
        task: ValueSetInclusionTask,
        *,
        sampled_child_rows: list[tuple[Any, ...] | None] | None = None,
    ) -> dict[str, int]:
        parent_limit = self._query_limit_for_collection(
            task.parent_collection, task.parent_columns
        )

        parent_rows = self._scan_collection_rows_for_columns(
            task.parent_collection,
            task.parent_columns,
            limit_rows=parent_limit,
        )
        parent_values = {
            row_values
            for row_values in parent_rows
            if row_values is not None and len(row_values) == len(task.parent_columns)
        }

        child_rows = sampled_child_rows
        if child_rows is None:
            child_limit = self._query_limit_for_collection(
                task.child_collection, task.child_columns
            )
            child_rows = self._scan_collection_rows_for_columns(
                task.child_collection,
                task.child_columns,
                limit_rows=child_limit,
            )
        for row_values in child_rows:
            if row_values is None or len(row_values) != len(task.child_columns):
                continue
            if any(value is None for value in row_values):
                continue
            if row_values not in parent_values:
                return {"missing_count": 1}

        return {"missing_count": 0}

    def _sdk_fallback_meta_id_reference_exists(
        self,
        task: MetaIdReferenceExistsTask,
        *,
        sampled_child_rows: list[tuple[Any, ...] | None] | None = None,
    ) -> dict[str, int]:
        scope_name, parent_collection = self._resolve_collection_keyspace(
            task.parent_collection
        )
        child_rows = sampled_child_rows
        if child_rows is None:
            child_limit = self._query_limit_for_collection(
                task.child_collection,
                (task.child_column,),
            )
            child_rows = self._scan_collection_rows_for_columns(
                task.child_collection,
                (task.child_column,),
                limit_rows=child_limit,
            )

        for row_value in child_rows:
            if row_value is None or not row_value:
                continue
            referenced_id = row_value[0]
            if referenced_id is None:
                continue
            if not isinstance(referenced_id, str):
                referenced_id = str(referenced_id)
            if not self.cb.document_exists(
                bucket_name=self.bucket_name,
                scope_name=scope_name,
                collection_name=parent_collection,
                document_id=referenced_id,
            ):
                return {"has_missing_reference": 1}

        return {"has_missing_reference": 0}

    def _sdk_fallback_type_compatibility(
        self, task: ColumnTypeCompatibilityTask
    ) -> dict[str, int]:
        child_limit = self._query_limit_for_collection(
            task.child_collection,
            (task.child_column,),
        )
        parent_limit = self._query_limit_for_collection(
            task.parent_collection,
            (task.parent_column,),
        )

        parent_values = self._scan_collection_rows_for_columns(
            task.parent_collection,
            (task.parent_column,),
            limit_rows=parent_limit,
        )
        parent_types = {
            self._python_value_type_name(parent_value[0])
            for parent_value in parent_values
            if parent_value is not None and parent_value[0] is not None
        }
        has_parent_string_or_number = bool({"string", "number"} & parent_types)

        child_values = self._scan_collection_rows_for_columns(
            task.child_collection,
            (task.child_column,),
            limit_rows=child_limit,
        )
        for child_value in child_values:
            if child_value is None:
                child_type = "missing"
            else:
                child_type = self._python_value_type_name(child_value[0])

            if child_type in {"string", "number"} and has_parent_string_or_number:
                continue
            if child_type not in parent_types:
                return {"type_mismatch_count": 1}

        return {"type_mismatch_count": 0}

    def _scan_collection_rows_for_columns(
        self,
        collection: str,
        columns: tuple[str, ...],
        *,
        limit_rows: int | None,
    ) -> list[tuple[Any, ...] | None]:
        if any("[]" in column for column in columns):
            raise ValueError("SDK fallback does not support array path columns")

        scope_name, collection_name = self._resolve_collection_keyspace(collection)
        scanned_rows = self.cb.scan_collection_documents(
            bucket_name=self.bucket_name,
            scope_name=scope_name,
            collection_name=collection_name,
            limit=limit_rows,
        )

        resolved_rows: list[tuple[Any, ...] | None] = []
        for document_id, document_body in scanned_rows:
            row_values: list[Any] = []
            missing_column = False
            for column in columns:
                value, is_present = self._extract_column_value(
                    document_body,
                    column,
                    document_id=document_id,
                )
                if not is_present:
                    missing_column = True
                    break
                row_values.append(value)

            if missing_column:
                resolved_rows.append(None)
            else:
                resolved_rows.append(tuple(row_values))

        return resolved_rows

    def _sample_collection_rows_for_columns(
        self,
        collection: str,
        columns: tuple[str, ...],
        *,
        sample_size: int,
        seed: int,
    ) -> list[tuple[Any, ...] | None]:
        if any("[]" in column for column in columns):
            raise ValueError("SDK sampling does not support array path columns")

        scope_name, collection_name = self._resolve_collection_keyspace(collection)
        sampled_rows = self.cb.sample_collection_documents(
            bucket_name=self.bucket_name,
            scope_name=scope_name,
            collection_name=collection_name,
            limit=sample_size,
            seed=seed,
        )

        resolved_rows: list[tuple[Any, ...] | None] = []
        for document_id, document_body in sampled_rows:
            row_values: list[Any] = []
            missing_column = False
            for column in columns:
                value, is_present = self._extract_column_value(
                    document_body,
                    column,
                    document_id=document_id,
                )
                if not is_present:
                    missing_column = True
                    break
                row_values.append(value)

            if missing_column:
                resolved_rows.append(None)
            else:
                resolved_rows.append(tuple(row_values))

        return resolved_rows

    def _extract_column_value(
        self, document_body: Any, column: str, *, document_id: str
    ) -> tuple[Any, bool]:
        if column == META_ID_SENTINEL:
            return document_id, True

        current_value: Any = document_body
        for segment in (part.strip() for part in column.split(".")):
            if not segment:
                continue
            if segment == "[]":
                return None, False
            if not isinstance(current_value, dict):
                return None, False
            if segment not in current_value:
                return None, False
            current_value = current_value.get(segment)

        return current_value, True

    @staticmethod
    def _python_value_type_name(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int | float):
            return "number"
        if isinstance(value, str):
            return "string"
        if isinstance(value, list):
            return "array"
        return "object" if isinstance(value, dict) else "unknown"

    def _task_unavailable_reason(self, operation: AnyTask) -> str | None:  # noqa: PLR0911
        """Return a reason when task should not run due to index/doc-count policy."""
        if isinstance(operation, ValueSetInclusionTask):
            child_ok = self._has_covering_index(
                operation.child_collection, operation.child_columns
            )
            parent_ok = self._has_covering_index(
                operation.parent_collection, operation.parent_columns
            )
            if child_ok or parent_ok:
                return None
            return (
                "inclusion_check_requires_covering_index_on_either_side"
                f": {operation.child_collection}{operation.child_columns}"
                f" -> {operation.parent_collection}{operation.parent_columns}"
            )

        if isinstance(operation, ColumnTypeCompatibilityTask):
            child_ok = self._has_covering_index(
                operation.child_collection, (operation.child_column,)
            )
            parent_ok = self._has_covering_index(
                operation.parent_collection, (operation.parent_column,)
            )
            if child_ok or parent_ok:
                return None
            return (
                "type_check_requires_covering_index_on_either_side"
                f": {operation.child_collection}.{operation.child_column}"
                f" -> {operation.parent_collection}.{operation.parent_column}"
            )

        if isinstance(operation, MetaIdReferenceExistsTask):
            if self._has_primary_index(operation.parent_collection):
                return None
            return (
                "meta_id_check_requires_primary_index"
                f": {operation.child_collection}.{operation.child_column}"
                f" -> {operation.parent_collection}.$meta_id"
            )

        return None

    def _collection_is_indexed_or_small(
        self, collection: str, columns: tuple[str, ...]
    ) -> bool:
        if self._has_covering_index(collection, columns):
            return True
        document_count = self._collection_doc_count(collection)
        if document_count is None:
            return False
        return document_count <= self.SMALL_COLLECTION_DOC_THRESHOLD

    def _collection_doc_count(self, collection: str) -> int | None:
        normalized_collection = self._normalize_name(collection)
        if normalized_collection in self._collection_doc_count_cache:
            return self._collection_doc_count_cache[normalized_collection]

        logger = get_verifier_logger()
        try:
            scope_name, resolved_collection = self._resolve_collection_keyspace(
                collection
            )
            count = self.cb.get_collection_document_count(
                bucket_name=self.bucket_name,
                scope_name=scope_name,
                collection_name=resolved_collection,
            )
            self._collection_doc_count_cache[normalized_collection] = count
            return count
        except Exception as error:
            logger.warning(
                "Could not resolve document count for %s.%s: %s",
                self.bucket_name,
                collection,
                error,
            )
            self._collection_doc_count_cache[normalized_collection] = None
            return None

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
        identifier_parts = [
            part.strip() for part in raw_name.split(".") if part.strip()
        ]

        if len(identifier_parts) == 2:
            scope_name, resolved_collection = identifier_parts
            return scope_name, resolved_collection

        if len(identifier_parts) == 3:
            _, scope_name, resolved_collection = identifier_parts
            return scope_name, resolved_collection

        if len(identifier_parts) > 3:
            raise ValueError(
                f"Invalid qualified collection identifier: {collection_name!r}"
            )

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
        offset_rows: int = 0,
    ) -> str:
        """Build a FROM clause and append unique UNNESTs required by parsed paths."""
        if limit_rows is None:
            from_clause = f"FROM {keyspace} AS {root_alias}"
        else:
            offset_clause = f" OFFSET {offset_rows}" if offset_rows > 0 else ""
            from_clause = (
                "FROM ("
                f"SELECT RAW source_doc FROM {keyspace} AS source_doc LIMIT {limit_rows}{offset_clause}"
                f") AS {root_alias}"
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

    def _has_primary_index(self, collection: str) -> bool:
        normalized_collection = self._normalize_name(collection)
        index_keys = self.index_map.get(normalized_collection, [])
        return any(not index_key for index_key in index_keys)

    @staticmethod
    def _normalize_index_token(token: str) -> str:
        normalized = token.strip().lower()
        normalized = normalized.replace("`", "")
        normalized = normalized.replace("[]", "")
        normalized = normalized.replace("(", "")
        normalized = normalized.replace(")", "")
        normalized = normalized.replace(" ", "")
        return normalized
