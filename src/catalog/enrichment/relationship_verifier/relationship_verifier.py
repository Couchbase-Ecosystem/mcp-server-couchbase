"""Data-backed verifier for PK/FK/OO/OM relationships."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import chain
from typing import Any, ClassVar

from catalog.enrichment.relationship_verifier.common.relationships import (
    AnyRelationship,
    ForeignKeyRelationship,
    PrimaryKeyAlternativeRelationship,
    PrimaryKeyRelationship,
)
from catalog.enrichment.relationship_verifier.couchbase_utils.cb_utils import CB
from catalog.enrichment.relationship_verifier.logger import get_verifier_logger
from catalog.enrichment.relationship_verifier.relationships.foreign_key_relationship import (
    ForeignKeyRelationshipRule,
)
from catalog.enrichment.relationship_verifier.relationships.one_to_many_relationship import (
    OneToManyRelationshipRule,
)
from catalog.enrichment.relationship_verifier.relationships.one_to_one_relationship import (
    OneToOneRelationshipRule,
)
from catalog.enrichment.relationship_verifier.relationships.primary_key_alternative_relationship import (
    PrimaryKeyAlternativeRelationshipRule,
)
from catalog.enrichment.relationship_verifier.relationships.primary_key_relationship import (
    PrimaryKeyRelationshipRule,
)
from catalog.enrichment.relationship_verifier.tasks import AnyTask

__all__ = ["RelationshipVerifier", "VerificationResult"]


@dataclass(frozen=True, slots=True)
class VerificationResult:
    relationship: AnyRelationship
    is_valid: bool
    is_unable_to_verify: bool = False
    failure_reason: str | None = None


class RelationshipVerifier:
    MAX_UNINDEXED_SCAN_ROWS = 10_000
    META_ID_TIMEOUT_SAMPLE_SIZE = 500
    META_ID_TIMEOUT_SAMPLE_SEED = 20260404
    VALUE_SET_TIMEOUT_SAMPLE_SIZE = 500
    VALUE_SET_TIMEOUT_SAMPLE_SEED = 20260402
    _RELATIONSHIP_RULES: ClassVar[dict[str, type[Any]]] = {
        "PK": PrimaryKeyRelationshipRule,
        "PKA": PrimaryKeyAlternativeRelationshipRule,
        "FK": ForeignKeyRelationshipRule,
        "OO": OneToOneRelationshipRule,
        "OM": OneToManyRelationshipRule,
    }

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
        self._sdk_operation_logs: list[dict[str, Any]] = []

    def verify(self, relationships: list[AnyRelationship]) -> list[VerificationResult]:
        logger = get_verifier_logger()
        logger.info("Starting verification for bucket '%s'", self.bucket_name)
        logger.info("Total relationships to verify: %s", len(relationships))

        planned_operations = self.devise_operations(relationships)
        unique_operations = self.collect_unique_operations(planned_operations)
        task_outputs = self.run_task_queries(unique_operations)
        results = self.coalesce_results(
            relationships=relationships,
            planned_operations=planned_operations,
            task_outputs=task_outputs,
        )

        valid_count = sum(1 for row in results if row.is_valid)
        logger.info(
            "Verification complete: %s valid, %s invalid",
            valid_count,
            len(results) - valid_count,
        )
        return results

    def devise_operations(
        self, relationships: list[AnyRelationship]
    ) -> dict[int, list[AnyTask]]:
        logger = get_verifier_logger()
        logger.info("Stage 1: Planning operations for each relationship")

        planned_operations: dict[int, list[AnyTask]] = {}
        for relationship_index, relationship in enumerate(relationships):
            rule = self._build_relationship_rule(relationship)
            planned_operations[relationship_index] = rule.build_tasks()
            logger.debug(
                "Relationship %s (%s): %s operations planned",
                relationship_index,
                self._relationship_label(relationship),
                len(planned_operations[relationship_index]),
            )

        logger.info(
            "Total operations planned: %s",
            sum(len(ops) for ops in planned_operations.values()),
        )
        return planned_operations

    def collect_unique_operations(
        self,
        planned_operations: dict[int, list[AnyTask]],
    ) -> dict[str, AnyTask]:
        logger = get_verifier_logger()
        logger.info("Stage 2: Deduplicating operations")
        unique_operations: dict[str, AnyTask] = {}
        for operation in chain.from_iterable(planned_operations.values()):
            unique_operations.setdefault(operation.task_id, operation)
        logger.info("Unique operations after deduplication: %s", len(unique_operations))
        return unique_operations

    def run_task_queries(self, operations: dict[str, AnyTask]) -> dict[str, Any]:
        logger = get_verifier_logger()
        logger.info("Stage 3: Executing tasks")

        runtime_args: dict[str, Any] = {
            "cb": self.cb,
            "bucket_name": self.bucket_name,
            "keyspace_map": self.keyspace_map,
            "index_map": self.index_map,
            "max_unindexed_scan_rows": self.MAX_UNINDEXED_SCAN_ROWS,
            "value_set_timeout_sample_size": self.VALUE_SET_TIMEOUT_SAMPLE_SIZE,
            "value_set_timeout_sample_seed": self.VALUE_SET_TIMEOUT_SAMPLE_SEED,
            "meta_id_timeout_sample_size": self.META_ID_TIMEOUT_SAMPLE_SIZE,
            "meta_id_timeout_sample_seed": self.META_ID_TIMEOUT_SAMPLE_SEED,
            "sdk_operation_logs": self._sdk_operation_logs,
        }

        task_outputs: dict[str, Any] = {}
        for task_id, operation in operations.items():
            task_output, execution_mode, error_reason, _ = operation.run(**runtime_args)
            if error_reason is not None:
                logger.warning(
                    "Task %s failed during %s execution: %s",
                    task_id,
                    execution_mode,
                    error_reason,
                )
            task_outputs[task_id] = task_output

        logger.info("Task execution complete: %s results", len(task_outputs))
        return task_outputs

    def coalesce_results(
        self,
        *,
        relationships: list[AnyRelationship],
        planned_operations: dict[int, list[AnyTask]],
        task_outputs: dict[str, Any],
    ) -> list[VerificationResult]:
        logger = get_verifier_logger()
        logger.info("Stage 5: Coalescing results for each relationship")

        results: list[VerificationResult] = []
        for relationship_index, relationship in enumerate(relationships):
            operations = planned_operations[relationship_index]
            is_valid, failure_reason = self._build_relationship_rule(relationship).verify(
                operations,
                task_outputs,
            )
            is_unable_to_verify = (
                failure_reason is not None and "_check_unavailable:" in failure_reason
            )
            results.append(
                VerificationResult(
                    relationship=relationship,
                    is_valid=is_valid,
                    is_unable_to_verify=is_unable_to_verify,
                    failure_reason=failure_reason,
                )
            )

            if is_valid:
                logger.debug(
                    "Relationship %s VALID: %s",
                    relationship_index,
                    self._relationship_label(relationship),
                )
            else:
                logger.warning(
                    "Relationship %s INVALID: %s - %s",
                    relationship_index,
                    self._relationship_label(relationship),
                    failure_reason,
                )
        return results

    def get_sdk_operation_metrics(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._sdk_operation_logs]

    def _build_relationship_rule(self, relationship: AnyRelationship) -> Any:
        kind = self._relationship_label(relationship)
        rule_class = self._RELATIONSHIP_RULES.get(kind)
        if rule_class is None:
            raise ValueError(
                f"Unsupported inferred relationship kind for verification: {kind!r}"
            )
        return rule_class(relationship)

    @staticmethod
    def _relationship_label(relationship: AnyRelationship) -> str:
        if isinstance(relationship, PrimaryKeyRelationship):
            return "PK"
        if isinstance(relationship, PrimaryKeyAlternativeRelationship):
            return "PKA"
        if isinstance(relationship, ForeignKeyRelationship):
            return "FK"
        return relationship.kind
