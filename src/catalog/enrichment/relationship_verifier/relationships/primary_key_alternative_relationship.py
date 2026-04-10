"""Primary key alternative relationship rule."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from catalog.enrichment.relationship_verifier.common.relationships import (
    PrimaryKeyAlternativeRelationship,
)
from catalog.enrichment.relationship_verifier.common.task_output import get_task_count
from catalog.enrichment.relationship_verifier.tasks import (
    AnyTask,
    ColumnExistsTask,
    ColumnNotObjectTask,
)


@dataclass(frozen=True, slots=True)
class PrimaryKeyAlternativeRelationshipRule:
    relationship: PrimaryKeyAlternativeRelationship

    def build_tasks(self) -> list[AnyTask]:
        tasks: list[AnyTask] = []
        for column in self.relationship.columns:
            tasks.append(ColumnExistsTask(collection=self.relationship.table, column=column))
            tasks.append(
                ColumnNotObjectTask(collection=self.relationship.table, column=column)
            )
        return tasks

    def verify(
        self,
        operations: list[AnyTask],
        task_outputs: dict[str, Any],
    ) -> tuple[bool, str | None]:
        exists_task_ids = [
            operation.task_id
            for operation in operations
            if isinstance(operation, ColumnExistsTask)
        ]
        for task_id, column in zip(exists_task_ids, self.relationship.columns, strict=True):
            exists_count, error_reason = get_task_count(
                task_outputs,
                task_id,
                "exists_count",
            )
            if exists_count is None:
                return False, (
                    "pka_check_unavailable: "
                    f"could not read sampled evidence for {self.relationship.table}.{column}"
                    f" ({error_reason})"
                )
            if exists_count == 0:
                return False, (
                    "pka_column_not_observed_in_sample: "
                    f"{self.relationship.table}.{column} had no non-null values in sampled rows"
                )

        nested_task_ids = [
            operation.task_id
            for operation in operations
            if isinstance(operation, ColumnNotObjectTask)
        ]
        for task_id, column in zip(nested_task_ids, self.relationship.columns, strict=True):
            nested_count, error_reason = get_task_count(
                task_outputs,
                task_id,
                "nested_count",
            )
            if nested_count is None:
                return False, (
                    "pka_check_unavailable: "
                    f"could not read nested-value check for {self.relationship.table}.{column}"
                    f" ({error_reason})"
                )
            if nested_count > 0:
                return False, (
                    "pka_column_contains_nested_values: "
                    f"{self.relationship.table}.{column} has {nested_count} sampled row(s) "
                    "with object/array values"
                )

        return True, None
