"""Foreign key relationship rule."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from catalog.enrichment.relationship_verifier.common.relationships import (
    META_ID_SENTINEL,
    ForeignKeyRelationship,
)
from catalog.enrichment.relationship_verifier.common.task_output import get_task_count
from catalog.enrichment.relationship_verifier.tasks import (
    AnyTask,
    ColumnExistsTask,
    ColumnTypeCompatibilityTask,
    MetaIdReferenceExistsTask,
    ValueSetInclusionTask,
)


@dataclass(frozen=True, slots=True)
class ForeignKeyRelationshipRule:
    relationship: ForeignKeyRelationship

    def build_tasks(self) -> list[AnyTask]:
        operations: list[AnyTask] = []
        for child_column in self.relationship.child_columns:
            operations.append(
                ColumnExistsTask(
                    collection=self.relationship.child_table,
                    column=child_column,
                )
            )

        for parent_column in self.relationship.parent_columns:
            if parent_column == META_ID_SENTINEL:
                continue
            operations.append(
                ColumnExistsTask(
                    collection=self.relationship.parent_table,
                    column=parent_column,
                )
            )

        operations.append(
            ValueSetInclusionTask(
                child_collection=self.relationship.child_table,
                child_columns=self.relationship.child_columns,
                parent_collection=self.relationship.parent_table,
                parent_columns=self.relationship.parent_columns,
            )
        )

        for child_column, parent_column in zip(
            self.relationship.child_columns,
            self.relationship.parent_columns,
            strict=True,
        ):
            if parent_column == META_ID_SENTINEL:
                operations.append(
                    MetaIdReferenceExistsTask(
                        child_collection=self.relationship.child_table,
                        child_column=child_column,
                        parent_collection=self.relationship.parent_table,
                    )
                )
                continue

            operations.append(
                ColumnTypeCompatibilityTask(
                    child_collection=self.relationship.child_table,
                    child_column=child_column,
                    parent_collection=self.relationship.parent_table,
                    parent_column=parent_column,
                )
            )

        return operations

    def verify(  # noqa: PLR0911, PLR0912
        self,
        operations: list[AnyTask],
        task_outputs: dict[str, Any],
    ) -> tuple[bool, str | None]:
        for child_column in self.relationship.child_columns:
            child_exists_task_id = ColumnExistsTask(
                collection=self.relationship.child_table,
                column=child_column,
            ).task_id
            exists_count, error_reason = get_task_count(
                task_outputs,
                child_exists_task_id,
                "exists_count",
            )
            if exists_count is None:
                return False, (
                    "fk_check_unavailable: "
                    f"could not read child column evidence for "
                    f"{self.relationship.child_table}.{child_column} ({error_reason})"
                )
            if exists_count == 0:
                return False, (
                    "fk_child_column_not_observed_in_sample: "
                    f"{self.relationship.child_table}.{child_column} had no non-null values in sampled rows"
                )

        for parent_column in self.relationship.parent_columns:
            if parent_column == META_ID_SENTINEL:
                continue
            parent_exists_task_id = ColumnExistsTask(
                collection=self.relationship.parent_table,
                column=parent_column,
            ).task_id
            exists_count, error_reason = get_task_count(
                task_outputs,
                parent_exists_task_id,
                "exists_count",
            )
            if exists_count is None:
                return False, (
                    "fk_check_unavailable: "
                    f"could not read parent column evidence for "
                    f"{self.relationship.parent_table}.{parent_column} ({error_reason})"
                )
            if exists_count == 0:
                return False, (
                    "fk_parent_column_not_observed_in_sample: "
                    f"{self.relationship.parent_table}.{parent_column} had no non-null values in sampled rows"
                )

        inclusion_task_id = ValueSetInclusionTask(
            child_collection=self.relationship.child_table,
            child_columns=self.relationship.child_columns,
            parent_collection=self.relationship.parent_table,
            parent_columns=self.relationship.parent_columns,
        ).task_id
        missing_count, error_reason = get_task_count(
            task_outputs,
            inclusion_task_id,
            "missing_count",
        )
        if missing_count is None:
            return False, (
                "fk_check_unavailable: "
                f"could not read referential inclusion check for "
                f"{self.relationship.child_table}{self.relationship.child_columns} -> "
                f"{self.relationship.parent_table}{self.relationship.parent_columns} ({error_reason})"
            )
        if missing_count > 0:
            return False, (
                "fk_referential_inclusion_failed: "
                f"{self.relationship.child_table}{self.relationship.child_columns} -> "
                f"{self.relationship.parent_table}{self.relationship.parent_columns} has "
                f"{missing_count} sampled child row(s) with no matching parent"
            )

        for child_column, parent_column in zip(
            self.relationship.child_columns,
            self.relationship.parent_columns,
            strict=True,
        ):
            if parent_column == META_ID_SENTINEL:
                meta_id_task_id = MetaIdReferenceExistsTask(
                    child_collection=self.relationship.child_table,
                    child_column=child_column,
                    parent_collection=self.relationship.parent_table,
                ).task_id
                missing_reference, error_reason = get_task_count(
                    task_outputs,
                    meta_id_task_id,
                    "has_missing_reference",
                )
                if missing_reference is None:
                    return False, (
                        "fk_check_unavailable: "
                        f"could not read META-ID reference check for "
                        f"{self.relationship.child_table}.{child_column} -> "
                        f"{self.relationship.parent_table}.$meta_id ({error_reason})"
                    )
                if missing_reference > 0:
                    return False, (
                        "fk_meta_id_reference_missing: "
                        f"{self.relationship.child_table}.{child_column} references "
                        f"document id(s) not present in {self.relationship.parent_table}"
                    )
                continue

            type_check_task_id = ColumnTypeCompatibilityTask(
                child_collection=self.relationship.child_table,
                child_column=child_column,
                parent_collection=self.relationship.parent_table,
                parent_column=parent_column,
            ).task_id
            mismatch_count, error_reason = get_task_count(
                task_outputs,
                type_check_task_id,
                "type_mismatch_count",
            )
            if mismatch_count is None:
                return False, (
                    "fk_check_unavailable: "
                    f"could not read type compatibility check for "
                    f"{self.relationship.child_table}.{child_column} -> "
                    f"{self.relationship.parent_table}.{parent_column} ({error_reason})"
                )
            if mismatch_count > 0:
                return False, (
                    "fk_type_mismatch: "
                    f"{self.relationship.child_table}.{child_column} -> "
                    f"{self.relationship.parent_table}.{parent_column} has "
                    f"{mismatch_count} incompatible sampled value(s)"
                )

        return True, None
