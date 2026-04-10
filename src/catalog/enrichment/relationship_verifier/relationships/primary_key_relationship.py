"""Primary key relationship rule."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from catalog.enrichment.relationship_verifier.common.relationships import (
    PrimaryKeyRelationship,
    uses_meta_id,
)
from catalog.enrichment.relationship_verifier.tasks import AnyTask


@dataclass(frozen=True, slots=True)
class PrimaryKeyRelationshipRule:
    relationship: PrimaryKeyRelationship

    def build_tasks(self) -> list[AnyTask]:
        return []

    def verify(
        self,
        operations: list[AnyTask],
        task_outputs: dict[str, Any],
    ) -> tuple[bool, str | None]:
        _ = task_outputs
        if not uses_meta_id(self.relationship.columns):
            return False, (
                "pk_must_be_meta_id: "
                f"{self.relationship.table}{self.relationship.columns} must use ($meta_id)"
            )
        return True, None
