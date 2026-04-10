"""One-to-many relationship rule."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from catalog.enrichment.relationship_verifier.common.relationships import (
    InferredRelationship,
    foreign_key_relationship_from_inferred,
)
from catalog.enrichment.relationship_verifier.tasks import AnyTask

from .foreign_key_relationship import ForeignKeyRelationshipRule


@dataclass(frozen=True, slots=True)
class OneToManyRelationshipRule:
    relationship: InferredRelationship

    def build_tasks(self) -> list[AnyTask]:
        fk_relationship = foreign_key_relationship_from_inferred(self.relationship)
        return ForeignKeyRelationshipRule(fk_relationship).build_tasks()

    def verify(
        self,
        operations: list[AnyTask],
        task_outputs: dict[str, Any],
    ) -> tuple[bool, str | None]:
        fk_relationship = foreign_key_relationship_from_inferred(self.relationship)
        return ForeignKeyRelationshipRule(fk_relationship).verify(
            operations,
            task_outputs,
        )
