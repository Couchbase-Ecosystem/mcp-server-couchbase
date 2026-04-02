"""Shared relationship models and conversion helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

META_ID_SENTINEL = "$meta_id"

InferredRelationshipKind = Literal["OO", "OM", "MM"]
RelationshipKind = Literal["PK", "PKA", "FK", "OO", "OM", "MM"]

__all__ = [
    "META_ID_SENTINEL",
    "AnyRelationship",
    "ForeignKeyRelationship",
    "InferredRelationship",
    "InferredRelationshipKind",
    "PrimaryKeyRelationship",
    "PrimaryKeyAlternativeRelationship",
    "RelationshipKind",
    "relationship_from_dict",
    "uses_meta_id",
]


@dataclass(frozen=True, slots=True)
class InferredRelationship:
    """Typed relationship output for schema-level FK inference."""

    kind: InferredRelationshipKind
    table1: str
    table2: str
    foreign_key_table: str
    from_columns: tuple[str, ...]
    to_columns: tuple[str, ...]
    connecting_table: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of the relationship."""
        return {
            "kind": self.kind,
            "table1": self.table1,
            "table2": self.table2,
            "foreign_key_table": self.foreign_key_table,
            "from_columns": list(self.from_columns),
            "to_columns": list(self.to_columns),
            "connecting_table": self.connecting_table,
        }


@dataclass(frozen=True, slots=True)
class PrimaryKeyRelationship:
    """Primary-key candidate for one collection."""

    table: str
    columns: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of the relationship."""
        return {
            "kind": "PK",
            "table": self.table,
            "columns": list(self.columns),
        }


@dataclass(frozen=True, slots=True)
class PrimaryKeyAlternativeRelationship:
    """Logical key candidate for one collection (index/small-data verified)."""

    table: str
    columns: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of the relationship."""
        return {
            "kind": "PKA",
            "table": self.table,
            "columns": list(self.columns),
        }


@dataclass(frozen=True, slots=True)
class ForeignKeyRelationship:
    """Foreign-key candidate from one collection to another."""

    child_table: str
    child_columns: tuple[str, ...]
    parent_table: str
    parent_columns: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of the relationship."""
        return {
            "kind": "FK",
            "child_table": self.child_table,
            "child_columns": list(self.child_columns),
            "parent_table": self.parent_table,
            "parent_columns": list(self.parent_columns),
        }


AnyRelationship = (
    InferredRelationship
    | PrimaryKeyRelationship
    | PrimaryKeyAlternativeRelationship
    | ForeignKeyRelationship
)


def uses_meta_id(columns: tuple[str, ...]) -> bool:
    """Return True when columns use the META().id sentinel representation."""
    return len(columns) == 1 and columns[0] == META_ID_SENTINEL


def relationship_from_dict(data: Mapping[str, Any]) -> AnyRelationship:
    """Convert a plain dictionary representation into a relationship object."""
    kind = str(data.get("kind", "")).strip().upper()

    if kind in {"OO", "OM", "MM"}:
        return InferredRelationship(
            kind=kind,  # type: ignore[arg-type]
            table1=str(data["table1"]),
            table2=str(data["table2"]),
            foreign_key_table=str(data["foreign_key_table"]),
            from_columns=_as_tuple_of_strings(data.get("from_columns", [])),
            to_columns=_as_tuple_of_strings(data.get("to_columns", [])),
            connecting_table=(
                str(data["connecting_table"])
                if data.get("connecting_table") is not None
                else None
            ),
        )

    if kind == "PK":
        return PrimaryKeyRelationship(
            table=str(data["table"]),
            columns=_as_tuple_of_strings(data.get("columns", [])),
        )

    if kind == "PKA":
        return PrimaryKeyAlternativeRelationship(
            table=str(data["table"]),
            columns=_as_tuple_of_strings(data.get("columns", [])),
        )

    if kind == "FK":
        return ForeignKeyRelationship(
            child_table=str(data["child_table"]),
            child_columns=_as_tuple_of_strings(data.get("child_columns", [])),
            parent_table=str(data["parent_table"]),
            parent_columns=_as_tuple_of_strings(data.get("parent_columns", [])),
        )

    raise ValueError(f"Unsupported relationship kind: {kind!r}")


def _as_tuple_of_strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()

    if isinstance(value, str):
        return (value,)

    if isinstance(value, list | tuple):
        return tuple(str(item) for item in value)

    raise TypeError(
        f"Expected list/tuple/string for columns, got {type(value).__name__}."
    )
