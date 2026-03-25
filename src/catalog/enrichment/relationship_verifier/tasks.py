"""Task models for relationship verification with data-backed SQL++ checks."""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "AnyTask",
    "ColumnExistsTask",
    "ColumnNotNullTask",
    "ColumnNotObjectTask",
    "ColumnTypeCompatibilityTask",
    "ColumnUniquenessTask",
    "ValueSetInclusionTask",
]


@dataclass(frozen=True, slots=True)
class ColumnExistsTask:
    """Check that a column exists (is present and non-null) in at least one document."""

    task_id: str
    collection: str
    column: str


@dataclass(frozen=True, slots=True)
class ColumnNotObjectTask:
    """Check that a column value is not an object/array."""

    task_id: str
    collection: str
    column: str


@dataclass(frozen=True, slots=True)
class ColumnNotNullTask:
    """Check that a column value is present and non-null."""

    task_id: str
    collection: str
    column: str


@dataclass(frozen=True, slots=True)
class ColumnUniquenessTask:
    """Check whether the given ordered tuple of columns is unique."""

    task_id: str
    collection: str
    columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ValueSetInclusionTask:
    """Check child tuple values are present in the referenced parent tuple."""

    task_id: str
    child_collection: str
    child_columns: tuple[str, ...]
    parent_collection: str
    parent_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ColumnTypeCompatibilityTask:
    """Check child column types are compatible with parent column types.

    Verifier logic allows string/number interchangeability when validating
    compatibility against observed parent values.
    """

    task_id: str
    child_collection: str
    child_column: str
    parent_collection: str
    parent_column: str


AnyTask = (
    ColumnExistsTask
    | ColumnNotObjectTask
    | ColumnNotNullTask
    | ColumnUniquenessTask
    | ValueSetInclusionTask
    | ColumnTypeCompatibilityTask
)
