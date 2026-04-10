"""Task package for relationship verification."""

from __future__ import annotations

from .column_exists_task import ColumnExistsTask
from .column_not_null_task import ColumnNotNullTask
from .column_not_object_task import ColumnNotObjectTask
from .column_type_compatibility_task import ColumnTypeCompatibilityTask
from .column_uniqueness_task import ColumnUniquenessTask
from .meta_id_reference_exists_task import MetaIdReferenceExistsTask
from .value_set_inclusion_task import ValueSetInclusionTask

__all__ = [
    "AnyTask",
    "ColumnExistsTask",
    "ColumnNotNullTask",
    "ColumnNotObjectTask",
    "MetaIdReferenceExistsTask",
    "ColumnTypeCompatibilityTask",
    "ColumnUniquenessTask",
    "ValueSetInclusionTask",
]

AnyTask = (
    ColumnExistsTask
    | ColumnNotObjectTask
    | ColumnNotNullTask
    | ColumnUniquenessTask
    | ValueSetInclusionTask
    | MetaIdReferenceExistsTask
    | ColumnTypeCompatibilityTask
)
