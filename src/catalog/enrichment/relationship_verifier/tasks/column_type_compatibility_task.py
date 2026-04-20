"""Column type compatibility task."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .common.runtime import (
    python_value_type_name,
    sample_collection_rows_for_columns,
    stable_sample_seed,
)


def _normalize_name(name: str) -> str:
    return name.strip().lower()


@dataclass(frozen=True, slots=True)
class ColumnTypeCompatibilityTask:
    child_collection: str
    child_column: str
    parent_collection: str
    parent_column: str
    _task_id: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_task_id", self._build_task_id())

    def _build_task_id(self) -> str:
        return (
            "column_type_compatibility"
            f"__{_normalize_name(self.child_collection)}"
            f"__{_normalize_name(self.child_column)}"
            f"__{_normalize_name(self.parent_collection)}"
            f"__{_normalize_name(self.parent_column)}"
        )

    @property
    def task_id(self) -> str:
        return self._task_id

    def _run_sampling_check(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        sample_size: int,
        sample_seed: int,
    ) -> dict[str, int]:
        sampled_child_rows = sample_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.child_collection,
            columns=(self.child_column,),
            sample_size=sample_size,
            seed=sample_seed,
        )
        parent_rows = sample_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.parent_collection,
            columns=(self.parent_column,),
            sample_size=sample_size,
            seed=sample_seed,
        )
        parent_types = {
            python_value_type_name(parent_value[0])
            for parent_value in parent_rows
            if parent_value is not None and parent_value[0] is not None
        }
        has_parent_string_or_number = bool({"string", "number"} & parent_types)
        for child_value in sampled_child_rows:
            child_type = (
                "missing"
                if child_value is None
                else python_value_type_name(child_value[0])
            )
            if child_type in {"string", "number"} and has_parent_string_or_number:
                continue
            if child_type not in parent_types:
                return {"type_mismatch_count": 1}
        return {"type_mismatch_count": 0}

    def run(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        value_set_timeout_sample_size: int,
        value_set_timeout_sample_seed: int,
        sdk_operation_logs: list[dict[str, Any]],
        **_: Any,
    ) -> tuple[Any, str, str | None, dict[str, Any] | None]:
        _ = index_map, max_unindexed_scan_rows, sdk_operation_logs
        sample_seed = stable_sample_seed(value_set_timeout_sample_seed, self.task_id)
        try:
            output = self._run_sampling_check(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                sample_size=value_set_timeout_sample_size,
                sample_seed=sample_seed,
            )
            return output, "sampling", None, {
                "sample_size": value_set_timeout_sample_size,
                "sampled_query": "sdk_sampling(column_type_compatibility)",
            }
        except Exception as error:
            error_text = str(error)
            return {"error": error_text}, "sampling", error_text, None
