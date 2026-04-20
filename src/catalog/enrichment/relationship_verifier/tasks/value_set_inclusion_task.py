"""Value set inclusion task."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .common.query_builders import value_set_parent_is_meta_id
from .common.runtime import (
    resolve_collection_keyspace,
    sample_collection_rows_for_columns,
    stable_sample_seed,
)


def _normalize_name(name: str) -> str:
    return name.strip().lower()


@dataclass(frozen=True, slots=True)
class ValueSetInclusionTask:
    child_collection: str
    child_columns: tuple[str, ...]
    parent_collection: str
    parent_columns: tuple[str, ...]
    _task_id: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_task_id", self._build_task_id())

    def _build_task_id(self) -> str:
        normalized_child_columns = "__".join(_normalize_name(column) for column in self.child_columns)
        normalized_parent_columns = "__".join(_normalize_name(column) for column in self.parent_columns)
        return (
            "value_set_inclusion"
            f"__{_normalize_name(self.child_collection)}"
            f"__{normalized_child_columns}"
            f"__{_normalize_name(self.parent_collection)}"
            f"__{normalized_parent_columns}"
        )

    @property
    def task_id(self) -> str:
        return self._task_id

    def _collect_valid_rows(
        self,
        rows: list[tuple[Any, ...] | None],
        expected_width: int,
    ) -> set[tuple[Any, ...]]:
        return {
            row_values
            for row_values in rows
            if row_values is not None
            and len(row_values) == expected_width
            and not any(value is None for value in row_values)
        }

    def _run_sampling_check(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        sample_size: int,
        sample_seed: int,
    ) -> dict[str, int]:
        child_rows = sample_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.child_collection,
            columns=self.child_columns,
            sample_size=sample_size,
            seed=sample_seed,
        )
        valid_child_rows = self._collect_valid_rows(child_rows, len(self.child_columns))
        if not valid_child_rows:
            return {"missing_count": 0}

        if value_set_parent_is_meta_id(self):
            scope_name, parent_collection = resolve_collection_keyspace(
                self.parent_collection,
                keyspace_map,
            )
            for row_values in valid_child_rows:
                referenced_id = str(row_values[0])
                if not cb.document_exists(
                    bucket_name=bucket_name,
                    scope_name=scope_name,
                    collection_name=parent_collection,
                    document_id=referenced_id,
                ):
                    return {"missing_count": 1}
            return {"missing_count": 0}

        parent_rows = sample_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.parent_collection,
            columns=self.parent_columns,
            sample_size=sample_size,
            seed=sample_seed,
        )
        parent_values = self._collect_valid_rows(parent_rows, len(self.parent_columns))
        for value in valid_child_rows:
            if value not in parent_values:
                return {"missing_count": 1}
        return {"missing_count": 0}

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
                "sampled_query": "sdk_sampling(value_set_inclusion)",
            }
        except Exception as error:
            error_text = str(error)
            return {"error": error_text}, "sampling", error_text, None
