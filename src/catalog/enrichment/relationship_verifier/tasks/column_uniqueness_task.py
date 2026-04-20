"""Column uniqueness task."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .common.runtime import sample_collection_rows_for_columns, stable_sample_seed


def _normalize_name(name: str) -> str:
    return name.strip().lower()


@dataclass(frozen=True, slots=True)
class ColumnUniquenessTask:
    collection: str
    columns: tuple[str, ...]
    _task_id: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_task_id", self._build_task_id())

    def _build_task_id(self) -> str:
        normalized_columns = "__".join(_normalize_name(column) for column in self.columns)
        return f"column_uniqueness__{_normalize_name(self.collection)}__{normalized_columns}"

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
        rows = sample_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.collection,
            columns=self.columns,
            sample_size=sample_size,
            seed=sample_seed,
        )
        seen_rows: set[tuple[Any, ...]] = set()
        for row in rows:
            if row is None:
                continue
            if row in seen_rows:
                return {"duplicate_groups": 1}
            seen_rows.add(row)
        return {"duplicate_groups": 0}

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
        **_: Any,
    ) -> tuple[Any, str, str | None, dict[str, Any] | None]:
        _ = index_map, max_unindexed_scan_rows
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
                "sampled_query": "sdk_sampling(column_uniqueness)",
            }
        except Exception as error:
            error_text = str(error)
            return {"error": error_text}, "sampling", error_text, None
