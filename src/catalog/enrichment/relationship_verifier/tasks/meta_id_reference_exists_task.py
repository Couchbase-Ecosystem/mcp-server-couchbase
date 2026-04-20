"""META ID reference exists task."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .common.runtime import (
    resolve_collection_keyspace,
    sample_collection_rows_for_columns,
    stable_sample_seed,
)


def _normalize_name(name: str) -> str:
    return name.strip().lower()


@dataclass(frozen=True, slots=True)
class MetaIdReferenceExistsTask:
    child_collection: str
    child_column: str
    parent_collection: str
    _task_id: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_task_id", self._build_task_id())

    def _build_task_id(self) -> str:
        return (
            "meta_id_reference_exists"
            f"__{_normalize_name(self.child_collection)}"
            f"__{_normalize_name(self.child_column)}"
            f"__{_normalize_name(self.parent_collection)}"
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
        scope_name, parent_collection = resolve_collection_keyspace(
            self.parent_collection,
            keyspace_map,
        )
        sampled_child_rows = sample_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.child_collection,
            columns=(self.child_column,),
            sample_size=sample_size,
            seed=sample_seed,
        )
        for row_value in sampled_child_rows:
            if row_value is None or not row_value or row_value[0] is None:
                continue
            referenced_id = str(row_value[0])
            if not cb.document_exists(
                bucket_name=bucket_name,
                scope_name=scope_name,
                collection_name=parent_collection,
                document_id=referenced_id,
            ):
                return {"has_missing_reference": 1}
        return {"has_missing_reference": 0}

    def run(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        meta_id_timeout_sample_size: int,
        meta_id_timeout_sample_seed: int,
        sdk_operation_logs: list[dict[str, Any]],
        **_: Any,
    ) -> tuple[Any, str, str | None, dict[str, Any] | None]:
        _ = index_map, max_unindexed_scan_rows, sdk_operation_logs
        sample_seed = stable_sample_seed(meta_id_timeout_sample_seed, self.task_id)
        try:
            output = self._run_sampling_check(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                sample_size=meta_id_timeout_sample_size,
                sample_seed=sample_seed,
            )
            return output, "sampling", None, {
                "sample_size": meta_id_timeout_sample_size,
                "sampled_query": "sdk_sampling(meta_id_reference_exists)",
            }
        except Exception as error:
            error_text = str(error)
            return {"error": error_text}, "sampling", error_text, None
