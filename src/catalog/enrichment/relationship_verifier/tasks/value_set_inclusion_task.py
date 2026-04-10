"""Value set inclusion task."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .common.query_builders import build_value_set_inclusion_query
from .common.runtime import (
    has_covering_index,
    is_timeout_error,
    query_limit_for_collection,
    sample_collection_rows_for_columns,
    scan_collection_rows_for_columns,
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

    def _build_query(
        self,
        *,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
    ) -> str:
        return build_value_set_inclusion_query(
            self,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            index_map=index_map,
            max_unindexed_scan_rows=max_unindexed_scan_rows,
        )

    def _sdk_fallback(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        sampled_child_rows: list[tuple[Any, ...] | None] | None = None,
    ) -> dict[str, int]:
        parent_limit = query_limit_for_collection(
            index_map=index_map,
            collection=self.parent_collection,
            columns=self.parent_columns,
            max_unindexed_scan_rows=max_unindexed_scan_rows,
        )
        parent_rows = scan_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.parent_collection,
            columns=self.parent_columns,
            limit_rows=parent_limit,
        )
        parent_values = {
            row_values
            for row_values in parent_rows
            if row_values is not None and len(row_values) == len(self.parent_columns)
        }

        child_rows = sampled_child_rows
        if child_rows is None:
            child_limit = query_limit_for_collection(
                index_map=index_map,
                collection=self.child_collection,
                columns=self.child_columns,
                max_unindexed_scan_rows=max_unindexed_scan_rows,
            )
            child_rows = scan_collection_rows_for_columns(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                collection=self.child_collection,
                columns=self.child_columns,
                limit_rows=child_limit,
            )

        for row_values in child_rows:
            if row_values is None or len(row_values) != len(self.child_columns):
                continue
            if any(value is None for value in row_values):
                continue
            if row_values not in parent_values:
                return {"missing_count": 1}
        return {"missing_count": 0}

    def _sampled_fallback(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        value_set_timeout_sample_size: int,
        value_set_timeout_sample_seed: int,
        fallback_stage: str,
        first_error: str,
    ) -> tuple[Any, str | None, dict[str, Any]]:
        sample_seed = stable_sample_seed(value_set_timeout_sample_seed, self.task_id)
        sampled_child_rows = sample_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.child_collection,
            columns=self.child_columns,
            sample_size=value_set_timeout_sample_size,
            seed=sample_seed,
        )
        metadata: dict[str, Any] = {
            "fallback_stage": fallback_stage,
            "first_error": first_error,
            "sample_size": value_set_timeout_sample_size,
            "sampled_query": "sdk_sampling(value_set_inclusion)",
        }
        try:
            output = self._sdk_fallback(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                index_map=index_map,
                max_unindexed_scan_rows=max_unindexed_scan_rows,
                sampled_child_rows=sampled_child_rows,
            )
            return output, None, metadata
        except Exception as sampled_error:
            return {"error": str(sampled_error)}, str(sampled_error), metadata

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
        child_indexed = has_covering_index(index_map, self.child_collection, self.child_columns)
        parent_indexed = has_covering_index(index_map, self.parent_collection, self.parent_columns)
        use_query = child_indexed or parent_indexed

        if use_query:
            try:
                rows = cb.run_query(
                    self._build_query(
                        bucket_name=bucket_name,
                        keyspace_map=keyspace_map,
                        index_map=index_map,
                        max_unindexed_scan_rows=max_unindexed_scan_rows,
                    )
                )
                return (rows[0] if rows else None), "query", None, None
            except Exception as error:
                error_text = str(error)
                if is_timeout_error(error_text):
                    fallback_output, fallback_error, fallback_metadata = self._sampled_fallback(
                        cb=cb,
                        bucket_name=bucket_name,
                        keyspace_map=keyspace_map,
                        index_map=index_map,
                        max_unindexed_scan_rows=max_unindexed_scan_rows,
                        value_set_timeout_sample_size=value_set_timeout_sample_size,
                        value_set_timeout_sample_seed=value_set_timeout_sample_seed,
                        fallback_stage="query_timeout",
                        first_error=error_text,
                    )
                    return fallback_output, "sample_fallback", fallback_error, fallback_metadata
                return {"error": error_text}, "query", error_text, None

        started_ms = time.perf_counter() * 1000
        try:
            fallback_output = self._sdk_fallback(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                index_map=index_map,
                max_unindexed_scan_rows=max_unindexed_scan_rows,
            )
            elapsed_ms = time.perf_counter() * 1000 - started_ms
            sdk_operation_logs.append(
                {
                    "task_id": self.task_id,
                    "operation_type": "value_set_inclusion",
                    "elapsed_ms": elapsed_ms,
                    "success": True,
                    "fallback_reason": "inclusion_check_requires_covering_index_on_either_side",
                    "error": None,
                }
            )
            return fallback_output, "sdk_fallback", None, None
        except Exception as error:
            elapsed_ms = time.perf_counter() * 1000 - started_ms
            sdk_operation_logs.append(
                {
                    "task_id": self.task_id,
                    "operation_type": "value_set_inclusion",
                    "elapsed_ms": elapsed_ms,
                    "success": False,
                    "fallback_reason": "inclusion_check_requires_covering_index_on_either_side",
                    "error": str(error),
                }
            )
            error_text = str(error)
            if is_timeout_error(error_text):
                fallback_output, fallback_error, fallback_metadata = self._sampled_fallback(
                    cb=cb,
                    bucket_name=bucket_name,
                    keyspace_map=keyspace_map,
                    index_map=index_map,
                    max_unindexed_scan_rows=max_unindexed_scan_rows,
                    value_set_timeout_sample_size=value_set_timeout_sample_size,
                    value_set_timeout_sample_seed=value_set_timeout_sample_seed,
                    fallback_stage="sdk_timeout",
                    first_error=error_text,
                )
                return fallback_output, "sample_fallback", fallback_error, fallback_metadata
            return {"error": error_text}, "sdk_fallback", error_text, None
