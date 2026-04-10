"""Value set inclusion task."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .common.query_builders import (
    build_use_keys_parent_lookup_query,
    build_value_set_child_rows_query,
    build_value_set_parent_rows_query,
    value_set_parent_is_meta_id,
)
from .common.runtime import (
    has_covering_index,
    is_timeout_error,
    normalize_name,
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

    @staticmethod
    def _rows_to_tuples(rows: list[dict[str, Any]], width: int) -> list[tuple[Any, ...] | None]:
        parsed: list[tuple[Any, ...] | None] = []
        keys = [f"col_{index}" for index in range(width)]
        for row in rows:
            if not isinstance(row, dict):
                parsed.append(None)
                continue
            values: list[Any] = []
            missing = False
            for key in keys:
                if key not in row:
                    missing = True
                    break
                values.append(row.get(key))
            parsed.append(None if missing else tuple(values))
        return parsed

    def _build_child_query(
        self,
        *,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        sample_size: int | None = None,
    ) -> str:
        return build_value_set_child_rows_query(
            self,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            index_map=index_map,
            max_unindexed_scan_rows=max_unindexed_scan_rows,
            sample_size=sample_size,
        )

    def _build_parent_query(
        self,
        *,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        sample_size: int | None = None,
    ) -> str:
        return build_value_set_parent_rows_query(
            self,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            index_map=index_map,
            max_unindexed_scan_rows=max_unindexed_scan_rows,
            sample_size=sample_size,
        )

    def _query_comparison(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        sample_size: int | None = None,
    ) -> dict[str, int]:
        child_rows_raw = cb.run_query(
            self._build_child_query(
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                index_map=index_map,
                max_unindexed_scan_rows=max_unindexed_scan_rows,
                sample_size=sample_size,
            )
        )
        child_rows = self._rows_to_tuples(child_rows_raw, len(self.child_columns))
        valid_child_rows = self._collect_valid_child_rows(child_rows)
        if not valid_child_rows:
            return {"missing_count": 0}

        if value_set_parent_is_meta_id(self):
            child_keys = [str(row[0]) for row in valid_child_rows]
            if not child_keys:
                return {"missing_count": 0}
            parent_lookup_rows = cb.run_query(
                build_use_keys_parent_lookup_query(
                    bucket_name=bucket_name,
                    parent_collection=self.parent_collection,
                    keyspace_map=keyspace_map,
                    keys=child_keys,
                )
            )
            parent_values = {str(value) for value in parent_lookup_rows}
            return self._has_missing_values(
                expected_values={(value,) for value in child_keys},
                actual_values={(value,) for value in parent_values},
            )

        parent_rows_raw = cb.run_query(
            self._build_parent_query(
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                index_map=index_map,
                max_unindexed_scan_rows=max_unindexed_scan_rows,
                sample_size=sample_size,
            )
        )
        parent_rows = self._rows_to_tuples(parent_rows_raw, len(self.parent_columns))
        parent_values = {
            row_values
            for row_values in parent_rows
            if row_values is not None and len(row_values) == len(self.parent_columns)
        }
        return self._has_missing_values(
            expected_values=valid_child_rows,
            actual_values=parent_values,
        )

    def _collect_valid_child_rows(
        self,
        rows: list[tuple[Any, ...] | None],
    ) -> set[tuple[Any, ...]]:
        return {
            row_values
            for row_values in rows
            if row_values is not None
            and len(row_values) == len(self.child_columns)
            and not any(value is None for value in row_values)
        }

    @staticmethod
    def _has_missing_values(
        *,
        expected_values: set[tuple[Any, ...]],
        actual_values: set[tuple[Any, ...]],
    ) -> dict[str, int]:
        for value in expected_values:
            if value not in actual_values:
                return {"missing_count": 1}
        return {"missing_count": 0}

    def _sqlpp_sampling_allowed(
        self,
        *,
        index_map: dict[str, list[list[str]]],
    ) -> bool:
        if any(normalize_name(column) == "$meta_id" for column in self.child_columns):
            return False
        if any(normalize_name(column) == "$meta_id" for column in self.parent_columns):
            return False
        return has_covering_index(index_map, self.child_collection, self.child_columns) or has_covering_index(
            index_map, self.parent_collection, self.parent_columns
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
        parent_rows = scan_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.parent_collection,
            columns=self.parent_columns,
            limit_rows=None,
        )
        parent_values = {
            row_values
            for row_values in parent_rows
            if row_values is not None and len(row_values) == len(self.parent_columns)
        }

        child_rows = sampled_child_rows
        if child_rows is None:
            child_rows = scan_collection_rows_for_columns(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                collection=self.child_collection,
                columns=self.child_columns,
                limit_rows=None,
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
        if self._sqlpp_sampling_allowed(index_map=index_map):
            metadata: dict[str, Any] = {
                "fallback_stage": fallback_stage,
                "first_error": first_error,
                "sample_size": value_set_timeout_sample_size,
                "sampled_query": "sqlpp_sampling(value_set_inclusion)",
            }
            try:
                output = self._query_comparison(
                    cb=cb,
                    bucket_name=bucket_name,
                    keyspace_map=keyspace_map,
                    index_map=index_map,
                    max_unindexed_scan_rows=max_unindexed_scan_rows,
                    sample_size=value_set_timeout_sample_size,
                )
                return output, None, metadata
            except Exception as sampled_error:
                return {"error": str(sampled_error)}, str(sampled_error), metadata

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
        metadata = {
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
        has_index_on_either_side = child_indexed or parent_indexed
        if has_index_on_either_side:
            try:
                output = self._query_comparison(
                    cb=cb,
                    bucket_name=bucket_name,
                    keyspace_map=keyspace_map,
                    index_map=index_map,
                    max_unindexed_scan_rows=max_unindexed_scan_rows,
                )
                return output, "query", None, None
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
