"""Column type compatibility task."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from catalog.enrichment.relationship_verifier.common.relationships import (
    META_ID_SENTINEL,
)

from .common.query_builders import build_type_compatibility_query
from .common.runtime import (
    has_covering_index,
    is_timeout_error,
    python_value_type_name,
    sample_collection_rows_for_columns,
    scan_collection_rows_for_columns,
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

    def _build_query(
        self,
        *,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        sample_size: int | None = None,
    ) -> str:
        return build_type_compatibility_query(
            child_collection=self.child_collection,
            child_column=self.child_column,
            parent_collection=self.parent_collection,
            parent_column=self.parent_column,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            index_map=index_map,
            max_unindexed_scan_rows=max_unindexed_scan_rows,
            sample_size=sample_size,
        )

    def _sdk_fallback(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
    ) -> dict[str, int]:
        parent_values = scan_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.parent_collection,
            columns=(self.parent_column,),
            limit_rows=None,
        )
        parent_types = {
            python_value_type_name(parent_value[0])
            for parent_value in parent_values
            if parent_value is not None and parent_value[0] is not None
        }
        has_parent_string_or_number = bool({"string", "number"} & parent_types)
        child_values = scan_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.child_collection,
            columns=(self.child_column,),
            limit_rows=None,
        )
        for child_value in child_values:
            if child_value is None:
                child_type = "missing"
            else:
                child_type = python_value_type_name(child_value[0])
            if child_type in {"string", "number"} and has_parent_string_or_number:
                continue
            if child_type not in parent_types:
                return {"type_mismatch_count": 1}
        return {"type_mismatch_count": 0}

    def _sampled_fallback(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        value_set_timeout_sample_size: int,
        value_set_timeout_sample_seed: int,
        fallback_stage: str,
        first_error: str,
    ):
        if (
            self.child_column not in {META_ID_SENTINEL}
            and self.parent_column not in {META_ID_SENTINEL}
            and (
                has_covering_index(index_map, self.child_collection, (self.child_column,))
                or has_covering_index(index_map, self.parent_collection, (self.parent_column,))
            )
        ):
            try:
                rows = cb.run_query(
                    self._build_query(
                        bucket_name=bucket_name,
                        keyspace_map=keyspace_map,
                        index_map=index_map,
                        max_unindexed_scan_rows=value_set_timeout_sample_size,
                        sample_size=value_set_timeout_sample_size,
                    )
                )
                output = rows[0] if rows else {"type_mismatch_count": 0}
                return (
                    output,
                    None,
                    {
                        "fallback_stage": fallback_stage,
                        "first_error": first_error,
                        "sample_size": value_set_timeout_sample_size,
                        "sampled_query": "sqlpp_sampling(column_type_compatibility)",
                    },
                )
            except Exception as sampled_error:
                return {"error": str(sampled_error)}, str(sampled_error), None

        try:
            sample_seed = stable_sample_seed(value_set_timeout_sample_seed, self.task_id)
            sampled_child_rows = sample_collection_rows_for_columns(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                collection=self.child_collection,
                columns=(self.child_column,),
                sample_size=value_set_timeout_sample_size,
                seed=sample_seed,
            )
            parent_rows = sample_collection_rows_for_columns(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                collection=self.parent_collection,
                columns=(self.parent_column,),
                sample_size=value_set_timeout_sample_size,
                seed=sample_seed,
            )
            parent_types = {
                python_value_type_name(parent_value[0])
                for parent_value in parent_rows
                if parent_value is not None and parent_value[0] is not None
            }
            has_parent_string_or_number = bool({"string", "number"} & parent_types)
            for child_value in sampled_child_rows:
                if child_value is None:
                    child_type = "missing"
                else:
                    child_type = python_value_type_name(child_value[0])
                if child_type in {"string", "number"} and has_parent_string_or_number:
                    continue
                if child_type not in parent_types:
                    return (
                        {"type_mismatch_count": 1},
                        None,
                        {
                            "fallback_stage": fallback_stage,
                            "first_error": first_error,
                            "sample_size": value_set_timeout_sample_size,
                            "sampled_query": "sdk_sampling(column_type_compatibility)",
                        },
                    )
            return (
                {"type_mismatch_count": 0},
                None,
                {
                    "fallback_stage": fallback_stage,
                    "first_error": first_error,
                    "sample_size": value_set_timeout_sample_size,
                    "sampled_query": "sdk_sampling(column_type_compatibility)",
                },
            )
        except Exception as sampled_error:
            return {"error": str(sampled_error)}, str(sampled_error), None

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
        child_indexed = has_covering_index(index_map, self.child_collection, (self.child_column,))
        parent_indexed = has_covering_index(index_map, self.parent_collection, (self.parent_column,))
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
                    "operation_type": "column_type_compatibility",
                    "elapsed_ms": elapsed_ms,
                    "success": True,
                    "fallback_reason": "type_check_requires_covering_index_on_either_side",
                    "error": None,
                }
            )
            return fallback_output, "sdk_fallback", None, None
        except Exception as error:
            elapsed_ms = time.perf_counter() * 1000 - started_ms
            sdk_operation_logs.append(
                {
                    "task_id": self.task_id,
                    "operation_type": "column_type_compatibility",
                    "elapsed_ms": elapsed_ms,
                    "success": False,
                    "fallback_reason": "type_check_requires_covering_index_on_either_side",
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
                    value_set_timeout_sample_size=value_set_timeout_sample_size,
                    value_set_timeout_sample_seed=value_set_timeout_sample_seed,
                    fallback_stage="sdk_timeout",
                    first_error=error_text,
                )
                return fallback_output, "sample_fallback", fallback_error, fallback_metadata
            return {"error": error_text}, "sdk_fallback", error_text, None
