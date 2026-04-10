"""META ID reference exists task."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .common.query_builders import build_meta_id_reference_exists_query
from .common.runtime import (
    has_primary_index,
    is_timeout_error,
    query_limit_for_collection,
    resolve_collection_keyspace,
    sample_collection_rows_for_columns,
    scan_collection_rows_for_columns,
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

    def _build_query(
        self,
        *,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
    ) -> str:
        return build_meta_id_reference_exists_query(
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
        scope_name, parent_collection = resolve_collection_keyspace(
            self.parent_collection,
            keyspace_map,
        )
        child_rows = sampled_child_rows
        if child_rows is None:
            child_limit = query_limit_for_collection(
                index_map=index_map,
                collection=self.child_collection,
                columns=(self.child_column,),
                max_unindexed_scan_rows=max_unindexed_scan_rows,
            )
            child_rows = scan_collection_rows_for_columns(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                collection=self.child_collection,
                columns=(self.child_column,),
                limit_rows=child_limit,
            )

        for row_value in child_rows:
            if row_value is None or not row_value:
                continue
            referenced_id = row_value[0]
            if referenced_id is None:
                continue
            if not isinstance(referenced_id, str):
                referenced_id = str(referenced_id)
            if not cb.document_exists(
                bucket_name=bucket_name,
                scope_name=scope_name,
                collection_name=parent_collection,
                document_id=referenced_id,
            ):
                return {"has_missing_reference": 1}
        return {"has_missing_reference": 0}

    def _sampled_fallback(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        meta_id_timeout_sample_size: int,
        meta_id_timeout_sample_seed: int,
        fallback_stage: str,
        first_error: str,
    ) -> tuple[Any, str | None, dict[str, Any]]:
        sample_seed = stable_sample_seed(meta_id_timeout_sample_seed, self.task_id)
        sampled_child_rows = sample_collection_rows_for_columns(
            cb=cb,
            bucket_name=bucket_name,
            keyspace_map=keyspace_map,
            collection=self.child_collection,
            columns=(self.child_column,),
            sample_size=meta_id_timeout_sample_size,
            seed=sample_seed,
        )
        metadata: dict[str, Any] = {
            "fallback_stage": fallback_stage,
            "first_error": first_error,
            "sample_size": meta_id_timeout_sample_size,
            "sampled_query": "sdk_sampling(meta_id_reference_exists)",
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
        meta_id_timeout_sample_size: int,
        meta_id_timeout_sample_seed: int,
        sdk_operation_logs: list[dict[str, Any]],
        **_: Any,
    ) -> tuple[Any, str, str | None, dict[str, Any] | None]:
        if has_primary_index(index_map, self.parent_collection):
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
                        meta_id_timeout_sample_size=meta_id_timeout_sample_size,
                        meta_id_timeout_sample_seed=meta_id_timeout_sample_seed,
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
                    "operation_type": "meta_id_reference_exists",
                    "elapsed_ms": elapsed_ms,
                    "success": True,
                    "fallback_reason": "meta_id_check_requires_primary_index",
                    "error": None,
                }
            )
            return fallback_output, "sdk_fallback", None, None
        except Exception as error:
            elapsed_ms = time.perf_counter() * 1000 - started_ms
            sdk_operation_logs.append(
                {
                    "task_id": self.task_id,
                    "operation_type": "meta_id_reference_exists",
                    "elapsed_ms": elapsed_ms,
                    "success": False,
                    "fallback_reason": "meta_id_check_requires_primary_index",
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
                    meta_id_timeout_sample_size=meta_id_timeout_sample_size,
                    meta_id_timeout_sample_seed=meta_id_timeout_sample_seed,
                    fallback_stage="sdk_timeout",
                    first_error=error_text,
                )
                return fallback_output, "sample_fallback", fallback_error, fallback_metadata
            return {"error": error_text}, "sdk_fallback", error_text, None
