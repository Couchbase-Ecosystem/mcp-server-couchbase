"""Column not object task."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from catalog.enrichment.relationship_verifier.common.relationships import (
    META_ID_SENTINEL,
)

from .common.runtime import (
    build_from_clause,
    has_covering_index,
    is_timeout_error,
    keyspace_expression,
    parse_path,
    query_limit_for_collection,
    sample_collection_rows_for_columns,
    scan_collection_rows_for_columns,
    stable_sample_seed,
)


def _normalize_name(name: str) -> str:
    return name.strip().lower()


@dataclass(frozen=True, slots=True)
class ColumnNotObjectTask:
    collection: str
    column: str
    _task_id: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_task_id", self._build_task_id())

    def _build_task_id(self) -> str:
        return f"column_not_object__{_normalize_name(self.collection)}__{_normalize_name(self.column)}"

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
        keyspace = keyspace_expression(bucket_name, self.collection, keyspace_map)
        column_path = parse_path("document", self.column)
        scan_limit = sample_size or query_limit_for_collection(
            index_map=index_map,
            collection=self.collection,
            columns=(self.column,),
            max_unindexed_scan_rows=max_unindexed_scan_rows,
        )
        from_clause = build_from_clause(
            keyspace,
            "document",
            column_path,
            limit_rows=scan_limit,
        )
        column_expression = column_path.column_ref
        return (
            "SELECT CASE WHEN EXISTS ("
            "SELECT 1 "
            f"{from_clause} "
            f"WHERE IS_OBJECT({column_expression}) OR IS_ARRAY({column_expression})"
            ") THEN 1 ELSE 0 END AS nested_count;"
        )

    def _sdk_fallback(
        self,
        *,
        cb: Any,
        bucket_name: str,
        keyspace_map: dict[str, str],
        index_map: dict[str, list[list[str]]],
        max_unindexed_scan_rows: int,
        value_set_timeout_sample_size: int,
        value_set_timeout_sample_seed: int,
        sampled: bool = False,
    ) -> dict[str, int]:
        if sampled:
            if (
                self.column != META_ID_SENTINEL
                and has_covering_index(index_map, self.collection, (self.column,))
            ):
                rows = cb.run_query(
                    self._build_query(
                        bucket_name=bucket_name,
                        keyspace_map=keyspace_map,
                        index_map=index_map,
                        max_unindexed_scan_rows=max_unindexed_scan_rows,
                        sample_size=value_set_timeout_sample_size,
                    )
                )
                if rows and isinstance(rows[0], dict):
                    return {"nested_count": int(rows[0].get("nested_count", 0))}
            rows = sample_collection_rows_for_columns(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                collection=self.collection,
                columns=(self.column,),
                sample_size=value_set_timeout_sample_size,
                seed=stable_sample_seed(value_set_timeout_sample_seed, self.task_id),
            )
        else:
            rows = scan_collection_rows_for_columns(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                collection=self.collection,
                columns=(self.column,),
                limit_rows=None,
            )

        for row in rows:
            if row is None or not row:
                continue
            value = row[0]
            if isinstance(value, dict | list):
                return {"nested_count": 1}
        return {"nested_count": 0}

    def run(  # noqa: PLR0911
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
        has_index = has_covering_index(index_map, self.collection, (self.column,))
        if has_index:
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
                    try:
                        return self._sdk_fallback(
                            cb=cb,
                            bucket_name=bucket_name,
                            keyspace_map=keyspace_map,
                            index_map=index_map,
                            max_unindexed_scan_rows=max_unindexed_scan_rows,
                            value_set_timeout_sample_size=value_set_timeout_sample_size,
                            value_set_timeout_sample_seed=value_set_timeout_sample_seed,
                            sampled=True,
                        ), "sample_fallback", None, {
                            "fallback_stage": "query_timeout",
                            "first_error": error_text,
                            "sample_size": value_set_timeout_sample_size,
                            "sampled_query": "sdk_sampling(column_not_object)",
                        }
                    except Exception as sampled_error:
                        return {"error": str(sampled_error)}, "sample_fallback", str(sampled_error), None
                return {"error": error_text}, "query", error_text, None

        try:
            return self._sdk_fallback(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                index_map=index_map,
                max_unindexed_scan_rows=max_unindexed_scan_rows,
                value_set_timeout_sample_size=value_set_timeout_sample_size,
                value_set_timeout_sample_seed=value_set_timeout_sample_seed,
            ), "sdk_fallback", None, None
        except Exception as error:
            error_text = str(error)
            if is_timeout_error(error_text):
                try:
                    return self._sdk_fallback(
                        cb=cb,
                        bucket_name=bucket_name,
                        keyspace_map=keyspace_map,
                        index_map=index_map,
                        max_unindexed_scan_rows=max_unindexed_scan_rows,
                        value_set_timeout_sample_size=value_set_timeout_sample_size,
                        value_set_timeout_sample_seed=value_set_timeout_sample_seed,
                        sampled=True,
                    ), "sample_fallback", None, {
                        "fallback_stage": "sdk_timeout",
                        "first_error": error_text,
                        "sample_size": value_set_timeout_sample_size,
                        "sampled_query": "sdk_sampling(column_not_object)",
                    }
                except Exception as sampled_error:
                    return {"error": str(sampled_error)}, "sample_fallback", str(sampled_error), None
            return {"error": error_text}, "sdk_fallback", error_text, None
