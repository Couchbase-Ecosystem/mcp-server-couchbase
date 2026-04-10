"""Column uniqueness task."""

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
        column_paths = [parse_path("document", column_name) for column_name in self.columns]
        scan_limit = sample_size or query_limit_for_collection(
            index_map=index_map,
            collection=self.collection,
            columns=self.columns,
            max_unindexed_scan_rows=max_unindexed_scan_rows,
        )
        from_clause = build_from_clause(keyspace, "document", *column_paths, limit_rows=scan_limit)
        aliased_columns = [
            (column_path.column_ref, f"group_col_{index}")
            for index, column_path in enumerate(column_paths)
        ]
        group_by_expressions = ", ".join(
            column_expression for column_expression, _ in aliased_columns
        )
        return (
            "SELECT CASE WHEN EXISTS ("
            "SELECT 1 "
            f"{from_clause} "
            f"GROUP BY {group_by_expressions} "
            "HAVING COUNT(*) > 1"
            ") THEN 1 ELSE 0 END AS duplicate_groups;"
        )

    def _can_use_sqlpp_sampling(
        self,
        *,
        index_map: dict[str, list[list[str]]],
    ) -> bool:
        return all(column != META_ID_SENTINEL for column in self.columns) and has_covering_index(
            index_map,
            self.collection,
            self.columns,
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
        rows: list[tuple[Any, ...] | None]
        if sampled:
            if self._can_use_sqlpp_sampling(index_map=index_map):
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
                    return {"duplicate_groups": int(rows[0].get("duplicate_groups", 0))}
            sampled_rows = sample_collection_rows_for_columns(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                collection=self.collection,
                columns=self.columns,
                sample_size=value_set_timeout_sample_size,
                seed=stable_sample_seed(value_set_timeout_sample_seed, self.task_id),
            )
            rows = sampled_rows
        else:
            rows = scan_collection_rows_for_columns(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=keyspace_map,
                collection=self.collection,
                columns=self.columns,
                limit_rows=None,
            )

        seen_rows: set[tuple[Any, ...]] = set()
        for row in rows:
            if row is None:
                continue
            if row in seen_rows:
                return {"duplicate_groups": 1}
            seen_rows.add(row)
        return {"duplicate_groups": 0}

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
        has_index = has_covering_index(index_map, self.collection, self.columns)
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
                            "sampled_query": "sdk_sampling(column_uniqueness)",
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
                        "sampled_query": "sdk_sampling(column_uniqueness)",
                    }
                except Exception as sampled_error:
                    return {"error": str(sampled_error)}, "sample_fallback", str(sampled_error), None
            return {"error": error_text}, "sdk_fallback", error_text, None
