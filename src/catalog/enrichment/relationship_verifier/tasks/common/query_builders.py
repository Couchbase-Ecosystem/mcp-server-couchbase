"""Shared SQL++ builders for task execution."""

from __future__ import annotations

from typing import Any

from catalog.enrichment.relationship_verifier.common.relationships import (
    META_ID_SENTINEL,
)

from .runtime import (
    build_from_clause,
    keyspace_expression,
    parse_path,
    query_limit_for_collection,
    quote_identifier,
)


def _row_projection(paths: list[Any], alias_prefix: str = "col") -> str:
    return ", ".join(
        f"{path.column_ref} AS {quote_identifier(f'{alias_prefix}_{index}')}"
        for index, path in enumerate(paths)
    )


def _presence_filter(paths: list[Any]) -> str:
    return " AND ".join(
        (
            f"{path.column_ref} IS NOT NULL"
            f" AND {path.column_ref} IS NOT MISSING"
        )
        for path in paths
    )


def build_value_set_child_rows_query(
    task: Any,
    *,
    bucket_name: str,
    keyspace_map: dict[str, str],
    index_map: dict[str, list[list[str]]],
    max_unindexed_scan_rows: int,
    child_offset: int = 0,
    sample_size: int | None = None,
) -> str:
    child_keyspace = keyspace_expression(bucket_name, task.child_collection, keyspace_map)
    child_paths = [parse_path("child_row", child_column) for child_column in task.child_columns]
    child_limit = sample_size or query_limit_for_collection(
        index_map=index_map,
        collection=task.child_collection,
        columns=task.child_columns,
        max_unindexed_scan_rows=max_unindexed_scan_rows,
    )

    child_from_clause = build_from_clause(
        child_keyspace,
        "child_row",
        *child_paths,
        limit_rows=child_limit,
        offset_rows=child_offset,
    )
    child_presence_filters = _presence_filter(child_paths) or "TRUE"
    return (
        f"SELECT {_row_projection(child_paths)} "
        f"{child_from_clause} "
        f"WHERE {child_presence_filters};"
    )


def build_value_set_parent_rows_query(
    task: Any,
    *,
    bucket_name: str,
    keyspace_map: dict[str, str],
    index_map: dict[str, list[list[str]]],
    max_unindexed_scan_rows: int,
    sample_size: int | None = None,
) -> str:
    parent_keyspace = keyspace_expression(bucket_name, task.parent_collection, keyspace_map)
    parent_paths = [parse_path("parent_row", parent_column) for parent_column in task.parent_columns]
    parent_limit = sample_size or query_limit_for_collection(
        index_map=index_map,
        collection=task.parent_collection,
        columns=task.parent_columns,
        max_unindexed_scan_rows=max_unindexed_scan_rows,
    )
    parent_from_clause = build_from_clause(
        parent_keyspace,
        "parent_row",
        *parent_paths,
        limit_rows=parent_limit,
    )
    return (
        f"SELECT {_row_projection(parent_paths)} "
        f"{parent_from_clause};"
    )


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_use_keys_parent_lookup_query(
    *,
    bucket_name: str,
    parent_collection: str,
    keyspace_map: dict[str, str],
    keys: list[str],
) -> str:
    parent_keyspace = keyspace_expression(bucket_name, parent_collection, keyspace_map)
    keys_array = ", ".join(_quote_sql_string(key) for key in keys)
    return (
        "SELECT RAW META(parent_row).id "
        f"FROM {parent_keyspace} AS parent_row "
        f"USE KEYS [{keys_array}];"
    )


def build_meta_id_reference_exists_use_keys_query(
    task: Any,
    *,
    bucket_name: str,
    keyspace_map: dict[str, str],
    index_map: dict[str, list[list[str]]],
    max_unindexed_scan_rows: int,
) -> str:
    child_keyspace = keyspace_expression(bucket_name, task.child_collection, keyspace_map)
    child_path = parse_path("child_row", task.child_column)
    child_limit = query_limit_for_collection(
        index_map=index_map,
        collection=task.child_collection,
        columns=(task.child_column,),
        max_unindexed_scan_rows=max_unindexed_scan_rows,
    )
    child_from_clause = build_from_clause(
        child_keyspace,
        "child_row",
        child_path,
        limit_rows=child_limit,
    )
    child_expression = child_path.column_ref
    return (
        "SELECT DISTINCT RAW "
        f"TOSTRING({child_expression}) "
        f"{child_from_clause} "
        f"WHERE {child_expression} IS NOT NULL AND {child_expression} IS NOT MISSING;"
    )


def build_type_compatibility_query(
    *,
    child_collection: str,
    child_column: str,
    parent_collection: str,
    parent_column: str,
    bucket_name: str,
    keyspace_map: dict[str, str],
    index_map: dict[str, list[list[str]]],
    max_unindexed_scan_rows: int,
    sample_size: int | None = None,
) -> str:
    child_keyspace = keyspace_expression(bucket_name, child_collection, keyspace_map)
    parent_keyspace = keyspace_expression(bucket_name, parent_collection, keyspace_map)
    child_path = parse_path("child_row", child_column)
    parent_path = parse_path("parent_row", parent_column)

    child_limit = sample_size or query_limit_for_collection(
        index_map=index_map,
        collection=child_collection,
        columns=(child_column,),
        max_unindexed_scan_rows=max_unindexed_scan_rows,
    )
    parent_limit = sample_size or query_limit_for_collection(
        index_map=index_map,
        collection=parent_collection,
        columns=(parent_column,),
        max_unindexed_scan_rows=max_unindexed_scan_rows,
    )

    child_from_clause = build_from_clause(
        child_keyspace,
        "child_row",
        child_path,
        limit_rows=child_limit,
    )
    parent_from_clause = build_from_clause(
        parent_keyspace,
        "parent_row",
        parent_path,
        limit_rows=parent_limit,
    )
    child_expression = child_path.column_ref
    parent_expression = parent_path.column_ref

    return (
        "SELECT CASE WHEN EXISTS ("
        "SELECT 1 "
        f"{child_from_clause} "
        "WHERE NOT ("
        f'TYPE({child_expression}) IN ["string", "number"] '
        "AND EXISTS ("
        "SELECT 1 "
        f"{parent_from_clause} "
        f"WHERE {parent_expression} IS NOT NULL AND {parent_expression} IS NOT MISSING "
        f'AND TYPE({parent_expression}) IN ["string", "number"]'
        ")"
        ") "
        f"AND TYPE({child_expression}) NOT IN ("
        "SELECT DISTINCT RAW TYPE(" + parent_expression + ") "
        f"{parent_from_clause} "
        f"WHERE {parent_expression} IS NOT NULL AND {parent_expression} IS NOT MISSING"
        ")"
        ") THEN 1 ELSE 0 END AS type_mismatch_count;"
    )


def value_set_parent_is_meta_id(task: Any) -> bool:
    return len(task.parent_columns) == 1 and task.parent_columns[0] == META_ID_SENTINEL
