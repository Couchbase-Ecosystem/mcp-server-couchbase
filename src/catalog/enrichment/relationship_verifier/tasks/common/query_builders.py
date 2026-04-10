"""Shared SQL++ builders for task execution."""

from __future__ import annotations

from typing import Any

from .runtime import (
    build_from_clause,
    keyspace_expression,
    parse_path,
    query_limit_for_collection,
)


def build_value_set_inclusion_query(
    task: Any,
    *,
    bucket_name: str,
    keyspace_map: dict[str, str],
    index_map: dict[str, list[list[str]]],
    max_unindexed_scan_rows: int,
    child_offset: int = 0,
) -> str:
    child_keyspace = keyspace_expression(bucket_name, task.child_collection, keyspace_map)
    parent_keyspace = keyspace_expression(bucket_name, task.parent_collection, keyspace_map)

    child_paths = [parse_path("child_row", child_column) for child_column in task.child_columns]
    parent_paths = [parse_path("parent_row", parent_column) for parent_column in task.parent_columns]

    child_limit = query_limit_for_collection(
        index_map=index_map,
        collection=task.child_collection,
        columns=task.child_columns,
        max_unindexed_scan_rows=max_unindexed_scan_rows,
    )
    parent_limit = query_limit_for_collection(
        index_map=index_map,
        collection=task.parent_collection,
        columns=task.parent_columns,
        max_unindexed_scan_rows=max_unindexed_scan_rows,
    )

    child_from_clause = build_from_clause(
        child_keyspace,
        "child_row",
        *child_paths,
        limit_rows=child_limit,
        offset_rows=child_offset,
    )
    parent_from_clause = build_from_clause(
        parent_keyspace,
        "parent_row",
        *parent_paths,
        limit_rows=parent_limit,
    )

    child_presence_filters = " AND ".join(
        (
            f"{child_path.column_ref} IS NOT NULL"
            f" AND {child_path.column_ref} IS NOT MISSING"
        )
        for child_path in child_paths
    )
    join_conditions = " AND ".join(
        (f"{parent_path.column_ref} = {child_path.column_ref}")
        for child_path, parent_path in zip(child_paths, parent_paths, strict=True)
    )

    if not child_presence_filters:
        child_presence_filters = "TRUE"
    if not join_conditions:
        join_conditions = "TRUE"

    return (
        "SELECT CASE WHEN EXISTS ("
        "SELECT 1 "
        f"{child_from_clause} "
        f"WHERE {child_presence_filters} "
        "AND NOT EXISTS ("
        "SELECT 1 "
        f"{parent_from_clause} "
        f"WHERE {join_conditions}"
        ")"
        ") THEN 1 ELSE 0 END AS missing_count;"
    )


def build_meta_id_reference_exists_query(
    task: Any,
    *,
    bucket_name: str,
    keyspace_map: dict[str, str],
    index_map: dict[str, list[list[str]]],
    max_unindexed_scan_rows: int,
) -> str:
    child_keyspace = keyspace_expression(bucket_name, task.child_collection, keyspace_map)
    parent_keyspace = keyspace_expression(bucket_name, task.parent_collection, keyspace_map)
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
        "SELECT CASE WHEN EXISTS ("
        "SELECT 1 "
        f"{child_from_clause} "
        f"WHERE {child_expression} IS NOT NULL AND {child_expression} IS NOT MISSING "
        "AND NOT EXISTS ("
        "SELECT 1 "
        f"FROM {parent_keyspace} AS parent_row "
        f"WHERE META(parent_row).id = {child_expression}"
        ")"
        ") THEN 1 ELSE 0 END AS has_missing_reference;"
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
) -> str:
    child_keyspace = keyspace_expression(bucket_name, child_collection, keyspace_map)
    parent_keyspace = keyspace_expression(bucket_name, parent_collection, keyspace_map)
    child_path = parse_path("child_row", child_column)
    parent_path = parse_path("parent_row", parent_column)

    child_limit = query_limit_for_collection(
        index_map=index_map,
        collection=child_collection,
        columns=(child_column,),
        max_unindexed_scan_rows=max_unindexed_scan_rows,
    )
    parent_limit = query_limit_for_collection(
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
