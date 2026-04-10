"""Shared task runtime helpers."""

from __future__ import annotations

import hashlib
from typing import Any

from catalog.enrichment.relationship_verifier.common.path_utils import (
    ParsedPath,
    parse_column_path,
)
from catalog.enrichment.relationship_verifier.common.relationships import (
    META_ID_SENTINEL,
)

TIMEOUT_MARKERS = (
    "timeout",
    "timed out",
    "unambiguous_timeout",
    "timeout 29.5s exceeded",
    "seq_scan.timeout",
    "datastore.seq_scan.timeout",
    'code":1080',
)


def quote_identifier(identifier: str) -> str:
    safe_identifier = identifier.replace("`", "``")
    return f"`{safe_identifier}`"


def normalize_name(name: str) -> str:
    return name.strip().lower()


def normalize_index_token(token: str) -> str:
    normalized = token.strip().lower()
    normalized = normalized.replace("`", "")
    normalized = normalized.replace("[]", "")
    normalized = normalized.replace("(", "")
    normalized = normalized.replace(")", "")
    normalized = normalized.replace(" ", "")
    return normalized


def resolve_collection_keyspace(
    collection_name: str,
    keyspace_map: dict[str, str],
) -> tuple[str, str]:
    raw_name = collection_name.strip()
    identifier_parts = [part.strip() for part in raw_name.split(".") if part.strip()]

    if len(identifier_parts) == 2:
        scope_name, resolved_collection = identifier_parts
        return scope_name, resolved_collection

    if len(identifier_parts) == 3:
        _, scope_name, resolved_collection = identifier_parts
        return scope_name, resolved_collection

    if len(identifier_parts) > 3:
        raise ValueError(f"Invalid qualified collection identifier: {collection_name!r}")

    normalized_name = normalize_name(raw_name)
    mapped_keyspace = keyspace_map.get(normalized_name)
    if mapped_keyspace is None:
        raise ValueError(
            "Collection keyspace not found in bucket keyspace map: "
            f"{collection_name!r}. Provide `scope.collection` for ambiguous names."
        )

    scope_name, resolved_collection = mapped_keyspace.split(".", maxsplit=1)
    return scope_name, resolved_collection


def keyspace_expression(
    bucket_name: str,
    collection_name: str,
    keyspace_map: dict[str, str],
) -> str:
    scope_name, resolved_collection = resolve_collection_keyspace(
        collection_name,
        keyspace_map,
    )
    return (
        f"{quote_identifier(bucket_name)}"
        f".{quote_identifier(scope_name)}"
        f".{quote_identifier(resolved_collection)}"
    )


def parse_path(alias: str, column_name: str) -> ParsedPath:
    if column_name == META_ID_SENTINEL:
        return ParsedPath(unnest_clauses=(), column_ref=f"META({alias}).id")
    return parse_column_path(alias, column_name, quote_fn=quote_identifier)


def build_from_clause(
    keyspace: str,
    root_alias: str,
    *paths: ParsedPath,
    limit_rows: int | None = None,
    offset_rows: int = 0,
) -> str:
    if limit_rows is None:
        from_clause = f"FROM {keyspace} AS {root_alias}"
    else:
        offset_clause = f" OFFSET {offset_rows}" if offset_rows > 0 else ""
        from_clause = (
            "FROM ("
            f"SELECT RAW source_doc FROM {keyspace} AS source_doc LIMIT {limit_rows}{offset_clause}"
            f") AS {root_alias}"
        )

    unique_unnests: list[str] = []
    seen_unnests: set[str] = set()
    for path in paths:
        for unnest_clause in path.unnest_clauses:
            if unnest_clause in seen_unnests:
                continue
            seen_unnests.add(unnest_clause)
            unique_unnests.append(unnest_clause)

    if unique_unnests:
        from_clause = f"{from_clause} {' '.join(unique_unnests)}"
    return from_clause


def has_covering_index(
    index_map: dict[str, list[list[str]]],
    collection: str,
    columns: tuple[str, ...],
) -> bool:
    required_columns = {
        normalize_index_token(column)
        for column in columns
        if normalize_index_token(column)
    }
    if not required_columns:
        return True

    normalized_collection = normalize_name(collection)
    index_keys = index_map.get(normalized_collection, [])
    for index_key in index_keys:
        if not index_key:
            return True
        indexed_columns = {
            normalize_index_token(index_component)
            for index_component in index_key
            if normalize_index_token(index_component)
        }
        if required_columns.issubset(indexed_columns):
            return True
    return False


def has_primary_index(index_map: dict[str, list[list[str]]], collection: str) -> bool:
    normalized_collection = normalize_name(collection)
    index_keys = index_map.get(normalized_collection, [])
    return any(not index_key for index_key in index_keys)


def query_limit_for_collection(
    *,
    index_map: dict[str, list[list[str]]],
    collection: str,
    columns: tuple[str, ...],
    max_unindexed_scan_rows: int,
) -> int | None:
    if has_covering_index(index_map, collection, columns):
        return None
    return max_unindexed_scan_rows


def stable_sample_seed(base_seed: int, task_id: str) -> int:
    hash_source = f"{base_seed}:{task_id}".encode()
    return int(hashlib.md5(hash_source).hexdigest()[:8], 16)


def is_timeout_error(error_text: str) -> bool:
    normalized = error_text.strip().lower()
    return any(marker in normalized for marker in TIMEOUT_MARKERS)


def extract_column_value(
    document_body: Any,
    column: str,
    *,
    document_id: str,
) -> tuple[Any, bool]:
    if column == META_ID_SENTINEL:
        return document_id, True

    current_value: Any = document_body
    for segment in (part.strip() for part in column.split(".")):
        if not segment:
            continue
        if segment == "[]":
            return None, False
        if not isinstance(current_value, dict):
            return None, False
        if segment not in current_value:
            return None, False
        current_value = current_value.get(segment)
    return current_value, True


def scan_collection_rows_for_columns(
    *,
    cb: Any,
    bucket_name: str,
    keyspace_map: dict[str, str],
    collection: str,
    columns: tuple[str, ...],
    limit_rows: int | None,
) -> list[tuple[Any, ...] | None]:
    if any("[]" in column for column in columns):
        raise ValueError("SDK fallback does not support array path columns")

    scope_name, collection_name = resolve_collection_keyspace(collection, keyspace_map)
    scanned_rows = cb.scan_collection_documents(
        bucket_name=bucket_name,
        scope_name=scope_name,
        collection_name=collection_name,
        limit=limit_rows,
    )

    resolved_rows: list[tuple[Any, ...] | None] = []
    for document_id, document_body in scanned_rows:
        row_values: list[Any] = []
        missing_column = False
        for column in columns:
            value, is_present = extract_column_value(
                document_body,
                column,
                document_id=document_id,
            )
            if not is_present:
                missing_column = True
                break
            row_values.append(value)

        resolved_rows.append(None if missing_column else tuple(row_values))
    return resolved_rows


def sample_collection_rows_for_columns(
    *,
    cb: Any,
    bucket_name: str,
    keyspace_map: dict[str, str],
    collection: str,
    columns: tuple[str, ...],
    sample_size: int,
    seed: int,
) -> list[tuple[Any, ...] | None]:
    if any("[]" in column for column in columns):
        raise ValueError("SDK sampling does not support array path columns")

    scope_name, collection_name = resolve_collection_keyspace(collection, keyspace_map)
    sampled_rows = cb.sample_collection_documents(
        bucket_name=bucket_name,
        scope_name=scope_name,
        collection_name=collection_name,
        limit=sample_size,
        seed=seed,
    )

    resolved_rows: list[tuple[Any, ...] | None] = []
    for document_id, document_body in sampled_rows:
        row_values: list[Any] = []
        missing_column = False
        for column in columns:
            value, is_present = extract_column_value(
                document_body,
                column,
                document_id=document_id,
            )
            if not is_present:
                missing_column = True
                break
            row_values.append(value)

        resolved_rows.append(None if missing_column else tuple(row_values))
    return resolved_rows


def python_value_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int | float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object" if isinstance(value, dict) else "unknown"
