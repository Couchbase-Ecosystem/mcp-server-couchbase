"""Parse one-line relationship text into verifier relationship models."""

from __future__ import annotations

import re
from typing import Any

from .relationships import AnyRelationship, relationship_from_dict

__all__ = [
    "parse_relationship_text_to_dicts",
    "parse_relationship_text_to_relationships",
]


def parse_relationship_text_to_relationships(
    relationship_text: str,
) -> list[AnyRelationship]:
    """Parse one-line relationship text into typed relationship models."""
    relationship_dicts = parse_relationship_text_to_dicts(relationship_text)
    return [relationship_from_dict(entry) for entry in relationship_dicts]


def parse_relationship_text_to_dicts(relationship_text: str) -> list[dict[str, Any]]:
    """Parse one-line relationship text into verifier-compatible dictionaries."""
    parsed = _parse_relationship_text(relationship_text)

    foreign_key_map: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for foreign_key in parsed["foreign_keys"]:
        key = (
            _normalize_name(str(foreign_key["parent_table"])),
            _normalize_name(str(foreign_key["child_table"])),
        )
        child_columns = foreign_key["child_columns"]
        parent_columns = foreign_key["parent_columns"]
        pairs = list(zip(child_columns, parent_columns, strict=True))
        foreign_key_map.setdefault(key, []).extend(pairs)

    relationship_entries: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()

    for primary_key in parsed["primary_keys"]:
        pk_entry = {
            "kind": "PK",
            "table": str(primary_key["table_name"]),
            "columns": list(primary_key["columns"]),
        }
        comparison_key = (
            pk_entry["kind"],
            _normalize_name(pk_entry["table"]),
            tuple(_normalize_name(column) for column in pk_entry["columns"]),
        )
        if comparison_key in seen:
            continue
        seen.add(comparison_key)
        relationship_entries.append(pk_entry)

    for primary_key_alternative in parsed["primary_key_alternatives"]:
        pka_entry = {
            "kind": "PKA",
            "table": str(primary_key_alternative["table_name"]),
            "columns": list(primary_key_alternative["columns"]),
        }
        comparison_key = (
            pka_entry["kind"],
            _normalize_name(pka_entry["table"]),
            tuple(_normalize_name(column) for column in pka_entry["columns"]),
        )
        if comparison_key in seen:
            continue
        seen.add(comparison_key)
        relationship_entries.append(pka_entry)

    for foreign_key in parsed["foreign_keys"]:
        fk_entry = {
            "kind": "FK",
            "child_table": str(foreign_key["child_table"]),
            "child_columns": list(foreign_key["child_columns"]),
            "parent_table": str(foreign_key["parent_table"]),
            "parent_columns": list(foreign_key["parent_columns"]),
        }
        comparison_key = (
            fk_entry["kind"],
            _normalize_name(fk_entry["child_table"]),
            tuple(_normalize_name(column) for column in fk_entry["child_columns"]),
            _normalize_name(fk_entry["parent_table"]),
            tuple(_normalize_name(column) for column in fk_entry["parent_columns"]),
        )
        if comparison_key in seen:
            continue
        seen.add(comparison_key)
        relationship_entries.append(fk_entry)

    for relationship in parsed["relationships"]:
        table1 = str(relationship["table1"])
        table2 = str(relationship["table2"])
        fk_pairs = foreign_key_map.get(
            (_normalize_name(table1), _normalize_name(table2)),
            [],
        )
        # Guardrail: inferred OO/OM relationships without FK column pairs cannot
        # be verified with SQL tasks and can generate invalid queries.
        if not fk_pairs:
            continue

        inferred_entry = {
            "kind": str(relationship["kind"]),
            "table1": table1,
            "table2": table2,
            "foreign_key_table": table2,
            "from_columns": [pair[0] for pair in fk_pairs],
            "to_columns": [pair[1] for pair in fk_pairs],
            "connecting_table": None,
        }

        comparison_key = (
            inferred_entry["kind"],
            _normalize_name(inferred_entry["table1"]),
            _normalize_name(inferred_entry["table2"]),
            tuple(_normalize_name(column) for column in inferred_entry["from_columns"]),
            tuple(_normalize_name(column) for column in inferred_entry["to_columns"]),
        )
        if comparison_key in seen:
            continue
        seen.add(comparison_key)
        relationship_entries.append(inferred_entry)

    return relationship_entries


def _parse_relationship_text(relationship_text: str) -> dict[str, Any]:
    normalized_text = _normalize_llm_text(relationship_text)

    primary_keys: list[dict[str, Any]] = []
    primary_key_alternatives: list[dict[str, Any]] = []
    foreign_keys: list[dict[str, Any]] = []
    relationships: list[dict[str, str]] = []

    for primary_key_expression in re.findall(
        r"\bPK\s*\(([^)]*)\)",
        normalized_text,
        flags=re.IGNORECASE,
    ):
        parts = _split_csv_like_args(primary_key_expression)
        if len(parts) < 2:
            continue
        primary_keys.append({"table_name": parts[0], "columns": parts[1:]})

    for primary_key_alternative_expression in re.findall(
        r"\bPKA\s*\(([^)]*)\)",
        normalized_text,
        flags=re.IGNORECASE,
    ):
        parts = _split_csv_like_args(primary_key_alternative_expression)
        if len(parts) < 2:
            continue
        primary_key_alternatives.append({"table_name": parts[0], "columns": parts[1:]})

    for foreign_key_expression in re.findall(
        r"\bFK\s*\(([^)]*)\)",
        normalized_text,
        flags=re.IGNORECASE,
    ):
        parsed_foreign_key = _parse_foreign_key_expression(foreign_key_expression)
        if parsed_foreign_key is None:
            continue
        foreign_keys.append(parsed_foreign_key)

    for kind, args in re.findall(
        r"\b(OO|OM|MM)\s*\(([^)]*)\)",
        normalized_text,
        flags=re.IGNORECASE,
    ):
        relationship_kind = kind.upper()
        if relationship_kind == "MM":
            continue

        parts = _split_csv_like_args(args)
        if len(parts) < 2:
            continue

        relationships.append(
            {
                "kind": relationship_kind,
                "table1": parts[0],
                "table2": parts[1],
            }
        )

    return {
        "primary_keys": primary_keys,
        "primary_key_alternatives": primary_key_alternatives,
        "foreign_keys": foreign_keys,
        "relationships": relationships,
    }


def _parse_foreign_key_expression(expression: str) -> dict[str, Any] | None:
    expression = expression.strip()
    if not expression:
        return None

    if ";" not in expression:
        parts = _split_csv_like_args(expression)
        if len(parts) < 4:
            return None
        return {
            "child_table": parts[0],
            "child_columns": [parts[1]],
            "parent_table": parts[2],
            "parent_columns": [parts[3]],
        }

    left_side, right_side = (part.strip() for part in expression.split(";", maxsplit=1))
    left_parts = _split_csv_like_args(left_side)
    right_parts = _split_csv_like_args(right_side)

    if len(left_parts) < 2 or len(right_parts) < 2:
        return None

    child_columns = left_parts[1:]
    parent_columns = right_parts[1:]
    pair_count = min(len(child_columns), len(parent_columns))

    if pair_count == 0:
        return None

    return {
        "child_table": left_parts[0],
        "child_columns": child_columns[:pair_count],
        "parent_table": right_parts[0],
        "parent_columns": parent_columns[:pair_count],
    }


def _normalize_llm_text(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    fence_match = re.fullmatch(
        r"```(?:text|plaintext)?\s*(.*?)\s*```",
        normalized,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if fence_match:
        normalized = fence_match.group(1).strip()
    # Accept common LLM variants and normalize to verifier sentinel.
    # Examples:
    # - $meta().id
    # - META().id
    # - `meta().id`
    normalized = re.sub(
        r"`?\$?\s*meta\s*\(\s*\)\s*\.\s*id`?",
        "$meta_id",
        normalized,
        flags=re.IGNORECASE,
    )
    return normalized


def _split_csv_like_args(args: str) -> list[str]:
    return [part.strip() for part in args.split(",") if part.strip()]


def _normalize_name(name: str) -> str:
    return name.strip().lower()
