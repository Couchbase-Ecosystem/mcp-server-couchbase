"""
Compact, deterministic formatting of catalog database_info for LLM prompts.

Goals:
- Replace verbose JSON dump with a token-efficient markdown layout that surfaces
  variants, samples, indexes, and doc_counts explicitly.
- Provide a coverage checklist with stable IDs the LLM must address so the
  enrichment response is parseable and verifiable.
- Provide post-response parsing utilities so missing entries can be detected
  and either re-requested or deterministically backfilled (no data loss).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from typing import Any


def _render_sample(value: Any) -> str:
    """Render a sample value into a single-line string without truncating its content."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int | float)):
        return str(value)
    if isinstance(value, str):
        text = value.replace("|", "/").replace("\n", " ")
        return f'"{text}"'
    return repr(value).replace("|", "/").replace("\n", " ")


def _format_samples(type_map: dict[str, list[Any]]) -> str:
    if not isinstance(type_map, dict):
        return "—"
    parts: list[str] = []
    for type_name in sorted(type_map.keys()):
        samples = type_map.get(type_name) or []
        if not isinstance(samples, list) or not samples:
            continue
        rendered = ", ".join(_render_sample(s) for s in samples)
        if rendered:
            parts.append(f"{type_name}=[{rendered}]")
    return "; ".join(parts) or "—"


def _format_types(type_map: dict[str, list[Any]]) -> str:
    if not isinstance(type_map, dict) or not type_map:
        return "—"
    return "|".join(sorted(type_map.keys()))


def iter_collections(
    database_info: dict[str, Any],
) -> Iterator[tuple[str, str, str, dict[str, Any]]]:
    """Yield (bucket, scope, collection, collection_data) tuples in stable order."""
    buckets = database_info.get("buckets", {})
    if not isinstance(buckets, dict):
        return
    for bucket_name in sorted(buckets.keys()):
        bucket_data = buckets.get(bucket_name) or {}
        if not isinstance(bucket_data, dict):
            continue
        scopes = bucket_data.get("scopes", {})
        if not isinstance(scopes, dict):
            continue
        for scope_name in sorted(scopes.keys()):
            scope_data = scopes.get(scope_name) or {}
            if not isinstance(scope_data, dict):
                continue
            collections = scope_data.get("collections", {})
            if not isinstance(collections, dict):
                continue
            for collection_name in sorted(collections.keys()):
                collection_data = collections.get(collection_name) or {}
                if not isinstance(collection_data, dict):
                    continue
                yield bucket_name, scope_name, collection_name, collection_data


def _merged_collection_paths(
    collection_data: dict[str, Any],
) -> list[tuple[str, dict[str, list[Any]]]]:
    """Merge fields across variants into a deterministic path -> {type: samples} map."""
    merged: dict[str, dict[str, list[Any]]] = {}
    schema_variants = collection_data.get("schema") or []
    if not isinstance(schema_variants, list):
        return []
    for variant in schema_variants:
        if not isinstance(variant, dict):
            continue
        fields = variant.get("fields") or {}
        if not isinstance(fields, dict):
            continue
        for path, type_map in fields.items():
            if not isinstance(type_map, dict):
                continue
            merged_types = merged.setdefault(path, {})
            for type_name, samples in type_map.items():
                merged_samples = merged_types.setdefault(type_name, [])
                if not isinstance(samples, list):
                    continue
                for sample in samples:
                    if sample not in merged_samples:
                        merged_samples.append(sample)
    return sorted(merged.items(), key=lambda item: item[0])


def _format_collection_header(
    bucket_name: str, scope_coll: str, variant_count: int
) -> str:
    return (
        f"### bucket={bucket_name} | collection={scope_coll} "
        f"| variants={variant_count}"
    )


def _format_variant_lines(variants: Any) -> list[str]:
    """Render per-variant lines listing every path (used only for polymorphic collections)."""
    lines: list[str] = []
    if not isinstance(variants, list):
        return lines
    for idx, variant in enumerate(variants):
        if not isinstance(variant, dict):
            continue
        vid = variant.get("variant_id") or f"variant_{idx}"
        vfields = variant.get("fields") or {}
        vpaths = sorted(vfields.keys()) if isinstance(vfields, dict) else []
        listing = ", ".join(f"`{p}`" for p in vpaths) if vpaths else "—"
        lines.append(
            f"- variant `{vid}` (paths={len(vpaths)}): {listing}"
        )
    return lines


def _format_paths_table(
    merged_paths: list[tuple[str, dict[str, list[Any]]]],
) -> list[str]:
    if not merged_paths:
        return ["- (no inferred fields)"]
    rows: list[str] = ["", "| path | types | samples |", "|------|-------|---------|"]
    for path, type_map in merged_paths:
        rows.append(
            f"| `{path}` | {_format_types(type_map)} | {_format_samples(type_map)} |"
        )
    return rows


def _format_indexes_lines(indexes: Any) -> list[str]:
    if not isinstance(indexes, list) or not indexes:
        return []
    lines: list[str] = ["", "Indexes:"]
    for idx_def in indexes:
        if not isinstance(idx_def, dict):
            continue
        name = (idx_def.get("name") or "").strip() or "(unnamed)"
        keys = idx_def.get("index_key") or []
        keys_str = (
            ", ".join(str(k) for k in keys) if isinstance(keys, list) else str(keys)
        )
        definition = (idx_def.get("definition") or "").strip()
        line = f"- `{name}` ON ({keys_str})"
        if definition:
            line += f" -- {definition}"
        lines.append(line)
    return lines


def format_database_info_compact(database_info: dict[str, Any]) -> str:
    """Return compact markdown representation of database_info.

    Format per collection:
        ### bucket=<bucket> | collection=<scope.coll> | variants=N
        - variant `vid` (paths=N): `p1`, `p2`, ...   (only when >1 variant)

        | path | types | samples |
        |------|-------|---------|
        | `path1` | string|number | string=["a","b"]; number=[1] |

        Indexes:
        - `idx_name` ON (key1, key2) -- definition snippet
    """
    sections: list[str] = []
    for bucket_name, scope_name, coll_name, coll_data in iter_collections(database_info):
        scope_coll = f"{scope_name}.{coll_name}"
        variants = coll_data.get("schema") or []
        variant_count = len(variants) if isinstance(variants, list) else 0

        sections.append(
            _format_collection_header(bucket_name, scope_coll, variant_count)
        )
        if variant_count > 1:
            sections.extend(_format_variant_lines(variants))
        sections.extend(_format_paths_table(_merged_collection_paths(coll_data)))
        sections.extend(_format_indexes_lines(coll_data.get("indexes") or []))
        sections.append("")

    return "\n".join(sections).rstrip()


def build_coverage_checklist(database_info: dict[str, Any]) -> list[str]:
    """Build the explicit coverage checklist of stable IDs the LLM must describe.

    ID grammar:
        COLLECTION: <scope>.<collection>
        FIELD: <scope>.<collection> :: <path>
        INDEX: <scope>.<collection> :: <index_name>
    """
    ids: list[str] = []
    for _bucket_name, scope_name, coll_name, coll_data in iter_collections(database_info):
        scope_coll = f"{scope_name}.{coll_name}"
        ids.append(f"COLLECTION: {scope_coll}")
        for path, _ in _merged_collection_paths(coll_data):
            ids.append(f"FIELD: {scope_coll} :: {path}")
        for idx_def in coll_data.get("indexes") or []:
            if not isinstance(idx_def, dict):
                continue
            name = (idx_def.get("name") or "").strip()
            if name:
                ids.append(f"INDEX: {scope_coll} :: {name}")
    return ids


# Lookahead allows trailing punctuation, backticks, or end-of-line after the value.
_FIELD_PATH_TOKEN = r"[A-Za-z0-9_\.\[\]\-]+"
_NAME_TOKEN = r"[A-Za-z0-9_\-]+"
_SCOPE_COLL_TOKEN = r"[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+"

_COLLECTION_RE = re.compile(rf"COLLECTION:\s*`?({_SCOPE_COLL_TOKEN})`?")
_FIELD_RE = re.compile(
    rf"FIELD:\s*`?({_SCOPE_COLL_TOKEN})`?\s*::\s*`?({_FIELD_PATH_TOKEN})`?"
)
_INDEX_RE = re.compile(
    rf"INDEX:\s*`?({_SCOPE_COLL_TOKEN})`?\s*::\s*`?({_NAME_TOKEN})`?"
)


def parse_described_ids(text: str) -> set[str]:
    """Extract checklist IDs that appear in the LLM response."""
    found: set[str] = set()
    if not text:
        return found
    for match in _COLLECTION_RE.finditer(text):
        found.add(f"COLLECTION: {match.group(1).strip()}")
    for match in _FIELD_RE.finditer(text):
        found.add(f"FIELD: {match.group(1).strip()} :: {match.group(2).strip()}")
    for match in _INDEX_RE.finditer(text):
        found.add(f"INDEX: {match.group(1).strip()} :: {match.group(2).strip()}")
    return found


def find_missing_ids(
    expected_ids: Iterable[str], described_ids: Iterable[str]
) -> list[str]:
    """Return expected IDs (in original order) that are not present in described."""
    described_set = set(described_ids)
    return [eid for eid in expected_ids if eid not in described_set]


def _index_database_info(
    database_info: dict[str, Any],
) -> tuple[
    dict[str, dict[str, dict[str, list[Any]]]],
    dict[str, dict[str, dict[str, Any]]],
]:
    """Build lookup tables for stub generation."""
    paths_by_coll: dict[str, dict[str, dict[str, list[Any]]]] = {}
    indexes_by_coll: dict[str, dict[str, dict[str, Any]]] = {}
    for _b, scope_name, coll_name, coll_data in iter_collections(database_info):
        scope_coll = f"{scope_name}.{coll_name}"
        paths_by_coll[scope_coll] = dict(_merged_collection_paths(coll_data))
        indexes: dict[str, dict[str, Any]] = {}
        for idx_def in coll_data.get("indexes") or []:
            if isinstance(idx_def, dict):
                name = (idx_def.get("name") or "").strip()
                if name:
                    indexes[name] = idx_def
        indexes_by_coll[scope_coll] = indexes
    return paths_by_coll, indexes_by_coll


def build_stub_section(
    database_info: dict[str, Any], missing_ids: Iterable[str]
) -> str:
    """Build deterministic stub descriptions for any missing IDs.

    This guarantees the stored prompt never loses schema information even if the
    LLM omits some entries. Stub lines are clearly labeled "(auto)" so callers
    can distinguish LLM-authored from deterministic content.
    """
    missing = list(missing_ids)
    if not missing:
        return ""

    paths_by_coll, indexes_by_coll = _index_database_info(database_info)

    lines: list[str] = [
        "## AUTO-FILLED COVERAGE",
        "The following entries were not described by the LLM and are filled in",
        "deterministically from the schema to guarantee zero data loss.",
        "",
    ]
    for entry in missing:
        if entry.startswith("COLLECTION: "):
            coll = entry[len("COLLECTION: "):].strip()
            lines.append(
                f"- COLLECTION: {coll} -- (auto) Couchbase collection; see field "
                "list for inferred shape."
            )
        elif entry.startswith("FIELD: "):
            body = entry[len("FIELD: "):].strip()
            scope_coll, _, path = body.partition(" :: ")
            type_map = paths_by_coll.get(scope_coll, {}).get(path, {})
            type_str = _format_types(type_map)
            sample_str = _format_samples(type_map)
            lines.append(
                f"- FIELD: {scope_coll} :: {path} -- type={type_str}; "
                f"samples={sample_str}; (auto)"
            )
        elif entry.startswith("INDEX: "):
            body = entry[len("INDEX: "):].strip()
            scope_coll, _, name = body.partition(" :: ")
            idx_def = indexes_by_coll.get(scope_coll, {}).get(name, {})
            keys = idx_def.get("index_key") or []
            if isinstance(keys, list):
                keys_str = ", ".join(str(k) for k in keys)
            else:
                keys_str = str(keys)
            lines.append(
                f"- INDEX: {scope_coll} :: {name} -- ON ({keys_str}); useful "
                "for filtering/joining on these keys; (auto)"
            )
    return "\n".join(lines).rstrip()
