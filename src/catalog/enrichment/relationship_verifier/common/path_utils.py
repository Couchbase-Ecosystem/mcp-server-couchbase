"""Helpers for parsing column paths used in SQL++ query generation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

__all__ = ["ParsedPath", "parse_column_path"]


@dataclass(frozen=True, slots=True)
class ParsedPath:
    """Parsed SQL++ column path with required UNNEST clauses."""

    unnest_clauses: tuple[str, ...]
    column_ref: str


def parse_column_path(
    root_alias: str, path: str, *, quote_fn: Callable[[str], str]
) -> ParsedPath:
    """Parse a dotted path with optional `[]` segments into SQL++ fragments.

    Path rules:
    - `a.b.c` means nested object traversal from `root_alias`.
    - `a.[].c` means `a` is an array and should be unnested before accessing `c`.
    - `a.[].c.[].d` emits multiple `UNNEST` clauses in order.

    The special token `[]` must be a standalone segment and must follow a field segment.
    """
    if not root_alias:
        raise ValueError("root_alias must not be empty.")

    raw_segments = [segment.strip() for segment in path.split(".")]
    segments = [segment for segment in raw_segments if segment]
    if not segments:
        raise ValueError("path must not be empty.")

    current_alias = root_alias
    pending_segment: str | None = None
    remaining_path: list[str] = []
    unnest_clauses: list[str] = []
    unnest_index = 0

    for segment in segments:
        if segment == "[]":
            if pending_segment is None:
                raise ValueError("`[]` must follow a field segment in the column path.")
            array_ref = f"{current_alias}.{quote_fn(pending_segment)}"
            array_alias = f"_arr_{unnest_index}"
            unnest_clauses.append(f"UNNEST {array_ref} AS {array_alias}")
            current_alias = array_alias
            pending_segment = None
            unnest_index += 1
            continue

        if pending_segment is None:
            pending_segment = segment
            continue

        remaining_path.append(pending_segment)
        pending_segment = segment

    if pending_segment is not None:
        remaining_path.append(pending_segment)

    if remaining_path:
        quoted_path = ".".join(quote_fn(segment) for segment in remaining_path)
        column_ref = f"{current_alias}.{quoted_path}"
    else:
        column_ref = current_alias

    return ParsedPath(unnest_clauses=tuple(unnest_clauses), column_ref=column_ref)
