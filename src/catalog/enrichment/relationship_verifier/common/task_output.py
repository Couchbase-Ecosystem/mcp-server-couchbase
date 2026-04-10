"""Task output parsing helpers."""

from __future__ import annotations

from typing import Any


def get_task_count(  # noqa: PLR0911
    task_outputs: dict[str, Any],
    task_id: str,
    count_key: str,
) -> tuple[int | None, str | None]:
    output_row = task_outputs.get(task_id)
    if output_row is None:
        return None, f"task output missing for task_id={task_id!r}"

    if not isinstance(output_row, dict):
        return (
            None,
            f"unexpected task output type={type(output_row).__name__} for task_id={task_id!r}",
        )

    if "error" in output_row:
        return (
            None,
            f"query execution failed for task_id={task_id!r}: {output_row['error']}",
        )

    if "unavailable" in output_row:
        return (
            None,
            f"task unavailable for task_id={task_id!r}: {output_row['unavailable']}",
        )

    if count_key not in output_row:
        return None, f"count key {count_key!r} not present for task_id={task_id!r}"

    raw_value = output_row.get(count_key)
    if raw_value is None:
        return None, f"count value {count_key!r} is NULL for task_id={task_id!r}"

    try:
        return int(raw_value), None
    except (TypeError, ValueError):
        return (
            None,
            f"count value {count_key!r} not int-castable for task_id={task_id!r}",
        )
