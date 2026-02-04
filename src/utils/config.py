import logging
from pathlib import Path

import click

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.config")


def get_settings() -> dict:
    """Get settings from Click context."""
    ctx = click.get_current_context()
    return ctx.obj or {}


def _parse_file(file_path: Path, valid_tool_names: set[str]) -> set[str]:
    """Parse tool names from a file (one tool per line)."""
    tools: set[str] = set()
    invalid_count = 0
    try:
        with open(file_path) as f:
            for raw_line in f:
                name = raw_line.strip()
                if not name or name.startswith("#"):
                    continue
                if name in valid_tool_names:
                    tools.add(name)
                else:
                    invalid_count += 1
        if invalid_count > 0:
            logger.warning(
                f"Ignored {invalid_count} invalid tool name(s) from file: {file_path}"
            )
        logger.debug(f"Loaded {len(tools)} disabled tools from file: {file_path}")
    except OSError as e:
        logger.warning(f"Failed to read disabled tools file {file_path}: {e}")
    return tools


def _parse_comma_separated(value: str, valid_tool_names: set[str]) -> set[str]:
    """Parse comma-separated tool names."""
    tools: set[str] = set()
    invalid_count = 0
    for part in value.split(","):
        name = part.strip()
        if name:
            if name in valid_tool_names:
                tools.add(name)
            else:
                invalid_count += 1
    if invalid_count > 0:
        logger.warning(
            f"Ignored {invalid_count} invalid tool name(s) from comma-separated input"
        )
    logger.debug(f"Parsed disabled tools from comma-separated string: {tools}")
    return tools


def parse_disabled_tools(
    disabled_tools_input: str | None,
    valid_tool_names: set[str],
) -> set[str]:
    """
    Parse disabled tools from CLI argument or environment variable.

    Supported formats:
    1. Comma-separated string: "tool_1,tool_2"
    2. File path containing one tool name per line: "disabled_tools.txt"

    Args:
        disabled_tools_input: Comma-separated tools or file path
        valid_tool_names: Set of valid tool names to validate against

    Returns:
        Set of tool names to disable
    """
    if not disabled_tools_input:
        return set()

    value = disabled_tools_input.strip()
<<<<<<< DA-1437-mcp-server-readonly-mode-update
    if not value:
        return set()
=======
>>>>>>> main

    # Check if it's a file path
    potential_path = Path(value)
    if potential_path.exists() and potential_path.is_file():
        return _parse_file(potential_path, valid_tool_names)

    # Otherwise, treat as comma-separated
    return _parse_comma_separated(value, valid_tool_names)
