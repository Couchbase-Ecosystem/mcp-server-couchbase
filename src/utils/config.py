import json
import logging
import re
from pathlib import Path

import click

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.config")

# Pattern to validate tool names (lowercase letters and underscores only)
VALID_TOOL_NAME_PATTERN = re.compile(r"^[a-z][a-z_]*$")


def _is_valid_tool_name(name: str) -> bool:
    """
    Validate that a string looks like a valid tool name (Python identifier).

    This validation helps prevent information disclosure from arbitrary file reads
    by ensuring only valid tool names are processed and logged.
    """
    return bool(VALID_TOOL_NAME_PATTERN.match(name))


def _filter_valid_tool_names(names: list[str], source: str) -> set[str]:
    """Filter a list of names, keeping only valid tool names and logging invalid count."""
    valid_tools: set[str] = set()
    invalid_count = 0
    for name in names:
        stripped = name.strip()
        if stripped:
            if _is_valid_tool_name(stripped):
                valid_tools.add(stripped)
            else:
                invalid_count += 1
    if invalid_count > 0:
        logger.warning(f"Ignored {invalid_count} invalid tool name(s) from {source}")
    return valid_tools


def _parse_json_list(json_str: str) -> set[str] | None:
    """Parse a JSON list of tool names. Returns None if not valid JSON list."""
    try:
        tools_list = json.loads(json_str)
        if isinstance(tools_list, list):
            str_items = [t for t in tools_list if isinstance(t, str)]
            tools = _filter_valid_tool_names(str_items, "JSON input")
            logger.debug(f"Parsed disabled tools from JSON: {tools}")
            return tools
    except json.JSONDecodeError:
        logger.warning(
            f"Failed to parse as JSON, treating as comma-separated: {json_str}"
        )
    return None


def _parse_file(file_path: Path) -> set[str] | None:
    """Parse tool names from a file. Returns None if file cannot be read."""
    try:
        with open(file_path) as f:
            lines = [
                line.strip()
                for line in f.readlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            tools = _filter_valid_tool_names(lines, f"file: {file_path}")
            logger.debug(f"Loaded {len(tools)} disabled tools from file: {file_path}")
            return tools
    except OSError as e:
        logger.warning(f"Failed to read disabled tools file {file_path}: {e}")
        return None


def _parse_comma_separated(csv_str: str) -> set[str]:
    """Parse comma-separated tool names."""
    parts = csv_str.split(",")
    tools = _filter_valid_tool_names(parts, "comma-separated input")
    logger.debug(f"Parsed disabled tools from comma-separated string: {tools}")
    return tools


def get_settings() -> dict:
    """Get settings from Click context."""
    ctx = click.get_current_context()
    return ctx.obj or {}


def _parse_single_item(item: str) -> set[str]:
    """
    Parse a single input item which could be:
    - A JSON list: '["tool1", "tool2"]'
    - A comma-separated string: "tool1,tool2"
    - A file path containing tool names
    - A single tool name
    """
    stripped_item = item.strip()
    if not stripped_item:
        return set()

    result: set[str] | None = None

    # Try JSON list format first
    if stripped_item.startswith("["):
        result = _parse_json_list(stripped_item)

    # Check if it's a file path
    if result is None:
        potential_path = Path(stripped_item)
        if potential_path.exists() and potential_path.is_file():
            result = _parse_file(potential_path)
            if result is None:
                result = set()  # File read failed, return empty set

    # Check if it's comma-separated (contains comma but not a file)
    if result is None and "," in stripped_item:
        result = _parse_comma_separated(stripped_item)

    # Single tool name - validate before returning
    if result is None:
        if _is_valid_tool_name(stripped_item):
            result = {stripped_item}
        else:
            logger.warning("Ignored invalid tool name from input")
            result = set()

    return result


def parse_disabled_tools(
    disabled_tools_input: tuple[str, ...] | None = None,
) -> set[str]:
    """
    Parse disabled tools from CLI arguments or environment variable.

    Click handles the environment variable (CB_MCP_DISABLED_TOOLS) and passes it
    through the same parameter as CLI arguments.

    Supports multiple input formats:
    1. CLI: Space-separated tool names: --disabled-tools tool1 tool2
    2. CLI: File path containing one tool name per line: --disabled-tools tools.txt
    3. ENV: Comma-separated string: CB_MCP_DISABLED_TOOLS="tool1,tool2"
    4. ENV: JSON list: CB_MCP_DISABLED_TOOLS='["tool1", "tool2"]'

    Args:
        disabled_tools_input: Tuple of tool names, file path, or formatted string

    Returns:
        Set of tool names to disable
    """
    if not disabled_tools_input:
        return set()

    disabled_tools: set[str] = set()

    for item in disabled_tools_input:
        disabled_tools.update(_parse_single_item(item))

    if disabled_tools:
        logger.debug(f"Disabled tools: {disabled_tools}")

    return disabled_tools
