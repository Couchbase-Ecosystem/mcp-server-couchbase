import json
import logging
from pathlib import Path

import click

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.config")


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

    # Try JSON list format first
    if stripped_item.startswith("["):
        try:
            tools_list = json.loads(stripped_item)
            if isinstance(tools_list, list):
                tools = {
                    tool.strip()
                    for tool in tools_list
                    if isinstance(tool, str) and tool.strip()
                }
                logger.debug(f"Parsed disabled tools from JSON: {tools}")
                return tools
        except json.JSONDecodeError:
            logger.warning(
                f"Failed to parse as JSON, treating as comma-separated: {stripped_item}"
            )

    # Check if it's a file path
    potential_path = Path(stripped_item)
    if potential_path.exists() and potential_path.is_file():
        try:
            with open(potential_path) as f:
                file_tools = {
                    line.strip()
                    for line in f.readlines()
                    if line.strip() and not line.strip().startswith("#")
                }
                logger.debug(
                    f"Loaded {len(file_tools)} disabled tools from file: {potential_path}"
                )
                return file_tools
        except Exception as e:
            logger.warning(f"Failed to read disabled tools file {potential_path}: {e}")
            return set()

    # Check if it's comma-separated (contains comma but not a file)
    if "," in stripped_item:
        tools = {tool.strip() for tool in stripped_item.split(",") if tool.strip()}
        logger.debug(f"Parsed disabled tools from comma-separated string: {tools}")
        return tools

    # Single tool name
    return {stripped_item}


def parse_disabled_tools(
    disabled_tools_input: tuple[str, ...] | None = None,
) -> set[str]:
    """
    Parse disabled tools from CLI arguments or environment variable.

    Click handles the environment variable (CB_DISABLED_TOOLS) and passes it
    through the same parameter as CLI arguments.

    Supports multiple input formats:
    1. CLI: Space-separated tool names: --disabled-tools tool1 tool2
    2. CLI: File path containing one tool name per line: --disabled-tools tools.txt
    3. ENV: Comma-separated string: CB_DISABLED_TOOLS="tool1,tool2"
    4. ENV: JSON list: CB_DISABLED_TOOLS='["tool1", "tool2"]'

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
        logger.debug(f"Total disabled tools: {disabled_tools}")

    return disabled_tools


def filter_tools_by_disabled_list(
    all_tools: list,
    disabled_tool_names: set[str],
) -> tuple[list, list[str]]:
    """
    Filter tools list by removing disabled tools.

    Args:
        all_tools: List of tool functions
        disabled_tool_names: Set of tool names to disable

    Returns:
        Tuple of (enabled_tools_list, list_of_disabled_tool_names_found)
    """
    if not disabled_tool_names:
        return all_tools, []

    enabled_tools = []
    actually_disabled = []

    for tool in all_tools:
        tool_name = tool.__name__
        if tool_name in disabled_tool_names:
            actually_disabled.append(tool_name)
            logger.info(f"Tool '{tool_name}' is disabled and will not be loaded")
        else:
            enabled_tools.append(tool)

    # Warn about disabled tools that don't exist
    unknown_tools = disabled_tool_names - set(actually_disabled)
    if unknown_tools:
        logger.warning(
            f"The following disabled tools were not found: {sorted(unknown_tools)}. "
            f"Please verify the tool names are correct."
        )

    return enabled_tools, actually_disabled
