"""
Tool registration orchestration shared across MCP implementations.
"""

import logging
from collections.abc import Callable

from .tools import get_tools
from .utils.config import parse_tool_names
from .utils.constants import MCP_SERVER_NAME
from .utils.elicitation import wrap_with_confirmation

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tool_registration")


def prepare_tools_for_registration(
    read_only_mode: bool,
    disabled_tools: str | None,
    confirmation_required_tools: str | None,
) -> tuple[list[Callable], set[str], set[str]]:
    """Prepare final tool list and confirmation configuration for registration.

    Loads the shared cb_mcp tools, parses the disabled and confirmation lists,
    filters disabled tools out, and wraps confirmation-required tools with
    elicitation. The same orchestration is reused by every cb_mcp host.
    """
    # When read_only_mode is True, KV write tools are not loaded.
    tools = get_tools(read_only_mode=read_only_mode)

    loaded_tool_names = {tool.__name__ for tool in tools}
    disabled_tool_names = parse_tool_names(disabled_tools, loaded_tool_names)

    if disabled_tool_names:
        logger.info(
            f"Disabled {len(disabled_tool_names)} tool(s): {sorted(disabled_tool_names)}"
        )

    configured_confirmation_tool_names = parse_tool_names(
        confirmation_required_tools, loaded_tool_names
    )

    if configured_confirmation_tool_names:
        logger.info(
            f"Confirmation required for {len(configured_confirmation_tool_names)} tool(s): "
            f"{sorted(configured_confirmation_tool_names)}"
        )

    enabled_tools = [tool for tool in tools if tool.__name__ not in disabled_tool_names]

    # Apply confirmation only to tools that are actually active.
    active_tool_names = {tool.__name__ for tool in enabled_tools}
    active_confirmation_tool_names = (
        configured_confirmation_tool_names & active_tool_names
    )

    skipped_confirmation_tool_names = (
        configured_confirmation_tool_names - active_tool_names
    )
    if skipped_confirmation_tool_names:
        logger.info(
            "Skipped confirmation for unavailable tool(s): "
            f"{sorted(skipped_confirmation_tool_names)}"
        )

    final_tools = [
        (
            wrap_with_confirmation(tool)
            if tool.__name__ in active_confirmation_tool_names
            else tool
        )
        for tool in enabled_tools
    ]

    return final_tools, configured_confirmation_tool_names, disabled_tool_names
