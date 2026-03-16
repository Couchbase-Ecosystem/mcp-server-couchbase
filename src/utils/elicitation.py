"""
Elicitation utilities for MCP tool confirmation.

Provides a wrapper to require user confirmation before executing
high-risk tools, using MCP's elicitation protocol.
"""

import functools
import inspect
import logging
from collections.abc import Callable

from pydantic import BaseModel, Field

from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.elicitation")


class ConfirmationResult(BaseModel):
    """Schema for confirmation elicitation requests."""

    confirm: bool = Field(
        default=True,
        title="Confirm Execution",
        description="Set to true to confirm execution of this tool",
    )


def _build_confirmation_message(tool_name: str, kwargs: dict) -> str:
    """Build a human-readable confirmation message for a tool invocation."""
    # Extract key identifiers from common parameters for a descriptive message
    parts = [f"Do you want to execute '{tool_name}'?"]

    identifiers = []
    for key in ("document_id", "bucket_name", "scope_name", "collection_name"):
        if key in kwargs:
            identifiers.append(f"{key}={kwargs[key]}")

    if identifiers:
        parts.append(f"Parameters: {', '.join(identifiers)}")

    return " ".join(parts)


def wrap_with_confirmation(fn: Callable) -> Callable:
    """Wrap a tool function with elicitation-based confirmation.

    The wrapper checks if the tool is in the confirmation_required_tools
    list in the AppContext. If so, it prompts the user for confirmation
    via MCP elicitation before executing the tool.

    If the client does not support elicitation, the tool executes without
    confirmation to maintain backward compatibility.
    """

    @functools.wraps(fn)
    async def wrapper(**kwargs):
        # Tools in this codebase consistently use `ctx` for FastMCP Context.
        ctx = kwargs.get("ctx")

        if ctx:
            app_context = ctx.request_context.lifespan_context
            tool_name = fn.__name__
            confirmation_tools = getattr(
                app_context, "confirmation_required_tools", set()
            )

            if tool_name in confirmation_tools:
                try:
                    message = _build_confirmation_message(tool_name, kwargs)
                    result = await ctx.elicit(
                        message=message,
                        schema=ConfirmationResult,
                    )

                except Exception as e:
                    # Client may not support elicitation; proceed without confirmation
                    logger.debug(
                        f"Elicitation not available for '{tool_name}', "
                        f"proceeding without confirmation: {e}"
                    )
                else:
                    if result.action in {"decline", "cancel"}:
                        logger.info(f"User {result.action}d execution of '{tool_name}'")
                        raise PermissionError(
                            f"Execution of '{tool_name}' was {result.action}d by the user."
                        )

                    if (
                        result.action == "accept"
                        and hasattr(result, "data")
                        and result.data
                        and not result.data.confirm
                    ):
                        logger.info(f"User did not confirm execution of '{tool_name}'")
                        raise PermissionError(
                            f"Execution of '{tool_name}' was not confirmed by the user."
                        )

                    logger.info(f"User confirmed execution of '{tool_name}'")

        # Call the original (sync) function
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        return fn(**kwargs)

    return wrapper
