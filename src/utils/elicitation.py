"""
Elicitation utilities for MCP tool confirmation.

Provides a wrapper to require user confirmation before executing
high-risk tools, using MCP's elicitation protocol.
"""

import functools
import inspect
import logging
from collections.abc import Callable

from mcp import types
from mcp.shared.exceptions import McpError
from pydantic import BaseModel, Field

from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.elicitation")

UNSUPPORTED_ELICITATION_ERROR_CODES = {-32601, -32602}


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


def _client_supports_elicitation(ctx) -> bool:
    """Return True when client explicitly advertises elicitation capability."""
    session = getattr(ctx.request_context, "session", None)
    if session is None or not hasattr(session, "check_client_capability"):
        return False

    return session.check_client_capability(
        types.ClientCapabilities(elicitation=types.ElicitationCapability())
    )


def _is_unsupported_elicitation_error(error: McpError) -> bool:
    """Return True when MCP error indicates client lacks elicitation support."""
    return error.error.code in UNSUPPORTED_ELICITATION_ERROR_CODES


def wrap_with_confirmation(fn: Callable) -> Callable:
    """Wrap a tool function with elicitation-based confirmation.

    The wrapper checks if the tool is in the confirmation_required_tools
    list in the AppContext. If so, it prompts the user for confirmation
    via MCP elicitation before executing the tool.

    If the client does not support elicitation, the tool executes without
    confirmation to maintain backward compatibility.
    """

    fn_signature = inspect.signature(fn)

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        bound_args = fn_signature.bind_partial(*args, **kwargs)
        call_arguments = dict(bound_args.arguments)

        # Tools in this codebase consistently use `ctx` for FastMCP Context.
        ctx = call_arguments.get("ctx")

        if ctx:
            app_context = ctx.request_context.lifespan_context
            tool_name = fn.__name__
            confirmation_tools = getattr(
                app_context, "confirmation_required_tools", set()
            )

            if tool_name in confirmation_tools:
                if not _client_supports_elicitation(ctx):
                    logger.debug(
                        f"Client does not advertise elicitation support for '{tool_name}'; "
                        "proceeding without confirmation"
                    )
                else:
                    try:
                        message = _build_confirmation_message(tool_name, call_arguments)
                        result = await ctx.elicit(
                            message=message,
                            schema=ConfirmationResult,
                        )
                    except McpError as error:
                        if _is_unsupported_elicitation_error(error):
                            logger.debug(
                                f"Client does not support elicitation for '{tool_name}' "
                                f"(code={error.error.code}); proceeding without confirmation"
                            )
                        else:
                            logger.error(
                                f"Elicitation failed for '{tool_name}' with code={error.error.code}; "
                                "blocking execution",
                                exc_info=True,
                            )
                            raise
                    except Exception:
                        logger.error(
                            f"Unexpected elicitation failure for '{tool_name}'; blocking execution",
                            exc_info=True,
                        )
                        raise
                    else:
                        if result.action in {"decline", "cancel"}:
                            action_past_tense = {
                                "decline": "declined",
                                "cancel": "canceled",
                            }[result.action]
                            logger.info(
                                f"User {action_past_tense} execution of '{tool_name}'"
                            )
                            raise PermissionError(
                                f"Execution of '{tool_name}' was {action_past_tense} by the user."
                            )

                        if (
                            result.action == "accept"
                            and hasattr(result, "data")
                            and result.data
                            and not result.data.confirm
                        ):
                            logger.info(
                                f"User did not confirm execution of '{tool_name}'"
                            )
                            raise PermissionError(
                                f"Execution of '{tool_name}' was not confirmed by the user."
                            )

                        logger.info(f"User confirmed execution of '{tool_name}'")

        # Call the original (sync) function
        if inspect.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        return fn(*args, **kwargs)

    return wrapper
