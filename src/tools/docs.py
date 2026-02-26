"""
Tools for querying Couchbase documentation and API reference.

This module provides an MCP tool that routes user questions about Couchbase
documentation, SDK usage, configuration, best practices, and API reference
to an agent backend service.  The backend uses the question text to identify
the relevant product categories and versions, so the question must be
self-contained.
"""

import logging
from typing import Annotated

from mcp.server.fastmcp import Context
from pydantic import Field

from utils.agent import call_agent, extract_answer
from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.docs")


# ---------------------------------------------------------------------------
# Public MCP tool
# ---------------------------------------------------------------------------

def search_couchbase_docs(
    ctx: Context,
    question: Annotated[
        str,
        Field(
            description=(
                "Self-contained question about Couchbase. "
                "MUST preserve every product name and version the user mentioned "
                "(e.g. 'Python SDK 4.3', 'Sync Gateway 3.2 and Couchbase Lite 3.1')."
            ),
        ),
    ],
) -> str:
    """Search Couchbase documentation for any product, SDK, connector, tutorials, or feature."""
    logger.debug("Docs search - question: %s", question)

    cleaned = question.strip() if question else ""
    if not cleaned:
        return (
            "Error: A question is required. "
            "Please ask a specific question about Couchbase."
        )

    try:
        resp_body = call_agent(content=cleaned)
    except (ConnectionError, RuntimeError) as exc:
        logger.error("Agent call failed: %s", exc)
        return f"Error: {exc}"

    return extract_answer(resp_body)
