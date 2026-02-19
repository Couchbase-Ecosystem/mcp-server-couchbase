"""
Tools for querying Couchbase documentation and API reference.

This module provides an MCP tool that routes user questions about Couchbase
documentation, SDK usage, configuration, best practices, and API reference
to an agent backend service. The backend determines the appropriate
agent (e.g., RAG-based docs agent, query generation agent) to handle the
request and returns the answer.
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
                "A clear, specific natural-language question about Couchbase. — e.g. documentation, "
                "Include relevant keywords such as SDK names, API methods, "
                "SQL++ functions, or configuration parameters to get the "
                "most accurate answer."
            ),
        ),
    ],
) -> str:
    """Search Couchbase documentation and API reference to answer a question.

    Use this tool when the user asks about:
      - Couchbase concepts, architecture, or features
      - SDK usage (Python, Java, Node.js, .NET, Go, etc.)
      - SQL++ (N1QL) syntax, functions, or operators
      - Cluster configuration, tuning, or administration
      - Best practices, design patterns, or troubleshooting
      - REST API / Management API reference
      - Method signatures, parameters, or return types
      - Code examples for a specific API call

    Returns:
        A detailed answer with optional source references.
    """
    logger.info("Docs search — question: %s", question)

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

    answer = extract_answer(resp_body)
    # sources = format_sources(resp_body)

    # Include agent metadata when available for transparency
    #agent_used = resp_body.get("agent_used", "")
    #prefix = f"[Answered by **{agent_used}** agent]\n\n" if agent_used else ""

    return f"{answer}"
