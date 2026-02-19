"""
Tools for querying Couchbase documentation and API reference.

This module provides an MCP tool that routes user questions about Couchbase
documentation, SDK usage, configuration, best practices, and API reference
to an agent backend service. The backend determines the appropriate
agent (e.g., RAG-based docs agent, query generation agent) to handle the
request and returns the answer.
"""

import logging
from typing import Annotated, Any

from mcp.server.fastmcp import Context
from pydantic import Field

from utils.constants import MCP_SERVER_NAME
from utils.agent import call_agent, extract_answer

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
                "The user's question about Couchbase — e.g. documentation, "
                "SDK usage, configuration, best practices, SQL++ syntax, "
                "API reference, method signatures, REST endpoints, or "
                "code examples."
            ),
        ),
    ],
) -> str:
    """Search Couchbase documentation and API reference to answer a question.

    Use this tool when the user asks about:
    • Couchbase concepts, architecture, or features
    • SDK usage (Python, Java, Node.js, .NET, Go, etc.)
    • SQL++ (N1QL) syntax, functions, or operators
    • Cluster configuration, tuning, or administration
    • Best practices, design patterns, or troubleshooting
    • REST API or Management API reference
    • Method signatures, parameters, or return types
    • SDK class or interface documentation
    • Code examples for a specific API call

    The question is routed to an agent backend to generate the answer.

    Args:
        question: Natural-language question about Couchbase.

    Returns:
        A detailed answer with optional source references.
    """
    logger.info("Docs search — question: %s", question)

    if not question or not question.strip():
        return "Error: A question is required. Please ask a specific question about Couchbase."

    try:
        resp_body = call_agent(content=question)
    except (ConnectionError, RuntimeError) as exc:
        return f"Error: {exc}"

    answer = extract_answer(resp_body)
    # sources = format_sources(resp_body)

    # Include agent metadata when available for transparency
    #agent_used = resp_body.get("agent_used", "")
    #prefix = f"[Answered by **{agent_used}** agent]\n\n" if agent_used else ""

    return f"{answer}"
