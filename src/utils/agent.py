"""
Agent backend client.

Provides helpers for communicating with the external agent
(LLM-backed) service. Every MCP tool that needs to reach the
agent backend should go through :func:`call_agent` so that
HTTP calls, error handling, and configuration are centralised in one
place.

Configuration
-------------
The base URL of the agent service defaults to ``http://localhost:8000``
but can be overridden by setting the **CB_AGENT_BASE_URL** environment
variable. When the env var is set its value takes precedence over the
hardcoded default.
"""

import json
import logging
import os
import uuid
from typing import Any

import requests

from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.agent")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_DEFAULT_AGENT_BASE_URL = "https://iq-fastapi.prod.cbdevx.com"
_CHAT_ENDPOINT = "/chat"
_REQUEST_TIMEOUT_SECONDS = 120  # generous timeout for LLM-backed agents


def get_agent_base_url() -> str:
    """Return the resolved agent base URL.

    The **CB_AGENT_BASE_URL** environment variable takes precedence over
    the hardcoded default (``https://iq-fastapi.prod.cbdevx.com``).
    """
    return os.environ.get("CB_AGENT_BASE_URL", _DEFAULT_AGENT_BASE_URL)


def _build_agent_url(base_url: str | None = None) -> str:
    """Build the full URL for the agent chat endpoint."""
    base = base_url or get_agent_base_url()
    return f"{base.rstrip('/')}{_CHAT_ENDPOINT}"


# ---------------------------------------------------------------------------
# Core HTTP helper
# ---------------------------------------------------------------------------

def call_agent(
    *,
    content: str,
    role: str = "mcp",
    thread_id: str | None = None,
    user_id: str = "",
    run_id: str | None = None,
    extra_payload: dict[str, Any] | None = None,
    base_url: str | None = None,
) -> dict[str, Any]:
    """Send a request to the agent backend and return the parsed response.

    This is a shared helper so that every public tool converges on one
    well-tested HTTP call path, with consistent error handling.

    Args:
        content: The text content / question to send.
        role: The role identifier (default ``"mcp"``).
        thread_id: Optional conversation thread ID (auto-generated if *None*).
        user_id: Optional user identifier.
        run_id: Optional run identifier (auto-generated if *None*).
        extra_payload: Extra top-level keys to merge into the request body.
        base_url: Base URL of the agent service.  When *None* the value is
            resolved via :func:`get_agent_base_url` (env → default).

    Returns:
        Parsed JSON response body as a dict.

    Raises:
        ConnectionError: When the agent service is unreachable.
        RuntimeError: For any other HTTP / parsing failure.
    """
    url = _build_agent_url(base_url)
    thread_id = thread_id or str(uuid.uuid4())
    run_id = run_id or str(uuid.uuid4())

    body: dict[str, Any] = {
        "data": {
            "threadId": thread_id,
            "userId": user_id,
            "runId": run_id,
            "content": content,
            "role": role,
        }
    }
    if extra_payload:
        body.update(extra_payload)

    logger.debug("POST %s — thread=%s run=%s", url, thread_id, run_id)

    try:
        response = requests.post(url, json=body, timeout=_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.exceptions.ConnectionError as exc:
        logger.error("Could not connect to agent service at %s", url)
        raise ConnectionError(
            f"Could not connect to the agent service at {url}. "
            "Ensure the service is running."
        ) from exc
    except requests.exceptions.Timeout as exc:
        logger.error("Request to %s timed out after %ss", url, _REQUEST_TIMEOUT_SECONDS)
        raise RuntimeError(
            f"Request to the agent service at {url} timed out after "
            f"{_REQUEST_TIMEOUT_SECONDS}s. The service may be overloaded."
        ) from exc
    except requests.exceptions.RequestException as exc:
        logger.error("Request to agent service failed: %s", exc)
        raise RuntimeError(
            f"Request to the agent service failed: {exc}"
        ) from exc

    try:
        resp_body = response.json()
    except (ValueError, requests.exceptions.JSONDecodeError) as exc:
        logger.error("Invalid JSON from agent service: %s", exc)
        raise RuntimeError(
            f"The agent service returned an invalid JSON response: {exc}"
        ) from exc

    # Surface server-side errors
    if resp_body.get("error"):
        error_msg = str(resp_body["error"])
        logger.error("Agent service returned error: %s", error_msg)
        raise RuntimeError(f"Agent service error: {error_msg}")

    return resp_body


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def extract_answer(resp_body: dict[str, Any]) -> str:
    """Extract the human-readable answer from the agent response.

    The backend may return the answer under different keys
    depending on which agent handled the request. This helper normalises
    the response into a single string.
    """

    # Fallback: query generation agent may populate this
    if resp_body.get("query_generated"):
        return str(resp_body["query_generated"])

    # Preferred key for docs/RAG answers
    if resp_body.get("content"):
        return str(resp_body["content"])

    # Last resort — dump the full body so the caller can debug
    logger.warning("No recognised answer key in response: %s", resp_body)
    return json.dumps(resp_body, indent=2)


# def format_sources(resp_body: dict[str, Any]) -> str:
#     """Format source references returned by the RAG agent, if any."""
#     sources = resp_body.get("sources")
#     if not sources:
#         return ""

#     lines = ["\n\n---\n**Sources:**"]
#     for idx, src in enumerate(sources, 1):
#         title = src.get("title") or src.get("name") or f"Source {idx}"
#         url = src.get("url") or src.get("link", "")
#         snippet = src.get("snippet") or src.get("content", "")
#         if snippet and len(snippet) > 200:
#             snippet = snippet[:200] + "…"
#         entry = f"{idx}. **{title}**"
#         if url:
#             entry += f" — {url}"
#         if snippet:
#             entry += f"\n   > {snippet}"
#         lines.append(entry)

#     return "\n".join(lines)
