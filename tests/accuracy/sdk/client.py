"""Bridge between the MCP client and the OpenAI tool-calling agent.

Responsibilities:
  1. Hold an active MCP ClientSession connected to the Couchbase MCP server.
  2. Expose MCP tools in OpenAI's ``tools`` schema.
  3. Record every tool call the LLM makes (name + parameters) so the scorer
     can compare them to expected calls.
  4. Allow individual tools to be mocked with canned results — useful for
     destructive tools or when we want to test selection without executing
     the operation.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from mcp import ClientSession

from .types import LLMToolCall

MockedToolFn = Callable[[dict[str, Any]], Awaitable[str] | str]


class AccuracyTestingClient:
    """Wraps an MCP ClientSession and tracks LLM tool calls."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session
        self._mocked_tools: dict[str, MockedToolFn] = {}
        self._tool_calls: list[LLMToolCall] = []

    # ----- tool schema ---------------------------------------------------

    async def openai_tools(self) -> list[dict[str, Any]]:
        """Return MCP tools translated to OpenAI's tool schema."""
        result = await self._session.list_tools()
        tools: list[dict[str, Any]] = []
        for tool in result.tools:
            parameters = tool.inputSchema or {"type": "object", "properties": {}}
            tools.append(
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description or "",
                        "parameters": parameters,
                    },
                }
            )
        return tools

    # ----- tool execution ------------------------------------------------

    async def execute_tool(self, name: str, arguments: dict[str, Any]) -> str:
        """Execute a tool. Records the call, applies mocks if configured.

        Returns a string suitable for handing back to the LLM as the
        ``tool`` message content.
        """
        self._tool_calls.append(
            LLMToolCall(
                tool_call_id=str(uuid.uuid4()),
                tool_name=name,
                parameters=arguments,
            )
        )

        mock = self._mocked_tools.get(name)
        if mock is not None:
            result = mock(arguments)
            if hasattr(result, "__await__"):
                result = await result  # type: ignore[assignment]
            return result if isinstance(result, str) else json.dumps(result)

        response = await self._session.call_tool(name, arguments=arguments)
        return _serialize_mcp_result(response)

    async def call_tool_silent(self, name: str, arguments: dict[str, Any]) -> Any:
        """Invoke an MCP tool *without* recording the call.

        Use this from test setup / teardown so seeding documents or
        deleting test artifacts does not pollute the LLM tool-call log.
        """
        return await self._session.call_tool(name, arguments=arguments)

    # ----- mock / reset helpers -----------------------------------------

    def mock_tools(self, mocks: dict[str, MockedToolFn]) -> None:
        self._mocked_tools = dict(mocks)

    def reset(self) -> None:
        self._mocked_tools = {}
        self._tool_calls = []

    def llm_tool_calls(self) -> list[LLMToolCall]:
        return list(self._tool_calls)


def _serialize_mcp_result(response: Any) -> str:
    """Serialize an MCP CallToolResult into a string for the LLM.

    MCP responses can carry text or structured content across multiple
    blocks. We concatenate text blocks and JSON-encode structured ones.
    """
    is_error = getattr(response, "isError", False) or getattr(
        response, "is_error", False
    )
    parts: list[str] = []
    for block in getattr(response, "content", None) or []:
        text = getattr(block, "text", None)
        if text is not None:
            parts.append(text)
            continue
        data = getattr(block, "data", None)
        if data is not None:
            parts.append(json.dumps(data) if not isinstance(data, str) else data)

    payload = "\n".join(parts) if parts else ""
    if is_error:
        return json.dumps({"isError": True, "content": payload})
    return payload or json.dumps({"ok": True})
