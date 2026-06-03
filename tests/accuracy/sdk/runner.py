"""Shared driver that executes one accuracy case end-to-end.

Every tool-family test file builds a list of ``AccuracyCase`` objects and
hands them to ``run_accuracy_case``. The runner:

  1. Opens a fresh MCP session (via the supplied async context-manager
     factory) so anyio task groups enter/exit on the same Task.
  2. Runs optional ``seed`` and (always) ``cleanup`` hooks via
     ``call_tool_silent`` so they don't pollute the LLM tool-call log.
  3. Lists tools, drives the OpenAI agent, captures the LLM tool calls.
  4. Scores the calls and persists the model response to disk.

Tests then assert on the returned score.
"""

from __future__ import annotations

import contextlib
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Any

from .agent import OpenAIAgent
from .client import AccuracyTestingClient, MockedToolFn
from .result_storage import DiskResultStorage
from .scorer import calculate_tool_calling_accuracy
from .types import ExpectedToolCall, ModelResponse, TokensUsed

SetupHook = Callable[[AccuracyTestingClient], Awaitable[None]]


@dataclass
class AccuracyCase:
    """A single accuracy test case, decoupled from any tool family."""

    test_id: str
    prompt: str
    expected_tools: list[ExpectedToolCall]
    seed: SetupHook | None = None
    cleanup: SetupHook | None = None
    mocks: dict[str, MockedToolFn] = field(default_factory=dict)


@dataclass
class AccuracyCaseResult:
    accuracy: float
    actual_calls: list[Any]
    response: ModelResponse


async def run_accuracy_case(
    case: AccuracyCase,
    *,
    accuracy_client_factory: Callable[
        [], AbstractAsyncContextManager[AccuracyTestingClient]
    ],
    openai_agent: OpenAIAgent,
    openai_model: str,
    result_storage: DiskResultStorage,
    accuracy_run_id: str,
    commit_sha: str,
) -> AccuracyCaseResult:
    async with accuracy_client_factory() as client:
        if case.seed is not None:
            await case.seed(client)

        client.reset()
        if case.mocks:
            client.mock_tools(case.mocks)
        tools = await client.openai_tools()

        try:
            agent_result = await openai_agent.run(
                case.prompt,
                tools=tools,
                execute_tool=client.execute_tool,
            )
        finally:
            if case.cleanup is not None:
                # Cleanup is best-effort; we always want to score and store
                # the result even if teardown fails.
                with contextlib.suppress(Exception):
                    await case.cleanup(client)

        actual_calls = client.llm_tool_calls()

    accuracy = calculate_tool_calling_accuracy(case.expected_tools, actual_calls)

    response = ModelResponse(
        provider="OpenAI",
        requested_model=openai_model,
        responding_model=agent_result.responding_model,
        llm_response_time_ms=agent_result.elapsed_ms,
        tool_calling_accuracy=accuracy,
        llm_tool_calls=actual_calls,
        tokens_used=TokensUsed(
            prompt_tokens=agent_result.prompt_tokens,
            completion_tokens=agent_result.completion_tokens,
            total_tokens=agent_result.total_tokens,
        ),
        text=agent_result.text,
        messages=agent_result.messages,
    )

    result_storage.save_model_response(
        run_id=accuracy_run_id,
        commit_sha=commit_sha,
        prompt=case.prompt,
        expected_tool_calls=case.expected_tools,
        model_response=response,
    )

    return AccuracyCaseResult(
        accuracy=accuracy,
        actual_calls=actual_calls,
        response=response,
    )
