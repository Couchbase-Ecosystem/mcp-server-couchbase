"""Shared data types for accuracy testing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExpectedToolCall:
    """A tool call that the LLM is expected to make.

    ``parameters`` is matched against the actual parameters via the Matcher
    hierarchy: literal values are compared for equality, while Matcher
    instances apply custom logic. Pass a top-level Matcher (e.g.
    ``Matcher.any_value()``) here to ignore parameters entirely — useful
    when you only want to assert which tool was selected.
    """

    tool_name: str
    parameters: Any = field(default_factory=dict)
    optional: bool = False


@dataclass
class LLMToolCall:
    """A tool call that the LLM actually made."""

    tool_call_id: str
    tool_name: str
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass
class TokensUsed:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


@dataclass
class ModelResponse:
    provider: str
    requested_model: str
    responding_model: str
    llm_response_time_ms: float
    tool_calling_accuracy: float
    llm_tool_calls: list[LLMToolCall]
    tokens_used: TokensUsed
    text: str
    messages: list[dict[str, Any]]


@dataclass
class PromptResult:
    prompt: str
    expected_tool_calls: list[ExpectedToolCall]
    model_responses: list[ModelResponse]
