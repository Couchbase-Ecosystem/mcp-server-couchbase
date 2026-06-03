"""SDK for AI-in-the-loop accuracy testing of the Couchbase MCP Server."""

from .agent import OpenAIAgent
from .client import AccuracyTestingClient
from .matcher import Matcher
from .result_storage import DiskResultStorage
from .runner import AccuracyCase, AccuracyCaseResult, run_accuracy_case
from .scorer import calculate_tool_calling_accuracy
from .types import ExpectedToolCall, LLMToolCall, ModelResponse, PromptResult

__all__ = [
    "AccuracyCase",
    "AccuracyCaseResult",
    "AccuracyTestingClient",
    "DiskResultStorage",
    "ExpectedToolCall",
    "LLMToolCall",
    "Matcher",
    "ModelResponse",
    "OpenAIAgent",
    "PromptResult",
    "calculate_tool_calling_accuracy",
    "run_accuracy_case",
]
