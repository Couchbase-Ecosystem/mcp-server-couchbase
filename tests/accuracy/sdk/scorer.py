"""Accuracy scoring logic.

The scorer reduces a list of expected tool calls and a list of actual tool
calls into a single number in ``{0, 0.75, 1.0}``:

  - 1.0   The LLM called exactly the tools expected, with correct parameters.
  - 0.75  The LLM called the right tools but with extras (extra tool calls
          or extra parameters).
  - 0     A required expected tool call was missing, or a matched call had
          incorrect / missing parameters.

Algorithm:
  1. Start with 1.0; if the LLM made more tool calls than expected, drop to
     0.75 (hallucinated extras).
  2. For each expected tool call, find the best matching actual call. A match
     requires (a) the same tool name and (b) parameter similarity >= 0.75.
  3. If no match is found and the expected call is *required*, return 0.
  4. The final score is the minimum across all matched parameter scores and
     the starting ceiling.
"""

from __future__ import annotations

from .matcher import Matcher
from .types import ExpectedToolCall, LLMToolCall


def calculate_tool_calling_accuracy(
    expected_tool_calls: list[ExpectedToolCall],
    actual_tool_calls: list[LLMToolCall],
) -> float:
    if not expected_tool_calls:
        return 1.0 if not actual_tool_calls else 0.75

    current_score = 0.75 if len(actual_tool_calls) > len(expected_tool_calls) else 1.0
    matched_indexes: set[int] = set()

    for expected in expected_tool_calls:
        candidates: list[tuple[float, int]] = []
        for idx, actual in enumerate(actual_tool_calls):
            if idx in matched_indexes:
                continue
            if actual.tool_name != expected.tool_name:
                continue
            score = Matcher.value(expected.parameters).match(actual.parameters)
            if score >= 0.75:
                candidates.append((score, idx))

        if not candidates:
            if expected.optional:
                continue
            return 0.0

        # Highest score first; tie-break by earliest index (preserve call order)
        candidates.sort(key=lambda pair: (-pair[0], pair[1]))
        best_score, best_idx = candidates[0]
        matched_indexes.add(best_idx)
        current_score = min(current_score, best_score)

    return current_score
