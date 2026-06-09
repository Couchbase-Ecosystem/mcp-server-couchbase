"""Result-validation evals for the query-performance tools (LLM-as-judge).

These tools read system:completed_requests, whose contents depend on prior
query history and are non-deterministic. So every case is a faithfulness
check with an explicit "empty is OK" clause: if the tool reports no completed
queries, an answer that says so is correct. The only failure mode is
fabricating query statements or numbers not present in the tool output.
"""

from __future__ import annotations

import pytest

from accuracy.sdk import ResultCase

from ._harness import assert_result_case

# Shared faithfulness rubric for all performance tools.
_FAITHFUL = (
    "Faithfulness check. The performance tools read completed-query history, "
    "which may be empty. PASS if the answer accurately reflects the tool "
    "output: when the tool returns queries/metrics, the answer reports them "
    "without inventing extra ones; when the tool returns no data (or a 'no "
    "completed queries' message), the answer correctly says there is nothing "
    "to report. FAIL ONLY if the answer fabricates query statements or numeric "
    "values that are not present in the tool output."
)


def _build_cases() -> list[ResultCase]:
    prompts = {
        "longest_running": (
            "Which SQL++ queries have been running the longest on average?"
        ),
        "most_frequent": "Which SQL++ queries run most frequently on this cluster?",
        "largest_response_sizes": (
            "Which queries return the largest response payloads in bytes?"
        ),
        "large_result_count": (
            "Which queries return the most documents (largest result counts)?"
        ),
        "using_primary_index": (
            "Which queries are scanning a primary index on this cluster?"
        ),
        "not_using_covering_index": (
            "Which queries scan an index but still fetch documents (not covered)?"
        ),
        "not_selective": (
            "Which queries are non-selective — their index scan reads far more "
            "documents than the result count?"
        ),
    }
    return [
        ResultCase(test_id=test_id, prompt=prompt, expectation=_FAITHFUL)
        for test_id, prompt in prompts.items()
    ]


@pytest.fixture()
def performance_cases():
    return _build_cases()


PERFORMANCE_RESULT_CASE_IDS = [
    "longest_running",
    "most_frequent",
    "largest_response_sizes",
    "large_result_count",
    "using_primary_index",
    "not_using_covering_index",
    "not_selective",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("case_id", PERFORMANCE_RESULT_CASE_IDS)
async def test_performance_result(
    case_id: str,
    performance_cases: list[ResultCase],
    accuracy_client,
    openai_agent,
    judge,
    openai_model: str,
    result_storage,
    accuracy_run_id: str,
    commit_sha: str,
) -> None:
    case = next(c for c in performance_cases if c.test_id == case_id)
    await assert_result_case(
        case,
        accuracy_client=accuracy_client,
        openai_agent=openai_agent,
        judge=judge,
        openai_model=openai_model,
        result_storage=result_storage,
        accuracy_run_id=accuracy_run_id,
        commit_sha=commit_sha,
    )
