"""Shared driver for result-validation test files.

Each family file (test_kv.py, test_query.py, ...) builds a list of
``ResultCase`` objects and delegates to ``assert_result_case`` so the
run-and-assert boilerplate lives in one place.
"""

from __future__ import annotations

from accuracy.sdk import (
    DiskResultStorage,
    LLMJudge,
    OpenAIAgent,
    ResultCase,
    ResultCaseResult,
    run_result_case,
)


async def assert_result_case(
    case: ResultCase,
    *,
    accuracy_client,
    openai_agent: OpenAIAgent,
    judge: LLMJudge,
    openai_model: str,
    result_storage: DiskResultStorage,
    accuracy_run_id: str,
    commit_sha: str,
) -> ResultCaseResult:
    result = await run_result_case(
        case,
        accuracy_client_factory=accuracy_client,
        openai_agent=openai_agent,
        judge=judge,
        openai_model=openai_model,
        result_storage=result_storage,
        accuracy_run_id=accuracy_run_id,
        commit_sha=commit_sha,
    )

    assert result.verdict.passed, (
        f"Result validation FAILED for case '{case.test_id}'.\n"
        f"  Judge score:     {result.verdict.score}\n"
        f"  Judge reasoning: {result.verdict.reasoning}\n"
        f"  Expectation:     {case.expectation}\n"
        f"  Agent answer:    {result.answer!r}"
    )
    return result
