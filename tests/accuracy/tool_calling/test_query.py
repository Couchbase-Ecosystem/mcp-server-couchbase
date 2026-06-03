"""Accuracy tests for SQL++ query tools.

Covers:
  - get_schema_for_collection
  - run_sql_plus_plus_query (simple SELECT, parameterised LIMIT, count)
  - explain_sql_plus_plus_query

We keep the query parameter loose (``Matcher.string`` with a sanity-check
predicate) because the LLM may format SQL whitespace differently, alias
columns, or quote identifiers. Tool selection plus the bucket/scope are
the primary signals; the query body is verified semantically with a
substring predicate where it matters.
"""

from __future__ import annotations

import json

import pytest

from accuracy.sdk import (
    AccuracyCase,
    DiskResultStorage,
    Matcher,
    OpenAIAgent,
    run_accuracy_case,
)
from accuracy.sdk.types import ExpectedToolCall


def _contains(*needles: str) -> Matcher:
    """Match a string that contains every (case-insensitive) needle."""
    lowered = [n.lower() for n in needles]
    return Matcher.string(lambda value: all(n in value.lower() for n in lowered))


def _build_cases(bucket: str, scope: str, collection: str) -> list[AccuracyCase]:
    cases: list[AccuracyCase] = []

    cases.append(
        AccuracyCase(
            test_id="get_schema_for_collection",
            prompt=(
                f"What is the schema / document structure of collection "
                f"'{collection}' in scope '{scope}' of bucket '{bucket}'? "
                "Infer it from existing documents."
            ),
            expected_tools=[
                ExpectedToolCall(
                    tool_name="get_schema_for_collection",
                    parameters={
                        "bucket_name": bucket,
                        "scope_name": scope,
                        "collection_name": collection,
                    },
                ),
            ],
        )
    )

    cases.append(
        AccuracyCase(
            test_id="run_sql_plus_plus_query_select_limit",
            prompt=(
                f"Run this SQL++ query in bucket '{bucket}', scope '{scope}': "
                f"SELECT * FROM `{collection}` LIMIT 5"
            ),
            expected_tools=[
                ExpectedToolCall(
                    tool_name="run_sql_plus_plus_query",
                    parameters={
                        "bucket_name": bucket,
                        "scope_name": scope,
                        "query": _contains("select", collection, "limit", "5"),
                    },
                ),
            ],
        )
    )

    cases.append(
        AccuracyCase(
            test_id="run_sql_plus_plus_query_count",
            prompt=(
                f"How many documents are in the '{collection}' collection of "
                f"bucket '{bucket}', scope '{scope}'? Run a SQL++ query that "
                "returns the total count."
            ),
            expected_tools=[
                ExpectedToolCall(
                    tool_name="run_sql_plus_plus_query",
                    parameters={
                        "bucket_name": bucket,
                        "scope_name": scope,
                        "query": _contains("count", collection),
                    },
                ),
            ],
        )
    )

    cases.append(
        AccuracyCase(
            test_id="explain_sql_plus_plus_query",
            prompt=(
                f"Show me the query execution plan for "
                f"SELECT * FROM `{collection}` LIMIT 10 in bucket '{bucket}', "
                f"scope '{scope}'. Use the EXPLAIN tool — do not actually run "
                "the query."
            ),
            expected_tools=[
                ExpectedToolCall(
                    tool_name="explain_sql_plus_plus_query",
                    parameters={
                        "bucket_name": bucket,
                        "scope_name": scope,
                        "query": _contains("select", collection),
                    },
                ),
            ],
        )
    )

    cases.append(
        AccuracyCase(
            test_id="conversational_what_does_the_data_look_like",
            prompt=(
                f"I'm new to the '{collection}' collection in bucket '{bucket}', "
                f"scope '{scope}'. What does the data look like — what fields do "
                "documents have?"
            ),
            expected_tools=[
                ExpectedToolCall(
                    tool_name="get_schema_for_collection",
                    parameters=Matcher.any_value(),
                ),
            ],
        )
    )

    cases.append(
        AccuracyCase(
            test_id="conversational_just_run_this",
            prompt=(
                f"Execute this SQL++ in bucket '{bucket}', scope '{scope}' for me: "
                f"SELECT name FROM `{collection}` LIMIT 3"
            ),
            expected_tools=[
                ExpectedToolCall(
                    tool_name="run_sql_plus_plus_query",
                    parameters=Matcher.any_value(),
                ),
            ],
        )
    )

    return cases


@pytest.fixture()
def query_cases(test_bucket: str, test_scope: str, test_collection: str):
    return _build_cases(test_bucket, test_scope, test_collection)


QUERY_CASE_IDS = [
    "get_schema_for_collection",
    "run_sql_plus_plus_query_select_limit",
    "run_sql_plus_plus_query_count",
    "explain_sql_plus_plus_query",
    "conversational_what_does_the_data_look_like",
    "conversational_just_run_this",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("case_id", QUERY_CASE_IDS)
async def test_query_tool_accuracy(
    case_id: str,
    query_cases: list[AccuracyCase],
    accuracy_client,
    openai_agent: OpenAIAgent,
    openai_model: str,
    result_storage: DiskResultStorage,
    accuracy_run_id: str,
    commit_sha: str,
) -> None:
    case = next(c for c in query_cases if c.test_id == case_id)
    result = await run_accuracy_case(
        case,
        accuracy_client_factory=accuracy_client,
        openai_agent=openai_agent,
        openai_model=openai_model,
        result_storage=result_storage,
        accuracy_run_id=accuracy_run_id,
        commit_sha=commit_sha,
    )

    assert result.accuracy >= 0.75, (
        f"Accuracy for case '{case_id}' was {result.accuracy}. "
        f"Expected: {case.expected_tools}. "
        f"Actual: {json.dumps([c.__dict__ for c in result.actual_calls], indent=2, default=str)}"
    )
