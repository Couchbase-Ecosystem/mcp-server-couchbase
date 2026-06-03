"""Tool-selection accuracy tests.

These tests answer the question: **given a natural, conversational user
prompt, does the LLM pick the right MCP tool from the pool of 24?**

Unlike the per-family files (`test_kv_accuracy.py`, etc.) which verify
both tool name AND parameter extraction, the cases here intentionally
use ``Matcher.any_value()`` for parameters — we only assert on tool
*selection* (intent recognition). The user's intent must be recoverable
from the prompt without naming the tool, but parameter values are not
the signal under test here.

Prompts are written the way a real user would phrase them in chat
("Tell me which queries are taking forever", "How does my data look in
this collection?") rather than as command-style instructions.
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


def _expect(tool_name: str) -> list[ExpectedToolCall]:
    """Tool-selection-only expectation: any params, just the right tool."""
    return [ExpectedToolCall(tool_name=tool_name, parameters=Matcher.any_value())]


def _build_cases(bucket: str, scope: str, collection: str) -> list[AccuracyCase]:
    cases: list[AccuracyCase] = []

    # ----- KV: intent recognition without naming the operation -----------

    cases.append(
        AccuracyCase(
            test_id="kv_lookup_document",
            prompt="I'm looking for a document — can you pull up the one with id 'doc_42'?",
            expected_tools=_expect("get_document_by_id"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="kv_permanently_remove",
            prompt="Permanently get rid of the document 'stale_doc_99'. I don't need it anymore.",
            expected_tools=_expect("delete_document_by_id"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="kv_create_only_if_new",
            prompt=(
                "I want to store a new record under id 'new_user_1' — but only if it "
                "doesn't already exist. Don't overwrite anything."
            ),
            expected_tools=_expect("insert_document_by_id"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="kv_save_existing_or_new",
            prompt=(
                "Save this profile data under id 'profile_x'. I don't care whether it "
                "already exists — just make sure the final value is what I'm giving you."
            ),
            expected_tools=_expect("upsert_document_by_id"),
        )
    )

    # ----- Server / cluster intent ---------------------------------------

    cases.append(
        AccuracyCase(
            test_id="server_what_buckets_do_i_have",
            prompt="What buckets do I actually have on this Couchbase cluster?",
            expected_tools=_expect("get_buckets_in_cluster"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="server_is_everything_healthy",
            prompt=(
                "Is my Couchbase cluster healthy? I want to know whether all the "
                "services are up and reachable."
            ),
            expected_tools=_expect("get_cluster_health_and_services"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="server_show_mcp_config",
            prompt=(
                "What does the MCP server itself look like configuration-wise — "
                "things like read-only mode and which tools are disabled?"
            ),
            expected_tools=_expect("get_server_configuration_status"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="server_can_you_connect",
            prompt=(
                f"Can you actually reach the cluster right now? Just verify the "
                f"connection to bucket '{bucket}' works."
            ),
            expected_tools=_expect("test_cluster_connection"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="server_what_scopes_exist",
            prompt=f"What scopes live inside the '{bucket}' bucket?",
            expected_tools=_expect("get_scopes_in_bucket"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="server_collections_in_scope",
            prompt=(
                f"I need to know what collections sit under scope '{scope}' "
                f"in bucket '{bucket}'."
            ),
            expected_tools=_expect("get_collections_in_scope"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="server_full_data_layout",
            prompt=(
                f"Give me the complete data layout of bucket '{bucket}' — every "
                f"scope and what collections each one holds."
            ),
            expected_tools=_expect("get_scopes_and_collections_in_bucket"),
        )
    )

    # ----- Query intent --------------------------------------------------

    cases.append(
        AccuracyCase(
            test_id="query_what_does_the_data_look_like",
            prompt=(
                f"I'm new to the '{collection}' collection in bucket '{bucket}', "
                f"scope '{scope}'. What does the data look like — what fields do "
                "documents have?"
            ),
            expected_tools=_expect("get_schema_for_collection"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="query_just_run_this",
            prompt=(
                f"Execute this SQL++ in bucket '{bucket}', scope '{scope}' for me: "
                f"SELECT name FROM `{collection}` LIMIT 3"
            ),
            expected_tools=_expect("run_sql_plus_plus_query"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="query_will_this_be_efficient",
            prompt=(
                f"Don't run it — but tell me whether this query would be efficient "
                f"in bucket '{bucket}', scope '{scope}': "
                f"SELECT * FROM `{collection}` WHERE name = 'x'"
            ),
            expected_tools=_expect("explain_sql_plus_plus_query"),
        )
    )

    # ----- Index intent --------------------------------------------------

    cases.append(
        AccuracyCase(
            test_id="index_what_indexes_exist",
            prompt="What indexes does my Couchbase cluster currently have?",
            expected_tools=_expect("list_indexes"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="index_make_this_faster",
            prompt=(
                f"This query feels slow — what index should I create to make it "
                f"faster? Use bucket '{bucket}', scope '{scope}'. Query: "
                f"SELECT * FROM `{collection}` WHERE country = 'US'"
            ),
            expected_tools=_expect("get_index_advisor_recommendations"),
        )
    )

    # ----- Performance intent --------------------------------------------

    cases.append(
        AccuracyCase(
            test_id="perf_slow_queries",
            prompt=(
                "Tell me which SQL++ queries on my cluster are taking forever to "
                "run — I want to know the biggest time hogs."
            ),
            expected_tools=_expect("get_longest_running_queries"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="perf_what_runs_constantly",
            prompt=(
                "Which queries get fired off over and over again? I want to see "
                "the most-executed ones."
            ),
            expected_tools=_expect("get_most_frequent_queries"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="perf_huge_payloads",
            prompt=(
                "Are any of my queries returning huge payloads — like, large in "
                "bytes? I'm worried about network usage."
            ),
            expected_tools=_expect("get_queries_with_largest_response_sizes"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="perf_huge_result_sets",
            prompt=(
                "Which queries are pulling back the most documents (by count, "
                "not size)?"
            ),
            expected_tools=_expect("get_queries_with_large_result_count"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="perf_primary_index_scans",
            prompt=(
                "Are any of my queries falling back to the primary index? That "
                "usually means they need a secondary index."
            ),
            expected_tools=_expect("get_queries_using_primary_index"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="perf_not_covering",
            prompt=(
                "Show me the queries that scan an index but then still have to go "
                "fetch the actual documents — they're not using a covering index."
            ),
            expected_tools=_expect("get_queries_not_using_covering_index"),
        )
    )

    cases.append(
        AccuracyCase(
            test_id="perf_wasteful_scans",
            prompt=(
                "I think some queries are wasteful — their index scans return "
                "way more documents than the queries actually need. Find them."
            ),
            expected_tools=_expect("get_queries_not_selective"),
        )
    )

    return cases


@pytest.fixture()
def tool_selection_cases(test_bucket: str, test_scope: str, test_collection: str):
    return _build_cases(test_bucket, test_scope, test_collection)


TOOL_SELECTION_CASE_IDS = [
    "kv_lookup_document",
    "kv_permanently_remove",
    "kv_create_only_if_new",
    "kv_save_existing_or_new",
    "server_what_buckets_do_i_have",
    "server_is_everything_healthy",
    "server_show_mcp_config",
    "server_can_you_connect",
    "server_what_scopes_exist",
    "server_collections_in_scope",
    "server_full_data_layout",
    "query_what_does_the_data_look_like",
    "query_just_run_this",
    "query_will_this_be_efficient",
    "index_what_indexes_exist",
    "index_make_this_faster",
    "perf_slow_queries",
    "perf_what_runs_constantly",
    "perf_huge_payloads",
    "perf_huge_result_sets",
    "perf_primary_index_scans",
    "perf_not_covering",
    "perf_wasteful_scans",
]


@pytest.mark.accuracy
@pytest.mark.asyncio
@pytest.mark.parametrize("case_id", TOOL_SELECTION_CASE_IDS)
async def test_tool_selection_accuracy(
    case_id: str,
    tool_selection_cases: list[AccuracyCase],
    accuracy_client,
    openai_agent: OpenAIAgent,
    openai_model: str,
    result_storage: DiskResultStorage,
    accuracy_run_id: str,
    commit_sha: str,
) -> None:
    case = next(c for c in tool_selection_cases if c.test_id == case_id)
    result = await run_accuracy_case(
        case,
        accuracy_client_factory=accuracy_client,
        openai_agent=openai_agent,
        openai_model=openai_model,
        result_storage=result_storage,
        accuracy_run_id=accuracy_run_id,
        commit_sha=commit_sha,
    )

    expected_name = case.expected_tools[0].tool_name
    actual_names = [c.tool_name for c in result.actual_calls]

    assert result.accuracy >= 0.75, (
        f"Tool selection failed for case '{case_id}'.\n"
        f"  Expected tool: {expected_name}\n"
        f"  Actual tool calls: {actual_names}\n"
        f"  Score: {result.accuracy}\n"
        f"  Full call detail: "
        f"{json.dumps([c.__dict__ for c in result.actual_calls], indent=2, default=str)}"
    )
