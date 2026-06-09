"""Result-validation evals for the index tools (LLM-as-judge).

Both are faithfulness checks — index state and advisor recommendations are
not seeded ground truth, so the judge verifies the answer is consistent with
the tool output rather than against fixed values.
"""

from __future__ import annotations

import pytest

from accuracy.sdk import ResultCase

from ._harness import assert_result_case


def _build_cases(bucket: str, scope: str, collection: str) -> list[ResultCase]:
    cases: list[ResultCase] = []

    # NOTE: we ask a single, bounded fact ("is there a primary index?") rather
    # than "list every index". A full-bucket index list can be 20+ entries, and
    # an LLM judge is unreliable at verifying exhaustive set-membership over a
    # large unordered list — it randomly mislabels real entries as fabricated.
    # A single-fact question keeps the faithfulness check reliable while still
    # validating that the agent correctly read the tool's index output.
    cases.append(
        ResultCase(
            test_id="list_indexes_faithful",
            prompt=(
                f"Does bucket '{bucket}' have a primary index? Answer yes or no, "
                "and if yes, name one primary index."
            ),
            expectation=(
                "Faithfulness check on a single fact: does a PRIMARY index exist "
                f"for bucket '{bucket}'? In the tool output a primary index has a "
                "definition like 'CREATE PRIMARY INDEX ...' (or isPrimary=true). "
                "PASS if the answer's yes/no matches the tool output: if the tool "
                "output contains any primary index, the answer must say yes and "
                "name a primary index that actually appears in the output; if it "
                "contains none, the answer must say no. FAIL only if the answer "
                "contradicts the tool output or names a primary index not present "
                "in it. The answer need NOT enumerate every index."
            ),
        )
    )

    cases.append(
        ResultCase(
            test_id="index_advisor_faithful",
            prompt=(
                f"Recommend an index for this query in bucket '{bucket}', scope "
                f"'{scope}': SELECT * FROM `{collection}` WHERE country = 'France'"
            ),
            expectation=(
                "Faithfulness check. The answer must reflect the advisor tool's "
                "output — if the tool recommended one or more indexes, the "
                "answer should convey that recommendation (e.g. a CREATE INDEX "
                "on the relevant field); if the tool returned no recommendation, "
                "the answer should say so. FAIL only if the answer fabricates a "
                "recommendation that contradicts the tool output or invents "
                "results the tool did not return."
            ),
        )
    )

    return cases


@pytest.fixture()
def index_cases(test_bucket: str, test_scope: str, test_collection: str):
    return _build_cases(test_bucket, test_scope, test_collection)


INDEX_RESULT_CASE_IDS = [
    "list_indexes_faithful",
    "index_advisor_faithful",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("case_id", INDEX_RESULT_CASE_IDS)
async def test_index_result(
    case_id: str,
    index_cases: list[ResultCase],
    accuracy_client,
    openai_agent,
    judge,
    openai_model: str,
    result_storage,
    accuracy_run_id: str,
    commit_sha: str,
) -> None:
    case = next(c for c in index_cases if c.test_id == case_id)
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
