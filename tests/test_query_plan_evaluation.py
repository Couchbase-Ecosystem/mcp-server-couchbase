"""Unit tests for query plan evaluation helpers."""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tools.query import evaluate_query_plan
from utils.query_utils import extract_plan_from_explain_results


def test_evaluate_query_plan_detects_primary_scan() -> None:
    """Primary scans should be reported as optimization findings."""
    plan = {
        "#operator": "Sequence",
        "~children": [
            {
                "#operator": "PrimaryScan3",
                "index": "def_inventory_landmark_primary",
                "keyspace": "landmark",
            },
            {"#operator": "Fetch", "keyspace": "landmark"},
        ],
    }

    evaluation = evaluate_query_plan(plan)

    assert evaluation["summary"] == "Plan has optimization opportunities."
    issues = {finding["issue"] for finding in evaluation["findings"]}
    assert "primary_index_scan" in issues
    assert "PrimaryScan3" in evaluation["operators"]


def test_evaluate_query_plan_detects_no_obvious_issues() -> None:
    """Secondary-index-only plans without fetch should be marked healthy."""
    plan = {
        "#operator": "Sequence",
        "~children": [
            {
                "#operator": "IndexScan3",
                "index": "idx_users_status",
                "keyspace": "users",
            },
            {"#operator": "InitialProject"},
        ],
    }

    evaluation = evaluate_query_plan(plan)

    assert evaluation["summary"] == "Plan looks healthy for common query-plan checks."
    assert evaluation["indexes_used"] == ["idx_users_status"]
    assert evaluation["findings"][0]["issue"] == "no_obvious_issues"


def test_evaluate_query_plan_handles_missing_plan() -> None:
    """A missing plan should produce an empty, non-failing evaluation payload."""
    evaluation = evaluate_query_plan(None)

    assert evaluation["summary"] == "No query plan found in EXPLAIN output."
    assert evaluation["operators"] == []
    assert evaluation["findings"] == []


def test_extract_plan_from_explain_results_with_documented_shape() -> None:
    """EXPLAIN payload with top-level `plan` should be extracted."""
    plan = {"#operator": "Sequence", "~children": []}
    results = [{"plan": plan, "text": "EXPLAIN"}]

    assert extract_plan_from_explain_results(results) == plan
