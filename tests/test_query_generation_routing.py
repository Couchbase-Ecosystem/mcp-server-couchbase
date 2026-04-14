"""Unit tests for bucket routing in query generation."""

from __future__ import annotations

import json

from tools import query as query_tools


def test_generate_query_returns_warning_when_catalog_missing(monkeypatch) -> None:
    """Return warning when no bucket catalog state is available."""
    monkeypatch.setattr(query_tools, "_get_bucket_catalog_prompt_states", lambda _ctx: {})
    called = {"value": False}

    def _unexpected_call(**_kwargs):
        called["value"] = True
        return {"content": ""}

    monkeypatch.setattr(query_tools, "call_agent", _unexpected_call)

    result = query_tools.generate_or_modify_sql_plus_plus_query(
        None, "show me all users"  # type: ignore[arg-type]
    )

    assert result["query"] == ""
    assert called["value"] is False
    assert "not been generated yet" in str(result["message"]).lower()


def test_generate_query_returns_candidates_on_low_confidence(monkeypatch) -> None:
    """Return candidate buckets when routing confidence is low."""
    monkeypatch.setattr(
        query_tools,
        "_get_bucket_catalog_prompt_states",
        lambda _ctx: {"b1": {"prompt": "p1"}, "b2": {"prompt": "p2"}},
    )
    monkeypatch.setattr(
        query_tools,
        "_route_question_to_bucket_with_llm",
        lambda _msg, _states: {
            "resolved": False,
            "reason": "low confidence",
            "top_candidates": ["b1", "b2"],
        },
    )

    result = query_tools.generate_or_modify_sql_plus_plus_query(
        None, "find top customers"  # type: ignore[arg-type]
    )

    assert result["query"] == ""
    assert result["candidate_buckets"] == ["b1", "b2"]
    assert "choose a bucket" in str(result["message"]).lower()


def test_generate_query_warns_when_bucket_prompt_not_ready(monkeypatch) -> None:
    """Still generate query with warning when routed bucket prompt is missing."""
    monkeypatch.setattr(
        query_tools,
        "_get_bucket_catalog_prompt_states",
        lambda _ctx: {
            "b1": {
                "prompt": "",
                "summary_line": "b1: scopes=1 (_default), collections=1 (users)",
                "scope_names": ["_default"],
                "collection_names": ["users"],
            }
        },
    )
    monkeypatch.setattr(
        query_tools,
        "_route_question_to_bucket_with_llm",
        lambda _msg, _states: {"resolved": True, "bucket_name": "b1"},
    )
    monkeypatch.setattr(
        query_tools,
        "call_agent",
        lambda **_kwargs: {
            "content": json.dumps(
                {"query": "SELECT * FROM `users` LIMIT 10", "scope_name": "_default"}
            )
        },
    )
    monkeypatch.setattr(
        query_tools, "extract_answer", lambda resp_body: str(resp_body["content"])
    )

    result = query_tools.generate_or_modify_sql_plus_plus_query(
        None, "show me all orders"  # type: ignore[arg-type]
    )

    assert result["query"] == "SELECT * FROM `users` LIMIT 10"
    assert result["bucket_name"] == "b1"
    assert "may be less accurate" in str(result["message"]).lower()


def test_generate_query_uses_routed_bucket_on_success(monkeypatch) -> None:
    """Use routed bucket name in final response even if query LLM returns another one."""
    monkeypatch.setattr(
        query_tools,
        "_get_bucket_catalog_prompt_states",
        lambda _ctx: {"b1": {"prompt": "catalog prompt"}},
    )
    monkeypatch.setattr(
        query_tools,
        "_route_question_to_bucket_with_llm",
        lambda _msg, _states: {"resolved": True, "bucket_name": "b1"},
    )
    monkeypatch.setattr(
        query_tools,
        "call_agent",
        lambda **_kwargs: {
            "content": json.dumps(
                {
                    "query": "SELECT COUNT(*) AS c FROM `users`",
                    "bucket_name": "wrong_bucket",
                    "scope_name": "_default",
                }
            )
        },
    )
    monkeypatch.setattr(
        query_tools, "extract_answer", lambda resp_body: str(resp_body["content"])
    )

    result = query_tools.generate_or_modify_sql_plus_plus_query(
        None, "count users"  # type: ignore[arg-type]
    )

    assert result["query"] == "SELECT COUNT(*) AS c FROM `users`"
    assert result["bucket_name"] == "b1"
    assert result["scope_name"] == "_default"
