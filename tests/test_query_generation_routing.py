"""Unit tests for bucket-aware query generation."""

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
        None, "show me all users", "b1"  # type: ignore[arg-type]
    )

    assert result["query"] == ""
    assert called["value"] is False
    assert "not been generated yet" in str(result["message"]).lower()


def test_generate_query_requires_bucket_name(monkeypatch) -> None:
    """Return an error when bucket_name is missing."""
    called = {"value": False}

    def _unexpected_call(**_kwargs):
        called["value"] = True
        return {"content": ""}

    monkeypatch.setattr(query_tools, "call_agent", _unexpected_call)

    result = query_tools.generate_or_modify_sql_plus_plus_query(
        None, "find top customers", "   "  # type: ignore[arg-type]
    )

    assert result["query"] == ""
    assert "bucket_name is required" in str(result["message"]).lower()
    assert called["value"] is False


def test_generate_query_returns_error_when_bucket_not_found(monkeypatch) -> None:
    """Return available bucket names when the requested bucket does not exist."""
    monkeypatch.setattr(
        query_tools,
        "_get_bucket_catalog_prompt_states",
        lambda _ctx: {"b1": {"prompt": "p1"}, "b2": {"prompt": "p2"}},
    )

    result = query_tools.generate_or_modify_sql_plus_plus_query(
        None, "find top customers", "missing_bucket"  # type: ignore[arg-type]
    )

    assert result["query"] == ""
    assert result["available_buckets"] == ["b1", "b2"]
    assert "was not found in the catalog" in str(result["message"]).lower()


def test_generate_query_warns_when_bucket_prompt_not_ready(monkeypatch) -> None:
    """Still generate query with warning when selected bucket prompt is missing."""
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
        None, "show me all orders", "b1"  # type: ignore[arg-type]
    )

    assert result["query"] == "SELECT * FROM `users` LIMIT 10"
    assert result["bucket_name"] == "b1"
    assert "may be less accurate" in str(result["message"]).lower()


def test_generate_query_uses_selected_bucket_on_success(monkeypatch) -> None:
    """Use selected bucket name in final response even if query LLM returns another one."""
    captured_call: dict[str, object] = {}

    monkeypatch.setattr(
        query_tools,
        "_get_bucket_catalog_prompt_states",
        lambda _ctx: {"b1": {"prompt": "catalog prompt"}},
    )
    monkeypatch.setattr(
        query_tools,
        "call_agent",
        lambda **kwargs: (
            captured_call.update(kwargs)
            or {
                "content": json.dumps(
                    {
                        "query": "SELECT COUNT(*) AS c FROM `users`",
                        "bucket_name": "wrong_bucket",
                        "scope_name": "_default",
                    }
                )
            }
        ),
    )
    monkeypatch.setattr(
        query_tools, "extract_answer", lambda resp_body: str(resp_body["content"])
    )

    result = query_tools.generate_or_modify_sql_plus_plus_query(
        None, "count users", "b1"  # type: ignore[arg-type]
    )

    assert result["query"] == "SELECT COUNT(*) AS c FROM `users`"
    assert result["bucket_name"] == "b1"
    assert result["scope_name"] == "_default"
    assert captured_call["content"] == "count users"
    assert captured_call["extra_payload"] == {
        "catalog": "catalog prompt",
        "has_schema_access": False,
    }


def test_generate_query_returns_error_for_hitl_response(monkeypatch) -> None:
    """Do not treat HITL get_schema instructions as final SQL++ output."""
    monkeypatch.setattr(
        query_tools,
        "_get_bucket_catalog_prompt_states",
        lambda _ctx: {"b1": {"prompt": "catalog prompt"}},
    )
    monkeypatch.setattr(
        query_tools,
        "call_agent",
        lambda **_kwargs: {
            "content": "Please provide schema.\n```sql\ninfer toy_sales;\n```",
            "is_final_response": False,
            "tool_args": {"tool_name": "get_schema", "collections": ["b1.s1.c1"]},
            "metadata": {"hitl_required": True},
        },
    )
    monkeypatch.setattr(
        query_tools, "extract_answer", lambda resp_body: str(resp_body["content"])
    )

    result = query_tools.generate_or_modify_sql_plus_plus_query(
        None, "count users", "b1"  # type: ignore[arg-type]
    )

    assert result["query"] == ""
    assert "non-final hitl response" in str(result["message"]).lower()


def test_generate_query_does_not_treat_non_final_flag_alone_as_hitl(
    monkeypatch,
) -> None:
    """Allow extraction when response is non-final but not a HITL tool interrupt."""
    monkeypatch.setattr(
        query_tools,
        "_get_bucket_catalog_prompt_states",
        lambda _ctx: {"b1": {"prompt": "catalog prompt"}},
    )
    monkeypatch.setattr(
        query_tools,
        "call_agent",
        lambda **_kwargs: {
            "content": "WITH t AS (SELECT 1 AS x) SELECT x FROM t;",
            "is_final_response": False,
            "metadata": {},
            "tool_args": {},
        },
    )
    monkeypatch.setattr(
        query_tools, "extract_answer", lambda resp_body: str(resp_body["content"])
    )

    result = query_tools.generate_or_modify_sql_plus_plus_query(
        None, "count users", "b1"  # type: ignore[arg-type]
    )

    assert result["query"].startswith("WITH t AS")
