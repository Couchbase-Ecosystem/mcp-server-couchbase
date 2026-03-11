"""
Tests for update_query_function_annotation in tools/query.py.
"""

import sys
from pathlib import Path
from typing import Annotated, get_args, get_origin

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tools.query import run_sql_plus_plus_query, update_query_function_annotation


class TestUpdateQueryFunctionAnnotation:
    """Tests for update_query_function_annotation."""

    def test_enable_query_generation_sets_annotated_type(self):
        """When enabled, the query param should be Annotated[str, Field(...)]."""
        update_query_function_annotation(True)
        annotation = run_sql_plus_plus_query.__annotations__["query"]

        assert get_origin(annotation) is Annotated
        args = get_args(annotation)
        assert args[0] is str
        field_info = args[1]
        assert "generate_or_modify_sql_plus_plus_query" in field_info.description

    def test_disable_query_generation_resets_to_str(self):
        """When disabled, the query param should be plain str."""
        update_query_function_annotation(False)
        annotation = run_sql_plus_plus_query.__annotations__["query"]

        assert annotation is str

    def test_toggle_enabled_then_disabled(self):
        """Toggling from enabled to disabled should reset annotation."""
        update_query_function_annotation(True)
        assert get_origin(run_sql_plus_plus_query.__annotations__["query"]) is Annotated

        update_query_function_annotation(False)
        assert run_sql_plus_plus_query.__annotations__["query"] is str

    def test_toggle_disabled_then_enabled(self):
        """Toggling from disabled to enabled should set Annotated type."""
        update_query_function_annotation(False)
        assert run_sql_plus_plus_query.__annotations__["query"] is str

        update_query_function_annotation(True)
        annotation = run_sql_plus_plus_query.__annotations__["query"]
        assert get_origin(annotation) is Annotated

    def test_idempotent_enable(self):
        """Calling with True multiple times should not change the result."""
        update_query_function_annotation(True)
        first = run_sql_plus_plus_query.__annotations__["query"]

        update_query_function_annotation(True)
        second = run_sql_plus_plus_query.__annotations__["query"]

        assert get_origin(first) is Annotated
        assert get_origin(second) is Annotated

    def test_idempotent_disable(self):
        """Calling with False multiple times should not change the result."""
        update_query_function_annotation(False)
        first = run_sql_plus_plus_query.__annotations__["query"]

        update_query_function_annotation(False)
        second = run_sql_plus_plus_query.__annotations__["query"]

        assert first is str
        assert second is str
