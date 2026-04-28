"""
Unit tests for prepare_tools_for_registration — tool disabling and confirmation wrapping.
"""

from mcp_server import prepare_tools_for_registration


class TestPrepareToolsDisabling:
    """Tests that disabled tools are excluded from the registered tool list."""

    def test_disabled_tool_excluded_from_final_list(self):
        """A named disabled tool should not appear in the returned tool list."""
        tools, _, disabled = prepare_tools_for_registration(
            read_only_mode=True,
            disabled_tools="get_document_by_id",
            confirmation_required_tools=None,
        )
        tool_names = {t.__name__ for t in tools}
        assert "get_document_by_id" not in tool_names
        assert disabled == {"get_document_by_id"}

    def test_non_disabled_tools_remain(self):
        """Tools that are not disabled should still appear in the final list."""
        tools, _, _ = prepare_tools_for_registration(
            read_only_mode=True,
            disabled_tools="get_document_by_id",
            confirmation_required_tools=None,
        )
        tool_names = {t.__name__ for t in tools}
        assert "get_buckets_in_cluster" in tool_names

    def test_no_disabled_tools(self):
        """Passing None for disabled_tools should leave all tools enabled."""
        tools_all, _, disabled = prepare_tools_for_registration(
            read_only_mode=True,
            disabled_tools=None,
            confirmation_required_tools=None,
        )
        assert disabled == set()


class TestPrepareToolsConfirmation:
    """Tests that confirmation-required tools are wrapped correctly."""

    def test_confirmation_tool_is_in_returned_set(self):
        """Specified confirmation tool should appear in the returned confirmed set."""
        _, confirmed, _ = prepare_tools_for_registration(
            read_only_mode=False,
            disabled_tools=None,
            confirmation_required_tools="delete_document_by_id",
        )
        assert "delete_document_by_id" in confirmed

    def test_confirmation_tool_preserves_name(self):
        """Wrapped confirmation tool should retain its original __name__."""
        tools, confirmed, _ = prepare_tools_for_registration(
            read_only_mode=False,
            disabled_tools=None,
            confirmation_required_tools="delete_document_by_id",
        )
        assert "delete_document_by_id" in confirmed
        delete_tool = next(t for t in tools if t.__name__ == "delete_document_by_id")
        assert delete_tool is not None

    def test_unavailable_confirmation_tool_skipped(self):
        """A confirmation tool excluded by read_only_mode should not appear in confirmed set."""
        # delete_document_by_id is a write tool, not loaded in read_only_mode
        _, confirmed, _ = prepare_tools_for_registration(
            read_only_mode=True,
            disabled_tools=None,
            confirmation_required_tools="delete_document_by_id",
        )
        assert "delete_document_by_id" not in confirmed
