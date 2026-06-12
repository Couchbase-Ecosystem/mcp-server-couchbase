"""
Unit tests for prepare_tools_for_registration — tool disabling and confirmation wrapping.
"""

from cb_mcp.tool_registration import prepare_tools_for_registration


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


class TestDisabledAndConfirmationOverlap:
    """Behavior when a tool is named in BOTH --disabled-tools and
    --confirmation-required-tools.
    """

    def test_disabled_tool_in_confirmation_list_is_dropped(self):
        """A tool that's both disabled and confirmation-required should end
        up disabled (not registered), and the confirmation wrapping should
        be silently skipped — disable wins."""
        tools, confirmed, disabled = prepare_tools_for_registration(
            read_only_mode=False,  # load all tools incl. write tools
            disabled_tools="delete_document_by_id",
            confirmation_required_tools="delete_document_by_id",
        )

        tool_names = {t.__name__ for t in tools}

        # The tool is not registered with the server — disable wins.
        assert "delete_document_by_id" not in tool_names

        # It's still in the user-supplied "configured" confirmation set
        # (we report what the user asked for, not what survived filtering).
        assert "delete_document_by_id" in confirmed

        # And it's in the disabled set.
        assert "delete_document_by_id" in disabled

    def test_disable_only_with_confirmation_on_sibling(self):
        """Disabling one tool while requiring confirmation on a different
        tool must leave the second tool registered AND wrapped — the
        precedence rule applies per-tool, not globally."""
        tools, confirmed, disabled = prepare_tools_for_registration(
            read_only_mode=False,
            disabled_tools="upsert_document_by_id",
            confirmation_required_tools="delete_document_by_id",
        )

        tool_names = {t.__name__ for t in tools}
        assert "upsert_document_by_id" not in tool_names  # disabled
        assert "delete_document_by_id" in tool_names  # still registered
        assert "upsert_document_by_id" in disabled
        assert "delete_document_by_id" in confirmed
