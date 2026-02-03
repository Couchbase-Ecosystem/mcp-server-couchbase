"""
Tests for the disabled tools functionality.
"""

import sys
import tempfile
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from utils.config import parse_disabled_tools

# Sample valid tool names (subset of actual tools)
VALID_TOOL_NAMES = {
    "get_buckets_in_cluster",
    "get_document_by_id",
    "upsert_document_by_id",
    "delete_document_by_id",
    "run_sql_plus_plus_query",
    "list_indexes",
    "get_index_advisor_recommendations",
}


class TestParseDisabledToolsCommaSeparated:
    """Tests for comma-separated input format."""

    def test_single_tool(self):
        """Test parsing a single tool name."""
        result = parse_disabled_tools("get_document_by_id", VALID_TOOL_NAMES)
        assert result == {"get_document_by_id"}

    def test_multiple_tools(self):
        """Test parsing multiple comma-separated tools."""
        result = parse_disabled_tools(
            "get_document_by_id,upsert_document_by_id,delete_document_by_id",
            VALID_TOOL_NAMES,
        )
        assert result == {
            "get_document_by_id",
            "upsert_document_by_id",
            "delete_document_by_id",
        }

    def test_with_spaces(self):
        """Test parsing comma-separated tools with spaces."""
        result = parse_disabled_tools(
            "get_document_by_id, upsert_document_by_id, delete_document_by_id",
            VALID_TOOL_NAMES,
        )
        assert result == {
            "get_document_by_id",
            "upsert_document_by_id",
            "delete_document_by_id",
        }

    def test_invalid_tools_ignored(self):
        """Test that invalid tool names are ignored."""
        result = parse_disabled_tools(
            "get_document_by_id,invalid_tool,another_invalid",
            VALID_TOOL_NAMES,
        )
        assert result == {"get_document_by_id"}

    def test_all_invalid_tools(self):
        """Test that all invalid tools returns empty set."""
        result = parse_disabled_tools(
            "invalid_tool,another_invalid",
            VALID_TOOL_NAMES,
        )
        assert result == set()

    def test_empty_string(self):
        """Test that empty string returns empty set."""
        result = parse_disabled_tools("", VALID_TOOL_NAMES)
        assert result == set()

    def test_none_input(self):
        """Test that None input returns empty set."""
        result = parse_disabled_tools(None, VALID_TOOL_NAMES)
        assert result == set()

    def test_whitespace_only(self):
        """Test that whitespace-only input returns empty set."""
        result = parse_disabled_tools("   ", VALID_TOOL_NAMES)
        assert result == set()

    def test_trailing_comma(self):
        """Test parsing with trailing comma."""
        result = parse_disabled_tools(
            "get_document_by_id,upsert_document_by_id,",
            VALID_TOOL_NAMES,
        )
        assert result == {"get_document_by_id", "upsert_document_by_id"}

    def test_leading_comma(self):
        """Test parsing with leading comma."""
        result = parse_disabled_tools(
            ",get_document_by_id,upsert_document_by_id",
            VALID_TOOL_NAMES,
        )
        assert result == {"get_document_by_id", "upsert_document_by_id"}


class TestParseDisabledToolsFile:
    """Tests for file input format."""

    def test_file_with_valid_tools(self):
        """Test parsing tools from a file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("get_document_by_id\n")
            f.write("upsert_document_by_id\n")
            f.write("delete_document_by_id\n")
            f.flush()

            result = parse_disabled_tools(f.name, VALID_TOOL_NAMES)
            assert result == {
                "get_document_by_id",
                "upsert_document_by_id",
                "delete_document_by_id",
            }

        Path(f.name).unlink()

    def test_file_with_comments(self):
        """Test parsing tools from a file with comments."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("# This is a comment\n")
            f.write("get_document_by_id\n")
            f.write("# Another comment\n")
            f.write("upsert_document_by_id\n")
            f.flush()

            result = parse_disabled_tools(f.name, VALID_TOOL_NAMES)
            assert result == {"get_document_by_id", "upsert_document_by_id"}

        Path(f.name).unlink()

    def test_file_with_empty_lines(self):
        """Test parsing tools from a file with empty lines."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("get_document_by_id\n")
            f.write("\n")
            f.write("   \n")
            f.write("upsert_document_by_id\n")
            f.flush()

            result = parse_disabled_tools(f.name, VALID_TOOL_NAMES)
            assert result == {"get_document_by_id", "upsert_document_by_id"}

        Path(f.name).unlink()

    def test_file_with_invalid_tools(self):
        """Test that invalid tool names in file are ignored."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("get_document_by_id\n")
            f.write("invalid_tool_name\n")
            f.write("upsert_document_by_id\n")
            f.flush()

            result = parse_disabled_tools(f.name, VALID_TOOL_NAMES)
            assert result == {"get_document_by_id", "upsert_document_by_id"}

        Path(f.name).unlink()

    def test_file_with_whitespace_around_names(self):
        """Test parsing tools from a file with whitespace around names."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("  get_document_by_id  \n")
            f.write("\tupsert_document_by_id\t\n")
            f.flush()

            result = parse_disabled_tools(f.name, VALID_TOOL_NAMES)
            assert result == {"get_document_by_id", "upsert_document_by_id"}

        Path(f.name).unlink()

    def test_nonexistent_file_treated_as_tool_name(self):
        """Test that nonexistent file path is treated as comma-separated input."""
        # A path that doesn't exist should be treated as comma-separated
        result = parse_disabled_tools(
            "/nonexistent/path/to/file.txt",
            VALID_TOOL_NAMES,
        )
        # Since the path doesn't match any valid tool, result should be empty
        assert result == set()

    def test_empty_file(self):
        """Test parsing an empty file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.flush()

            result = parse_disabled_tools(f.name, VALID_TOOL_NAMES)
            assert result == set()

        Path(f.name).unlink()

    def test_file_with_only_comments(self):
        """Test parsing a file with only comments."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("# Comment 1\n")
            f.write("# Comment 2\n")
            f.flush()

            result = parse_disabled_tools(f.name, VALID_TOOL_NAMES)
            assert result == set()

        Path(f.name).unlink()


class TestParseDisabledToolsSecurity:
    """Tests for security-related behavior."""

    def test_arbitrary_file_content_not_leaked(self):
        """Test that arbitrary file content is filtered and not leaked."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            # Simulate content from a sensitive file like /etc/passwd
            f.write("root:x:0:0:root:/root:/bin/bash\n")
            f.write("user:x:1000:1000:User:/home/user:/bin/bash\n")
            f.write("get_document_by_id\n")  # One valid tool
            f.flush()

            result = parse_disabled_tools(f.name, VALID_TOOL_NAMES)
            # Only the valid tool name should be in the result
            assert result == {"get_document_by_id"}
            # Sensitive content should not be in the result
            assert "root:x:0:0:root:/root:/bin/bash" not in result

        Path(f.name).unlink()


class TestParseDisabledToolsEdgeCases:
    """Tests for edge cases."""

    def test_duplicate_tools(self):
        """Test that duplicate tool names are deduplicated."""
        result = parse_disabled_tools(
            "get_document_by_id,get_document_by_id,get_document_by_id",
            VALID_TOOL_NAMES,
        )
        assert result == {"get_document_by_id"}
        assert len(result) == 1

    def test_mixed_valid_invalid(self):
        """Test mixed valid and invalid tool names."""
        result = parse_disabled_tools(
            "get_document_by_id,invalid1,upsert_document_by_id,invalid2,delete_document_by_id",
            VALID_TOOL_NAMES,
        )
        assert result == {
            "get_document_by_id",
            "upsert_document_by_id",
            "delete_document_by_id",
        }

    def test_case_sensitive(self):
        """Test that tool names are case-sensitive."""
        result = parse_disabled_tools(
            "GET_DOCUMENT_BY_ID,Get_Document_By_Id",
            VALID_TOOL_NAMES,
        )
        # Uppercase versions should not match
        assert result == set()
