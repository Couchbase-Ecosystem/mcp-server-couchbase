"""Unit tests for index tool branches not reached by the live-cluster suite.

Covers:
- get_index_advisor_recommendations empty-result envelope.
- get_index_advisor_recommendations error propagation.
- list_indexes REST-API path with return_raw_index_stats=True.
- list_indexes top-level error propagation.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cb_mcp.tools.index import (
    get_index_advisor_recommendations,
    list_indexes,
)


class TestGetIndexAdvisorRecommendations:
    """Branches of get_index_advisor_recommendations."""

    def test_empty_advisor_response(self) -> None:
        """An empty advisor result must return the documented empty envelope
        rather than raising or returning an unstructured payload."""
        mock_ctx = MagicMock()

        with patch(
            "cb_mcp.tools.index.run_sql_plus_plus_query",
            return_value=[],
        ):
            result = get_index_advisor_recommendations(
                mock_ctx, "b", "s", "SELECT * FROM x"
            )

        assert result == {
            "message": "No recommendations available",
            "current_used_indexes": [],
            "recommended_indexes": [],
            "recommended_covering_indexes": [],
        }

    def test_summary_reflects_recommendation_counts(self) -> None:
        """The summary block must report counts that match the data arrays."""
        mock_ctx = MagicMock()
        advisor_payload = [
            {
                "advisor_result": {
                    "current_used_indexes": [{"index": "CREATE INDEX a"}],
                    "recommended_indexes": [
                        {"index": "CREATE INDEX b"},
                        {"index": "CREATE INDEX c"},
                    ],
                    "recommended_covering_indexes": [{"index": "CREATE INDEX d"}],
                }
            }
        ]

        with patch(
            "cb_mcp.tools.index.run_sql_plus_plus_query",
            return_value=advisor_payload,
        ):
            result = get_index_advisor_recommendations(
                mock_ctx, "b", "s", "SELECT * FROM x"
            )

        assert result["summary"]["current_indexes_count"] == 1
        assert result["summary"]["recommended_indexes_count"] == 2
        assert result["summary"]["recommended_covering_indexes_count"] == 1
        assert result["summary"]["has_recommendations"] is True

    def test_no_recommendations_flag_when_empty(self) -> None:
        """has_recommendations is False when both recommendation arrays are empty."""
        mock_ctx = MagicMock()
        advisor_payload = [
            {
                "advisor_result": {
                    "current_used_indexes": [{"index": "CREATE INDEX a"}],
                    "recommended_indexes": [],
                    "recommended_covering_indexes": [],
                }
            }
        ]

        with patch(
            "cb_mcp.tools.index.run_sql_plus_plus_query",
            return_value=advisor_payload,
        ):
            result = get_index_advisor_recommendations(
                mock_ctx, "b", "s", "SELECT * FROM x"
            )

        assert result["summary"]["has_recommendations"] is False

    def test_error_propagates(self) -> None:
        """Underlying query failures must be re-raised so the caller can
        see the real Couchbase error rather than a fabricated empty result."""
        mock_ctx = MagicMock()

        with patch(
            "cb_mcp.tools.index.run_sql_plus_plus_query",
            side_effect=Exception("syntax error in ADVISOR"),
        ):
            with pytest.raises(Exception, match="syntax error in ADVISOR"):
                get_index_advisor_recommendations(
                    mock_ctx, "b", "s", "SELECT * FROM x"
                )


class TestListIndexesRestRawPath:
    """The REST-API + return_raw_index_stats=True branch."""

    def test_rest_path_returns_raw_rows_unprocessed(self) -> None:
        """On a pre-8 cluster, raw mode must short-circuit before the row
        processor runs — REST rows pass through verbatim."""
        mock_ctx = MagicMock()
        mock_cluster = MagicMock()
        info = MagicMock()
        info.nodes = [{"version": "7.6.11-enterprise"}]
        mock_cluster.cluster_info.return_value = info

        raw_rows = [
            {
                "defnId": 123,
                "indexName": "idx1",
                "definition": "CREATE INDEX idx1 ON b.s.c(x)",
                "status": "Ready",
                "bucket": "b",
                "scope": "s",
                "collection": "c",
                "lastScanTime": "NA",
            }
        ]

        with (
            patch(
                "cb_mcp.tools.index.get_settings",
                return_value={
                    "connection_string": "couchbase://localhost",
                    "username": "u",
                    "password": "p",
                },
            ),
            patch(
                "cb_mcp.tools.index.get_cluster_connection",
                return_value=mock_cluster,
            ),
            patch(
                "cb_mcp.tools.index.fetch_indexes_from_rest_api",
                return_value=raw_rows,
            ),
            patch(
                "cb_mcp.tools.index.process_index_data_from_rest_api"
            ) as mock_process,
        ):
            result = list_indexes(mock_ctx, return_raw_index_stats=True)

        # Raw mode must NOT invoke the processor — that's the whole point.
        mock_process.assert_not_called()
        assert result == raw_rows
        # Defensive: defnId is a raw-only key that should survive.
        assert result[0]["defnId"] == 123


class TestListIndexesErrorPropagation:
    """list_indexes wraps everything in a try/except — verify the re-raise."""

    def test_resolve_version_failure_propagates(self) -> None:
        """If cluster version detection fails, the error must surface so the
        caller can diagnose connectivity rather than seeing an empty list."""
        mock_ctx = MagicMock()

        with (
            patch(
                "cb_mcp.tools.index.get_settings",
                return_value={
                    "connection_string": "couchbase://localhost",
                    "username": "u",
                    "password": "p",
                },
            ),
            patch(
                "cb_mcp.tools.index.get_cluster_connection",
                side_effect=Exception("cluster down"),
            ),
        ):
            with pytest.raises(Exception, match="cluster down"):
                list_indexes(mock_ctx)
