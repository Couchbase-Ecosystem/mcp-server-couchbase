"""
Unit tests for utility modules.

Tests for:
- utils/index_utils.py - Index utility functions
- utils/constants.py - Constants validation
- utils/config.py - Configuration functions
- utils/connection.py - Connection functions
- utils/context.py - Context management functions
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cb_mcp.tools.index import (
    fetch_indexes_via_query_service,
    list_indexes,
)
from cb_mcp.utils.config import get_settings
from cb_mcp.utils.connection import connect_to_bucket, connect_to_couchbase_cluster
from cb_mcp.utils.constants import (
    ALLOWED_TRANSPORTS,
    DEFAULT_READ_ONLY_MODE,
    DEFAULT_TRANSPORT,
    MCP_SERVER_NAME,
    NETWORK_TRANSPORTS,
)
from cb_mcp.utils.context import (
    AppContext,
    get_cluster_connection,
)
from cb_mcp.utils.index_utils import (
    _build_query_params,
    _determine_ssl_verification,
    _extract_hosts_from_connection_string,
    clean_index_definition,
    map_rest_status_to_query_state,
    parse_major_version,
    process_index_data_from_query,
    process_index_data_from_rest_api,
    resolve_cluster_major_version,
    validate_connection_settings,
    validate_filter_params,
)
from providers.static import StaticClusterProvider


class TestIndexUtilsFunctions:
    """Unit tests for index_utils.py pure functions."""

    def test_validate_filter_params_valid_all(self) -> None:
        """Validate all filter params provided correctly."""
        # Should not raise
        validate_filter_params(
            bucket_name="bucket",
            scope_name="scope",
            collection_name="collection",
            index_name="index",
        )

    def test_validate_filter_params_valid_bucket_only(self) -> None:
        """Validate bucket-only filter is valid."""
        validate_filter_params(
            bucket_name="bucket",
            scope_name=None,
            collection_name=None,
        )

    def test_validate_filter_params_valid_bucket_scope(self) -> None:
        """Validate bucket+scope filter is valid."""
        validate_filter_params(
            bucket_name="bucket",
            scope_name="scope",
            collection_name=None,
        )

    def test_validate_filter_params_scope_without_bucket(self) -> None:
        """Scope without bucket should raise ValueError."""
        with pytest.raises(ValueError, match="bucket_name is required"):
            validate_filter_params(
                bucket_name=None,
                scope_name="scope",
                collection_name=None,
            )

    def test_validate_filter_params_collection_without_scope(self) -> None:
        """Collection without scope should raise ValueError."""
        with pytest.raises(ValueError, match="bucket_name and scope_name are required"):
            validate_filter_params(
                bucket_name="bucket",
                scope_name=None,
                collection_name="collection",
            )

    def test_validate_filter_params_index_without_collection(self) -> None:
        """Index without collection should raise ValueError."""
        with pytest.raises(ValueError, match="collection_name are required"):
            validate_filter_params(
                bucket_name="bucket",
                scope_name="scope",
                collection_name=None,
                index_name="index",
            )

    def test_validate_connection_settings_valid(self) -> None:
        """Valid connection settings should not raise."""
        settings = {
            "connection_string": "couchbase://localhost",
            "username": "admin",
            "password": "password",
        }
        # Should not raise
        validate_connection_settings(settings)

    def test_validate_connection_settings_missing_password(self) -> None:
        """Missing password should raise ValueError."""
        settings = {
            "connection_string": "couchbase://localhost",
            "username": "admin",
        }
        with pytest.raises(ValueError, match="password"):
            validate_connection_settings(settings)

    def test_validate_connection_settings_empty(self) -> None:
        """Empty settings should raise ValueError."""
        with pytest.raises(ValueError, match="connection_string"):
            validate_connection_settings({})

    def test_clean_index_definition_with_quotes(self) -> None:
        """Clean index definition with surrounding quotes."""
        definition = '"CREATE INDEX idx ON bucket(field)"'
        result = clean_index_definition(definition)
        assert result == "CREATE INDEX idx ON bucket(field)"

    def test_clean_index_definition_with_escaped_quotes(self) -> None:
        """Clean index definition with escaped quotes."""
        definition = 'CREATE INDEX idx ON bucket(\\"field\\")'
        result = clean_index_definition(definition)
        assert result == 'CREATE INDEX idx ON bucket("field")'

    def test_clean_index_definition_empty(self) -> None:
        """Clean empty definition returns empty string."""
        assert clean_index_definition("") == ""
        assert clean_index_definition(None) == ""

    def test_process_index_data_basic(self) -> None:
        """Process basic index data."""
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "status": "Ready",
            "bucket": "travel-sample",
            "scope": "_default",
            "collection": "_default",
            "lastScanTime": "NA",
        }
        result = process_index_data_from_rest_api(idx)

        assert result is not None
        assert result["name"] == "idx_test"
        assert result["bucket"] == "travel-sample"
        assert result["status"] == "online"
        assert result["isPrimary"] is False
        assert "lastScanTime" in result

    def test_process_index_data_with_last_scan_time(self) -> None:
        """Process index data includes lastScanTime."""
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "status": "Ready",
            "bucket": "bucket",
            "scope": "scope",
            "collection": "collection",
            "lastScanTime": "Thu Feb 26 13:12:55 IST 2026",
            "extra_field": "some_value",
        }
        result = process_index_data_from_rest_api(idx)

        assert result is not None
        assert result["lastScanTime"] == "Thu Feb 26 13:12:55 IST 2026"
        assert "extra_field" not in result

    def test_process_index_data_with_raw_stats(self) -> None:
        """return_raw_index_stats=True should return the raw row unprocessed."""
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "status": "Ready",
            "bucket": "bucket",
            "scope": "scope",
            "collection": "collection",
            "extra_field": "some_value",
        }
        result = process_index_data_from_rest_api(idx, return_raw_index_stats=True)

        # Returned value IS the input row — no copy, no field rewrites.
        assert result is idx
        # Raw fields are still present (e.g. unmodified status casing).
        assert result["status"] == "Ready"
        assert result["extra_field"] == "some_value"

    def test_process_index_data_without_raw_stats(self) -> None:
        """Process index data without raw stats by default."""
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "status": "Ready",
            "bucket": "bucket",
            "lastScanTime": "NA",
        }
        result = process_index_data_from_rest_api(idx)

        assert result is not None
        assert "raw_index_stats" not in result

    def test_rest_missing_name_falls_back_to_raw(self) -> None:
        """Missing 'name' field should return raw fallback with error message."""
        idx = {"status": "Ready", "bucket": "bucket"}
        result = process_index_data_from_rest_api(idx)
        assert result == {
            "error": result["error"],
            "raw_index_stats": idx,
        }
        assert "name" in result["error"]
        # Raw stats must be the unmodified original input.
        assert result["raw_index_stats"] is idx

    def test_rest_missing_definition_falls_back_to_raw(self) -> None:
        """Missing 'definition' field should return raw fallback with error message."""
        idx = {"name": "idx_test", "status": "Ready", "bucket": "bucket"}
        result = process_index_data_from_rest_api(idx)
        assert "error" in result
        assert "definition" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_rest_missing_bucket_falls_back_to_raw(self) -> None:
        """Missing 'bucket' field should return raw fallback. REST always
        emits bucket today, so its absence indicates a problem in fetching
        the index information."""
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "status": "Ready",
        }
        result = process_index_data_from_rest_api(idx)
        assert "error" in result
        assert "bucket" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_process_index_data_primary_index(self) -> None:
        """Process primary index data."""
        idx = {
            "name": "#primary",
            "definition": "CREATE PRIMARY INDEX `#primary` ON `bucket`",
            "status": "Ready",
            "isPrimary": True,
            "bucket": "bucket",
            "lastScanTime": "NA",
        }
        result = process_index_data_from_rest_api(idx)

        assert result is not None
        assert result["isPrimary"] is True

    def test_extract_hosts_single_host(self) -> None:
        """Extract single host from connection string."""
        conn_str = "couchbase://localhost"
        hosts = _extract_hosts_from_connection_string(conn_str)
        assert hosts == ["localhost"]

    def test_extract_hosts_multiple_hosts(self) -> None:
        """Extract multiple hosts from connection string."""
        conn_str = "couchbase://host1,host2,host3"
        hosts = _extract_hosts_from_connection_string(conn_str)
        assert hosts == ["host1", "host2", "host3"]

    def test_extract_hosts_with_port(self) -> None:
        """Extract hosts with port numbers."""
        conn_str = "couchbase://localhost:8091"
        hosts = _extract_hosts_from_connection_string(conn_str)
        assert hosts == ["localhost"]

    def test_extract_hosts_tls_connection(self) -> None:
        """Extract hosts from TLS connection string."""
        conn_str = "couchbases://secure-host.example.com"
        hosts = _extract_hosts_from_connection_string(conn_str)
        assert hosts == ["secure-host.example.com"]

    def test_extract_hosts_capella(self) -> None:
        """Extract hosts from Capella connection string."""
        conn_str = "couchbases://cb.abc123.cloud.couchbase.com"
        hosts = _extract_hosts_from_connection_string(conn_str)
        assert hosts == ["cb.abc123.cloud.couchbase.com"]

    def test_build_query_params_all(self) -> None:
        """Build query params with all fields."""
        params = _build_query_params(
            bucket_name="bucket",
            scope_name="scope",
            collection_name="collection",
            index_name="index",
        )
        assert params == {
            "bucket": "bucket",
            "scope": "scope",
            "collection": "collection",
            "index": "index",
        }

    def test_build_query_params_partial(self) -> None:
        """Build query params with some fields."""
        params = _build_query_params(
            bucket_name="bucket",
            scope_name=None,
            collection_name=None,
        )
        assert params == {"bucket": "bucket"}

    def test_build_query_params_empty(self) -> None:
        """Build query params with no fields."""
        params = _build_query_params(
            bucket_name=None,
            scope_name=None,
            collection_name=None,
        )
        assert params == {}

    def test_determine_ssl_non_tls(self) -> None:
        """Non-TLS connection should disable SSL verification."""
        result = _determine_ssl_verification("couchbase://localhost", None)
        assert result is False

    def test_determine_ssl_tls_no_cert(self) -> None:
        """TLS connection without cert uses system CA bundle."""
        result = _determine_ssl_verification("couchbases://localhost", None)
        assert result is True

    def test_determine_ssl_tls_with_cert(self) -> None:
        """TLS connection with cert uses provided cert."""
        result = _determine_ssl_verification(
            "couchbases://localhost", "/path/to/ca.pem"
        )
        assert result == "/path/to/ca.pem"

    def test_parse_major_version_basic(self) -> None:
        """Parse a typical full version string."""
        assert parse_major_version("8.0.0-1928-enterprise") == 8
        assert parse_major_version("7.6.11") == 7

    def test_parse_major_version_only_major(self) -> None:
        """Parse a string that is just a major version."""
        assert parse_major_version("8") == 8

    def test_parse_major_version_v_prefix(self) -> None:
        """A 'v' prefix should be tolerated."""
        assert parse_major_version("v8.0.0") == 8

    def test_parse_major_version_empty_or_none(self) -> None:
        """Empty/None inputs should raise ValueError."""
        with pytest.raises(ValueError):
            parse_major_version("")
        with pytest.raises(ValueError):
            parse_major_version(None)

    def test_parse_major_version_malformed(self) -> None:
        """Malformed input should raise ValueError."""
        with pytest.raises(ValueError):
            parse_major_version("unknown")
        with pytest.raises(ValueError):
            parse_major_version("abc.def")

    def test_process_index_data_from_query_basic(self) -> None:
        """Map a typical post-LET system:indexes row to the standard schema.

        The processor reads bucket/scope/collection (LET-injected by
        fetch_indexes_via_query_service) and ignores the raw bucket_id /
        scope_id / keyspace_id fields, so the fixture only needs the
        injected shape.
        """
        idx = {
            "name": "def_inventory_airport_city",
            "bucket": "travel-sample",
            "scope": "inventory",
            "collection": "airport",
            "state": "online",
            "metadata": {
                "definition": (
                    "CREATE INDEX `def_inventory_airport_city` ON "
                    "`travel-sample`.`inventory`.`airport`(`city`)"
                ),
                "last_scan_time": "2026-02-26T13:12:56.581+05:30",
            },
        }

        result = process_index_data_from_query(idx)

        assert result is not None
        assert result["name"] == "def_inventory_airport_city"
        assert result["bucket"] == "travel-sample"
        assert result["scope"] == "inventory"
        assert result["collection"] == "airport"
        assert result["status"] == "online"
        assert "city" in result["definition"]
        assert result["isPrimary"] is False
        assert result["lastScanTime"] == "2026-02-26T13:12:56.581+05:30"

    def test_process_index_data_from_query_primary(self) -> None:
        """Primary index rows should set isPrimary=True."""
        idx = {
            "name": "def_inventory_airport_primary",
            "bucket": "travel-sample",
            "scope": "inventory",
            "collection": "airport",
            "is_primary": True,
            "state": "online",
            "metadata": {
                "definition": "CREATE PRIMARY INDEX ...",
                "last_scan_time": None,
            },
        }

        result = process_index_data_from_query(idx)

        assert result is not None
        assert result["isPrimary"] is True

    def test_process_index_data_from_query_last_scan_time(self) -> None:
        """lastScanTime should be included from metadata."""
        idx = {
            "name": "idx",
            "bucket": "b",
            "scope": "s",
            "collection": "c",
            "state": "online",
            "metadata": {
                "definition": "CREATE INDEX idx ON b.s.c(x)",
                "last_scan_time": "2026-02-26T13:12:56.581+05:30",
            },
        }
        result = process_index_data_from_query(idx)
        assert result is not None
        assert result["lastScanTime"] == "2026-02-26T13:12:56.581+05:30"

    def test_process_index_data_from_query_with_raw_stats(self) -> None:
        """return_raw_index_stats=True should return the raw row unprocessed."""
        idx = {
            "name": "idx",
            "bucket_id": "b",
            "scope_id": "s",
            "keyspace_id": "c",
            "state": "online",
            "metadata": {"definition": "CREATE INDEX idx ON b.s.c(x)"},
        }
        result = process_index_data_from_query(idx, return_raw_index_stats=True)
        # Returned value IS the input row — no field renaming, no defaults applied.
        assert result is idx
        # Raw shape preserved (state, not status; bucket_id, not bucket).
        assert result["state"] == "online"
        assert result["bucket_id"] == "b"
        assert "bucket" not in result

    def test_process_index_data_from_query_without_raw_stats(self) -> None:
        """Default (processed) shape should not carry raw-row keys."""
        idx = {
            "name": "idx",
            "bucket_id": "b",
            "scope_id": "s",
            "keyspace_id": "c",
            "bucket": "b",
            "scope": "s",
            "collection": "c",
            "state": "online",
            "metadata": {
                "definition": "CREATE INDEX idx ON b.s.c(x)",
                "last_scan_time": None,
            },
        }
        result = process_index_data_from_query(idx)
        assert result is not None
        # Raw-shape keys should not leak into the processed output.
        assert "raw_index_stats" not in result
        assert "bucket_id" not in result
        assert "scope_id" not in result
        assert "keyspace_id" not in result
        assert "state" not in result  # processed shape uses 'status'

    def test_query_missing_name_falls_back_to_raw(self) -> None:
        """Rows without a name should return raw fallback with error message."""
        idx = {"bucket_id": "b"}
        result = process_index_data_from_query(idx)
        assert "error" in result
        assert "name" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_query_missing_metadata_falls_back_to_raw(self) -> None:
        """Missing metadata.definition should return raw fallback, not empty string."""
        idx = {
            "name": "idx",
            "bucket_id": "b",
            "scope_id": "s",
            "keyspace_id": "c",
            "state": "online",
        }
        result = process_index_data_from_query(idx)
        assert "error" in result
        assert "metadata.definition" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_query_missing_let_bucket_falls_back_to_raw(self) -> None:
        """Query path: bucket is injected by the SQL LET clause. Its absence
        means the row didn't come from our SQL or the LET semantics have
        changed — must fail loud."""
        idx = {
            "name": "idx",
            "state": "online",
            "scope": "s",
            "collection": "c",
            "metadata": {"definition": "CREATE INDEX idx ON b.s.c(x)"},
        }
        result = process_index_data_from_query(idx)
        assert "error" in result
        assert "bucket" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_query_missing_let_scope_falls_back_to_raw(self) -> None:
        """Query path: scope is injected by the SQL LET clause — same fail-
        loud contract as bucket."""
        idx = {
            "name": "idx",
            "state": "online",
            "bucket": "b",
            "collection": "c",
            "metadata": {"definition": "CREATE INDEX idx ON b.s.c(x)"},
        }
        result = process_index_data_from_query(idx)
        assert "error" in result
        assert "scope" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_query_missing_let_collection_falls_back_to_raw(self) -> None:
        """Query path: collection is injected by the SQL LET clause — same
        fail-loud contract as bucket."""
        idx = {
            "name": "idx",
            "state": "online",
            "bucket": "b",
            "scope": "s",
            "metadata": {"definition": "CREATE INDEX idx ON b.s.c(x)"},
        }
        result = process_index_data_from_query(idx)
        assert "error" in result
        assert "collection" in result["error"]
        assert result["raw_index_stats"] is idx

    # ------------------------------------------------------------------
    # Failure-mode tests: missing status, missing lastScanTime, etc.
    # ------------------------------------------------------------------

    def test_rest_missing_status_falls_back_to_raw(self) -> None:
        """REST path: missing 'status' must NOT default to empty string —
        the row should fall back to raw with an error message."""
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "bucket": "bucket",
            "scope": "scope",
            "collection": "collection",
        }
        result = process_index_data_from_rest_api(idx)
        assert result.get("status") is None
        assert "error" in result
        assert "status" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_query_missing_state_falls_back_to_raw(self) -> None:
        """Query path: missing 'state' must NOT default to empty string —
        the row should fall back to raw with an error message."""
        idx = {
            "name": "idx",
            "bucket_id": "b",
            "scope_id": "s",
            "keyspace_id": "c",
            "metadata": {"definition": "CREATE INDEX idx ON b.s.c(x)"},
        }
        result = process_index_data_from_query(idx)
        assert result.get("status") is None
        assert "error" in result
        assert "state" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_rest_missing_last_scan_time_key_falls_back_to_raw(self) -> None:
        """REST path: REST always emits the 'lastScanTime' key today (with
        literal 'NA' for never-scanned). Its absence indicates a schema
        change upstream and must fall back to raw.
        """
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "status": "Ready",
            "bucket": "bucket",
            "scope": "scope",
            "collection": "collection",
            # no lastScanTime — simulate a schema change
        }
        result = process_index_data_from_rest_api(idx)
        assert "error" in result
        assert "lastScanTime" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_rest_literal_NA_last_scan_time_passes_through(self) -> None:
        """REST path: never-scanned indexes carry the literal 'NA' string —
        this is the normal case and must NOT trigger a fallback."""
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "status": "Ready",
            "bucket": "bucket",
            "lastScanTime": "NA",
        }
        result = process_index_data_from_rest_api(idx)
        assert "error" not in result
        assert result["lastScanTime"] == "NA"

    def test_rest_null_last_scan_time_defaults_to_NA(self) -> None:
        """REST path: explicit null lastScanTime (defensive — REST doesn't
        emit null today but we coerce it to 'NA' if it ever does)."""
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "status": "Ready",
            "bucket": "bucket",
            "lastScanTime": None,
        }
        result = process_index_data_from_rest_api(idx)
        assert "error" not in result
        assert result["lastScanTime"] == "NA"

    def test_query_missing_last_scan_time_key_falls_back_to_raw(self) -> None:
        """Query path: system:indexes always emits 'metadata.last_scan_time'
        today (with value null for never-scanned). Its absence indicates a
        schema change upstream and must fall back to raw.
        """
        idx = {
            "name": "idx",
            "bucket": "b",
            "scope": "s",
            "collection": "c",
            "state": "online",
            # metadata is present but the last_scan_time key is missing —
            # this is the schema-drift case we want to detect.
            "metadata": {"definition": "CREATE INDEX idx ON b.s.c(x)"},
        }
        result = process_index_data_from_query(idx)
        assert "error" in result
        assert "last_scan_time" in result["error"]
        assert result["raw_index_stats"] is idx

    def test_query_null_last_scan_time_passes_through(self) -> None:
        """Query path: null last_scan_time (never-scanned) is honored verbatim
        — we don't substitute 'NA' or any other sentinel."""
        idx = {
            "name": "idx",
            "bucket": "b",
            "scope": "s",
            "collection": "c",
            "state": "online",
            "metadata": {
                "definition": "CREATE INDEX idx ON b.s.c(x)",
                "last_scan_time": None,
            },
        }
        result = process_index_data_from_query(idx)
        assert "error" not in result
        assert result["lastScanTime"] is None

    def test_query_timestamp_last_scan_time_passes_through(self) -> None:
        """Query path: timestamp last_scan_time is passed through verbatim."""
        ts = "2026-02-26T13:12:56.581+05:30"
        idx = {
            "name": "idx",
            "bucket": "b",
            "scope": "s",
            "collection": "c",
            "state": "online",
            "metadata": {
                "definition": "CREATE INDEX idx ON b.s.c(x)",
                "last_scan_time": ts,
            },
        }
        result = process_index_data_from_query(idx)
        assert "error" not in result
        assert result["lastScanTime"] == ts

    def test_query_legacy_keyspace_id_only_works(self) -> None:
        """Query path: legacy bucket-level indexes are normalised in SQL via
        the LET clause in fetch_indexes_via_query_service, so by the time the
        processor sees the row, bucket/scope/collection are already populated.
        """
        # Simulated post-LET shape for a legacy bucket-level index:
        # the SQL coerces scope="_default", collection="_default" when
        # bucket_id is absent.
        idx = {
            "name": "legacy_idx",
            "keyspace_id": "my-bucket",
            "bucket": "my-bucket",
            "scope": "_default",
            "collection": "_default",
            "state": "online",
            "metadata": {
                "definition": "CREATE INDEX legacy_idx ON `my-bucket`(x)",
                "last_scan_time": None,
            },
        }
        result = process_index_data_from_query(idx)
        assert "error" not in result
        assert result["bucket"] == "my-bucket"
        assert result["scope"] == "_default"
        assert result["collection"] == "_default"

    # ------------------------------------------------------------------
    # Unknown REST status: must log a warning and still return lowercase
    # (so the caller doesn't break on a new/unexpected backend value).
    # ------------------------------------------------------------------

    def test_unknown_rest_status_logs_warning_and_returns_lowercase(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An unrecognised REST status should produce a warning log AND be
        returned in lowercase form, exactly as the current contract states."""
        with caplog.at_level(
            logging.WARNING, logger=f"{MCP_SERVER_NAME}.utils.index_utils"
        ):
            result = map_rest_status_to_query_state("BrandNewStatus")

        assert result == "brandnewstatus"
        # Must log a warning so operators can report it.
        assert any(
            record.levelno == logging.WARNING
            and "BrandNewStatus" in record.getMessage()
            for record in caplog.records
        ), "Expected a WARNING log mentioning the unknown status string"
        assert any(
            "report" in record.getMessage().lower() for record in caplog.records
        ), "Warning should ask the user to report the issue"

    # ------------------------------------------------------------------
    # Raw passthrough: when return_raw_index_stats=True, the returned
    # value is the unmodified input row itself (no processing applied).
    # ------------------------------------------------------------------

    def test_rest_raw_index_stats_is_unmodified_passthrough(self) -> None:
        """REST path: returned object IS the input idx — same identity,
        no copies, no field rewrites, no status normalisation."""
        idx = {
            "name": "idx_test",
            "definition": "CREATE INDEX idx_test ON bucket(field)",
            "status": "Ready",
            "bucket": "bucket",
            "lastScanTime": "Thu Feb 26 13:12:55 IST 2026",
            "extra_field": "untouched",
        }
        result = process_index_data_from_rest_api(idx, return_raw_index_stats=True)
        # Same object — no copy, no field stripping, no processing.
        assert result is idx
        # Status is NOT mapped to query-service casing.
        assert result["status"] == "Ready"
        # Untouched extra field still present.
        assert result["extra_field"] == "untouched"

    def test_query_raw_index_stats_is_unmodified_passthrough(self) -> None:
        """Query path: returned object IS the input idx — same identity,
        no copies, no field renaming (state stays 'state', bucket_id stays)."""
        idx = {
            "name": "idx",
            "bucket_id": "b",
            "scope_id": "s",
            "keyspace_id": "c",
            "state": "online",
            "metadata": {
                "definition": "CREATE INDEX idx ON b.s.c(x)",
                "last_scan_time": "2026-02-26T13:12:56.581+05:30",
                "extra_meta": "untouched",
            },
        }
        result = process_index_data_from_query(idx, return_raw_index_stats=True)
        assert result is idx
        # Raw query-service field names preserved (no rename to bucket/status).
        assert "bucket_id" in result and "bucket" not in result
        assert "state" in result and "status" not in result
        assert result["metadata"]["extra_meta"] == "untouched"

    def test_map_rest_status_to_query_state(self) -> None:
        """REST API status strings should map to SQL++ query service equivalents."""
        assert map_rest_status_to_query_state("Ready") == "online"
        assert map_rest_status_to_query_state("Building") == "building"
        assert map_rest_status_to_query_state("Error") == "offline"
        assert (
            map_rest_status_to_query_state("Scheduled for Creation")
            == "scheduled for creation"
        )
        assert map_rest_status_to_query_state("Moving") == "building"
        assert map_rest_status_to_query_state("Paused") == "offline"
        assert map_rest_status_to_query_state("Warmup") == "pending"

    def test_map_rest_status_created_with_defer_build(self) -> None:
        """Created + defer_build in definition -> deferred."""
        definition = 'CREATE INDEX idx ON b(x) WITH {"defer_build": true}'
        assert map_rest_status_to_query_state("Created", definition) == "deferred"

    def test_map_rest_status_created_without_defer_build(self) -> None:
        """Created without defer_build in definition -> pending."""
        definition = "CREATE INDEX idx ON b(x)"
        assert map_rest_status_to_query_state("Created", definition) == "pending"

    def test_map_rest_status_created_no_definition(self) -> None:
        """Created with no definition defaults to pending."""
        assert map_rest_status_to_query_state("Created") == "pending"
        assert map_rest_status_to_query_state("Created", "") == "pending"

    def test_map_rest_status_to_query_state_qualified(self) -> None:
        """Qualified REST statuses (with parenthesis) should use prefix for mapping."""
        assert map_rest_status_to_query_state("Building (Upgrading)") == "building"
        assert map_rest_status_to_query_state("Building (Downgrading)") == "building"
        assert (
            map_rest_status_to_query_state(
                "Created (Upgrading)", 'WITH {"defer_build":true}'
            )
            == "deferred"
        )
        assert (
            map_rest_status_to_query_state(
                "Created (Downgrading)", "CREATE INDEX idx ON b(x)"
            )
            == "pending"
        )

    def test_map_rest_status_to_query_state_unknown(self) -> None:
        """Unknown REST statuses should be returned as-is in lowercase."""
        assert map_rest_status_to_query_state("SomeNewStatus") == "somenewstatus"


class TestConstants:
    """Unit tests for constants.py."""

    def test_mcp_server_name(self) -> None:
        """Verify MCP server name constant."""
        assert MCP_SERVER_NAME == "couchbase"

    def test_default_transport(self) -> None:
        """Verify default transport constant."""
        assert DEFAULT_TRANSPORT == "stdio"

    def test_allowed_transports(self) -> None:
        """Verify allowed transports include expected values."""
        assert "stdio" in ALLOWED_TRANSPORTS
        assert "http" in ALLOWED_TRANSPORTS
        assert "sse" in ALLOWED_TRANSPORTS

    def test_network_transports(self) -> None:
        """Verify network transports are subset of allowed."""
        for transport in NETWORK_TRANSPORTS:
            assert transport in ALLOWED_TRANSPORTS

    def test_default_read_only_mode(self) -> None:
        """Verify default read-only mode is True for safety."""
        assert DEFAULT_READ_ONLY_MODE is True


class TestConfigModule:
    """Unit tests for config.py module."""

    def test_get_settings_reads_from_lifespan_context(self) -> None:
        """get_settings returns the mapping attached to AppContext.settings."""
        payload = {
            "connection_string": "couchbase://localhost",
            "username": "admin",
        }
        mock_ctx = MagicMock()
        mock_ctx.request_context.lifespan_context.settings = payload

        assert get_settings(mock_ctx) is payload

    def test_get_settings_returns_empty_when_unset(self) -> None:
        """Before the lifespan populates settings, the default empty dict is returned."""
        mock_ctx = MagicMock()
        mock_ctx.request_context.lifespan_context.settings = {}

        assert get_settings(mock_ctx) == {}


class TestConnectionModule:
    """Unit tests for connection.py module."""

    @pytest.mark.asyncio
    async def test_connect_to_couchbase_cluster_with_password(self) -> None:
        """Verify password authentication path is used correctly."""
        mock_cluster = MagicMock()
        mock_cluster.wait_until_ready = AsyncMock()

        with (
            patch("cb_mcp.utils.connection.PasswordAuthenticator") as mock_auth,
            patch("cb_mcp.utils.connection.ClusterOptions") as mock_options,
            patch(
                "cb_mcp.utils.connection.Cluster", return_value=mock_cluster
            ) as mock_cluster_class,
        ):
            mock_options_instance = MagicMock()
            mock_options.return_value = mock_options_instance

            result = await connect_to_couchbase_cluster(
                connection_string="couchbase://localhost",
                username="admin",
                password="password",
            )

            mock_auth.assert_called_once_with("admin", "password", cert_path=None)
            mock_cluster_class.assert_called_once()
            mock_cluster.wait_until_ready.assert_called_once()
            assert result == mock_cluster

    @pytest.mark.asyncio
    async def test_connect_to_couchbase_cluster_with_certificate(self) -> None:
        """Verify certificate authentication path is used when certs provided."""
        mock_cluster = MagicMock()
        mock_cluster.wait_until_ready = AsyncMock()

        with (
            patch("cb_mcp.utils.connection.CertificateAuthenticator") as mock_cert_auth,
            patch("cb_mcp.utils.connection.ClusterOptions") as mock_options,
            patch("cb_mcp.utils.connection.Cluster", return_value=mock_cluster),
            patch("cb_mcp.utils.connection.os.path.exists", return_value=True),
        ):
            mock_options_instance = MagicMock()
            mock_options.return_value = mock_options_instance

            result = await connect_to_couchbase_cluster(
                connection_string="couchbases://localhost",
                username="admin",
                password="password",
                ca_cert_path="/path/to/ca.pem",
                client_cert_path="/path/to/client.pem",
                client_key_path="/path/to/client.key",
            )

            mock_cert_auth.assert_called_once_with(
                cert_path="/path/to/client.pem",
                key_path="/path/to/client.key",
                trust_store_path="/path/to/ca.pem",
            )
            assert result == mock_cluster

    @pytest.mark.asyncio
    async def test_connect_to_couchbase_cluster_missing_cert_file(self) -> None:
        """Verify FileNotFoundError raised when cert files don't exist."""
        with (
            patch("cb_mcp.utils.connection.os.path.exists", return_value=False),
            pytest.raises(
                FileNotFoundError, match="Client certificate files not found"
            ),
        ):
            await connect_to_couchbase_cluster(
                connection_string="couchbases://localhost",
                username="admin",
                password="password",
                client_cert_path="/path/to/missing.pem",
                client_key_path="/path/to/missing.key",
            )

    @pytest.mark.asyncio
    async def test_connect_to_couchbase_cluster_connection_failure(self) -> None:
        """Verify exceptions are re-raised on connection failure."""
        with (
            patch("cb_mcp.utils.connection.PasswordAuthenticator"),
            patch("cb_mcp.utils.connection.ClusterOptions"),
            patch(
                "cb_mcp.utils.connection.Cluster",
                side_effect=Exception("Connection refused"),
            ),
            pytest.raises(Exception, match="Connection refused"),
        ):
            await connect_to_couchbase_cluster(
                connection_string="couchbase://invalid-host",
                username="admin",
                password="password",
            )

    @pytest.mark.asyncio
    async def test_connect_to_bucket_success(self) -> None:
        """Verify connect_to_bucket returns bucket object."""
        mock_cluster = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.on_connect = AsyncMock()
        mock_cluster.bucket.return_value = mock_bucket

        result = await connect_to_bucket(mock_cluster, "my-bucket")

        mock_cluster.bucket.assert_called_once_with("my-bucket")
        assert result == mock_bucket

    @pytest.mark.asyncio
    async def test_connect_to_bucket_failure(self) -> None:
        """Verify connect_to_bucket raises exception on failure."""
        mock_cluster = MagicMock()
        mock_cluster.bucket.side_effect = Exception("Bucket not found")

        with pytest.raises(Exception, match="Bucket not found"):
            await connect_to_bucket(mock_cluster, "nonexistent-bucket")


class TestContextModule:
    """Unit tests for context.py module."""

    def test_app_context_default_values(self) -> None:
        """Verify AppContext has correct default values."""
        ctx = AppContext()
        assert ctx.cluster_provider is None
        assert ctx.read_only_query_mode is True

    def test_app_context_with_provider(self) -> None:
        """Verify AppContext can hold a cluster provider."""
        mock_provider = MagicMock()
        ctx = AppContext(cluster_provider=mock_provider, read_only_query_mode=False)

        assert ctx.cluster_provider is mock_provider
        assert ctx.read_only_query_mode is False

    @pytest.mark.asyncio
    async def test_get_cluster_connection_delegates_to_provider(self) -> None:
        """get_cluster_connection calls into the provider attached to AppContext."""
        mock_cluster = MagicMock()
        mock_provider = MagicMock()
        mock_provider.get_cluster = AsyncMock(return_value=mock_cluster)

        mock_ctx = MagicMock()
        mock_ctx.request_context.lifespan_context.cluster_provider = mock_provider

        result = await get_cluster_connection(mock_ctx)

        assert result is mock_cluster
        mock_provider.get_cluster.assert_called_once_with(mock_ctx)

    @pytest.mark.asyncio
    async def test_get_cluster_connection_raises_without_provider(self) -> None:
        """get_cluster_connection fails fast if the lifespan forgot to wire a provider."""
        mock_ctx = MagicMock()
        mock_ctx.request_context.lifespan_context.cluster_provider = None

        with pytest.raises(RuntimeError, match="Cluster provider not initialized"):
            await get_cluster_connection(mock_ctx)

    @pytest.mark.asyncio
    async def test_static_cluster_provider_connects_lazily(self) -> None:
        """StaticClusterProvider defers connection until first get_cluster call."""
        mock_cluster = MagicMock()
        mock_settings = {
            "connection_string": "couchbase://localhost",
            "username": "admin",
            "password": "password",
        }

        with patch(
            "providers.static.connect_to_couchbase_cluster",
            return_value=mock_cluster,
        ) as mock_connect:
            provider = StaticClusterProvider(settings=mock_settings)
            # Constructor alone must not open a connection.
            mock_connect.assert_not_called()

            result = await provider.get_cluster(MagicMock())
            assert result is mock_cluster
            mock_connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_static_cluster_provider_caches_cluster(self) -> None:
        """Repeated get_cluster calls reuse the first cluster."""
        mock_cluster = MagicMock()
        mock_settings = {
            "connection_string": "couchbase://localhost",
            "username": "admin",
            "password": "password",
        }

        with patch(
            "providers.static.connect_to_couchbase_cluster",
            return_value=mock_cluster,
        ) as mock_connect:
            provider = StaticClusterProvider(settings=mock_settings)
            first = await provider.get_cluster(MagicMock())
            second = await provider.get_cluster(MagicMock())

        assert first is second is mock_cluster
        mock_connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_static_cluster_provider_propagates_connection_failure(self) -> None:
        """A failed connect raises and does not poison the cache."""
        mock_settings = {
            "connection_string": "couchbase://invalid",
            "username": "admin",
            "password": "wrong",
        }

        with patch(
            "providers.static.connect_to_couchbase_cluster",
            side_effect=Exception("Auth failed"),
        ):
            provider = StaticClusterProvider(settings=mock_settings)
            with pytest.raises(Exception, match="Auth failed"):
                await provider.get_cluster(MagicMock())

        # Cache stayed empty so a subsequent attempt can retry.
        assert provider._cluster is None

    @pytest.mark.asyncio
    async def test_static_cluster_provider_close_releases_cluster(self) -> None:
        """close() calls cluster.close() and clears the cache."""
        mock_cluster = MagicMock()
        mock_cluster.close = AsyncMock()
        mock_settings = {
            "connection_string": "couchbase://localhost",
            "username": "admin",
            "password": "password",
        }

        with patch(
            "providers.static.connect_to_couchbase_cluster",
            return_value=mock_cluster,
        ):
            provider = StaticClusterProvider(settings=mock_settings)
            await provider.get_cluster(MagicMock())
            await provider.close()

        mock_cluster.close.assert_called_once()
        assert provider._cluster is None


class TestFetchIndexesViaQueryService:
    """Unit tests for fetch_indexes_via_query_service."""

    _LET_CLAUSE = (
        "LET bid = IFMISSING(s.bucket_id, s.keyspace_id), "
        "sid = IFMISSING(s.scope_id, '_default'), "
        "kid = NVL2(s.bucket_id, s.keyspace_id, '_default')"
    )
    _BASE_WHERE = "s.namespace_id = 'default' AND s.`using` = 'gsi'"

    @pytest.mark.asyncio
    async def test_no_filters(self) -> None:
        """With no filters, query should carry the namespace + GSI guards
        and the LET-based bucket/scope/collection normalization."""
        mock_ctx = MagicMock()
        expected_query = (
            "SELECT s.*, bid AS `bucket`, sid AS `scope`, kid AS `collection` "
            f"FROM system:indexes AS s {self._LET_CLAUSE} "
            f"WHERE {self._BASE_WHERE}"
        )

        with patch(
            "cb_mcp.tools.index.run_cluster_query",
            new_callable=AsyncMock,
            return_value=[{"name": "idx1"}, {"name": "idx2"}],
        ) as mock_query:
            result = await fetch_indexes_via_query_service(
                mock_ctx, None, None, None, None
            )

        mock_query.assert_called_once_with(
            mock_ctx, expected_query, named_parameters={}
        )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_raw_mode_selects_raw_source_rows(self) -> None:
        """return_raw_index_stats=True must SELECT RAW s — no injected
        bucket/scope/collection on the result rows."""
        mock_ctx = MagicMock()
        expected_query = (
            f"SELECT RAW s FROM system:indexes AS s {self._LET_CLAUSE} "
            f"WHERE {self._BASE_WHERE}"
        )

        with patch(
            "cb_mcp.tools.index.run_cluster_query",
            new_callable=AsyncMock,
            return_value=[{"name": "idx1"}],
        ) as mock_query:
            await fetch_indexes_via_query_service(
                mock_ctx, None, None, None, None, return_raw_index_stats=True
            )

        mock_query.assert_called_once_with(
            mock_ctx, expected_query, named_parameters={}
        )

    @pytest.mark.asyncio
    async def test_all_filters(self) -> None:
        """All filters should apply against the normalized LET aliases so
        legacy indexes match by bucket symmetrically with modern ones."""
        mock_ctx = MagicMock()

        with patch(
            "cb_mcp.tools.index.run_cluster_query",
            new_callable=AsyncMock,
            return_value=[{"name": "idx1"}],
        ) as mock_query:
            result = await fetch_indexes_via_query_service(
                mock_ctx, "bucket", "scope", "collection", "idx1"
            )

        sent_query = mock_query.call_args[0][1]
        params = mock_query.call_args[1]["named_parameters"]
        assert params == {
            "bucket_id": "bucket",
            "scope_id": "scope",
            "keyspace_id": "collection",
            "index_name": "idx1",
        }
        # Verify filters are applied against LET aliases (not raw fields)
        # so legacy and modern indexes both match.
        assert "bid = $bucket_id" in sent_query
        assert "sid = $scope_id" in sent_query
        assert "kid = $keyspace_id" in sent_query
        assert "s.name = $index_name" in sent_query
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_non_dict_rows_filtered(self) -> None:
        """Non-dict rows returned by the query should be dropped."""
        mock_ctx = MagicMock()

        with patch(
            "cb_mcp.tools.index.run_cluster_query",
            new_callable=AsyncMock,
            return_value=[{"name": "idx1"}, "stray_string", 42, None],
        ):
            result = await fetch_indexes_via_query_service(
                mock_ctx, None, None, None, None
            )

        assert result == [{"name": "idx1"}]


class TestResolveClusterMajorVersion:
    """Unit tests for resolve_cluster_major_version."""

    @pytest.mark.asyncio
    async def test_dict_nodes(self) -> None:
        """Version detection with nodes represented as dicts."""
        mock_cluster = AsyncMock()
        info = MagicMock()
        info.nodes = [
            {"version": "8.0.0-1928-enterprise"},
            {"version": "8.0.1-2000-enterprise"},
        ]
        mock_cluster.cluster_info.return_value = info

        result = await resolve_cluster_major_version(mock_cluster)

        assert result == 8

    @pytest.mark.asyncio
    async def test_object_nodes(self) -> None:
        """Version detection with nodes represented as objects with attributes."""
        mock_cluster = AsyncMock()
        info = MagicMock()
        node = MagicMock()
        node.version = "7.6.0"
        info.nodes = [node]
        mock_cluster.cluster_info.return_value = info

        result = await resolve_cluster_major_version(mock_cluster)

        assert result == 7

    @pytest.mark.asyncio
    async def test_mixed_versions_returns_min(self) -> None:
        """Mixed-version cluster returns the minimum major version."""
        mock_cluster = AsyncMock()
        info = MagicMock()
        info.nodes = [
            {"version": "8.0.0-enterprise"},
            {"version": "7.6.11-enterprise"},
        ]
        mock_cluster.cluster_info.return_value = info

        result = await resolve_cluster_major_version(mock_cluster)

        assert result == 7

    @pytest.mark.asyncio
    async def test_cluster_info_exception_propagates(self) -> None:
        """If cluster_info() throws, the exception should propagate."""
        mock_cluster = AsyncMock()
        mock_cluster.cluster_info.side_effect = Exception("connection refused")

        with pytest.raises(Exception, match="connection refused"):
            await resolve_cluster_major_version(mock_cluster)

    @pytest.mark.asyncio
    async def test_empty_nodes_raises(self) -> None:
        """If cluster reports no nodes, raise RuntimeError."""
        mock_cluster = AsyncMock()
        info = MagicMock()
        info.nodes = []
        mock_cluster.cluster_info.return_value = info

        with pytest.raises(RuntimeError, match="no nodes"):
            await resolve_cluster_major_version(mock_cluster)


class TestListIndexesVersionRouting:
    """Integration-level tests verifying list_indexes routes to the correct path."""

    @pytest.mark.asyncio
    async def test_version_8_uses_query_service(self) -> None:
        """Cluster version >= 8 should use system:all_indexes, not REST API."""
        mock_ctx = MagicMock()
        mock_cluster = AsyncMock()
        info = MagicMock()
        info.nodes = [{"version": "8.0.0-enterprise"}]
        mock_cluster.cluster_info.return_value = info

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
                new_callable=AsyncMock,
                return_value=mock_cluster,
            ),
            patch(
                "cb_mcp.tools.index.run_cluster_query",
                new_callable=AsyncMock,
                return_value=[
                    {
                        "name": "idx1",
                        "bucket_id": "b",
                        "scope_id": "s",
                        "keyspace_id": "c",
                        # bucket/scope/collection are injected by the LET
                        # clause in the production SQL; the mock simulates
                        # what the query service actually returns.
                        "bucket": "b",
                        "scope": "s",
                        "collection": "c",
                        "state": "online",
                        "metadata": {
                            "definition": "CREATE INDEX idx1 ON b.s.c(x)",
                            "last_scan_time": None,
                        },
                    }
                ],
            ) as mock_query,
            patch(
                "cb_mcp.tools.index.fetch_indexes_from_rest_api", new_callable=AsyncMock
            ) as mock_rest,
        ):
            result = await list_indexes(mock_ctx)

        mock_query.assert_called_once()
        mock_rest.assert_not_called()
        assert len(result) == 1
        assert result[0]["name"] == "idx1"

    @pytest.mark.asyncio
    async def test_version_7_uses_rest_api(self) -> None:
        """Cluster version < 8 should fall back to the REST API."""
        mock_ctx = MagicMock()
        mock_cluster = AsyncMock()
        info = MagicMock()
        info.nodes = [{"version": "7.6.11-enterprise"}]
        mock_cluster.cluster_info.return_value = info

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
                new_callable=AsyncMock,
                return_value=mock_cluster,
            ),
            patch(
                "cb_mcp.tools.index.run_cluster_query", new_callable=AsyncMock
            ) as mock_query,
            patch(
                "cb_mcp.tools.index.fetch_indexes_from_rest_api",
                new_callable=AsyncMock,
                return_value=[
                    {
                        "name": "idx1",
                        "definition": "CREATE INDEX idx1 ON b.s.c(x)",
                        "status": "Ready",
                        "bucket": "b",
                        "scope": "s",
                        "collection": "c",
                        "isPrimary": False,
                        "lastScanTime": "NA",
                    }
                ],
            ) as mock_rest,
        ):
            result = await list_indexes(mock_ctx)

        mock_query.assert_not_called()
        mock_rest.assert_called_once()
        assert len(result) == 1
        assert result[0]["name"] == "idx1"
