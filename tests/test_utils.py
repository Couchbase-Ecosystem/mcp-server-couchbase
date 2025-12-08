"""
Unit tests for utility modules.

Tests for:
- utils/index_utils.py - Index utility functions
- utils/constants.py - Constants validation
"""

from __future__ import annotations

import pytest

from utils.constants import (
    ALLOWED_TRANSPORTS,
    DEFAULT_READ_ONLY_MODE,
    DEFAULT_TRANSPORT,
    MCP_SERVER_NAME,
    NETWORK_TRANSPORTS,
)
from utils.index_utils import (
    _build_query_params,
    _determine_ssl_verification,
    _extract_hosts_from_connection_string,
    clean_index_definition,
    process_index_data,
    validate_connection_settings,
    validate_filter_params,
)


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
        }
        result = process_index_data(idx, include_raw_index_stats=False)

        assert result is not None
        assert result["name"] == "idx_test"
        assert result["bucket"] == "travel-sample"
        assert result["status"] == "Ready"
        assert result["isPrimary"] is False
        assert "raw_index_stats" not in result

    def test_process_index_data_with_raw_stats(self) -> None:
        """Process index data with raw stats included."""
        idx = {
            "name": "idx_test",
            "status": "Ready",
            "bucket": "bucket",
            "scope": "scope",
            "collection": "collection",
            "extra_field": "some_value",
        }
        result = process_index_data(idx, include_raw_index_stats=True)

        assert result is not None
        assert "raw_index_stats" in result
        assert result["raw_index_stats"] == idx

    def test_process_index_data_no_name(self) -> None:
        """Index without name should return None."""
        idx = {"status": "Ready", "bucket": "bucket"}
        result = process_index_data(idx, include_raw_index_stats=False)
        assert result is None

    def test_process_index_data_primary_index(self) -> None:
        """Process primary index data."""
        idx = {
            "name": "#primary",
            "isPrimary": True,
            "bucket": "bucket",
        }
        result = process_index_data(idx, include_raw_index_stats=False)

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
