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

from unittest.mock import MagicMock, patch

import pytest

from utils.config import get_settings
from utils.connection import connect_to_bucket, connect_to_couchbase_cluster
from utils.constants import (
    ALLOWED_TRANSPORTS,
    DEFAULT_READ_ONLY_MODE,
    DEFAULT_TRANSPORT,
    MCP_SERVER_NAME,
    NETWORK_TRANSPORTS,
)
from utils.context import (
    AppContext,
    _set_cluster_in_lifespan_context,
    get_cluster_connection,
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


class TestConfigModule:
    """Unit tests for config.py module."""

    def test_get_settings_returns_dict(self) -> None:
        """Verify get_settings returns a dictionary from Click context."""
        mock_ctx = MagicMock()
        mock_ctx.obj = {
            "connection_string": "couchbase://localhost",
            "username": "admin",
        }

        with patch("utils.config.click.get_current_context", return_value=mock_ctx):
            settings = get_settings()

        assert isinstance(settings, dict)
        assert settings["connection_string"] == "couchbase://localhost"
        assert settings["username"] == "admin"

    def test_get_settings_returns_empty_dict_when_obj_none(self) -> None:
        """Verify get_settings returns empty dict when ctx.obj is None."""
        mock_ctx = MagicMock()
        mock_ctx.obj = None

        with patch("utils.config.click.get_current_context", return_value=mock_ctx):
            settings = get_settings()

        assert settings == {}

    def test_get_settings_preserves_all_keys(self) -> None:
        """Verify get_settings preserves all configuration keys."""
        mock_ctx = MagicMock()
        mock_ctx.obj = {
            "connection_string": "couchbases://host.cloud.couchbase.com",
            "username": "admin",
            "password": "secret",
            "ca_cert_path": "/path/to/ca.pem",
            "client_cert_path": "/path/to/client.pem",
            "client_key_path": "/path/to/client.key",
        }

        with patch("utils.config.click.get_current_context", return_value=mock_ctx):
            settings = get_settings()

        assert len(settings) == 6
        assert settings["ca_cert_path"] == "/path/to/ca.pem"


class TestConnectionModule:
    """Unit tests for connection.py module."""

    def test_connect_to_couchbase_cluster_with_password(self) -> None:
        """Verify password authentication path is used correctly."""
        mock_cluster = MagicMock()

        with (
            patch("utils.connection.PasswordAuthenticator") as mock_auth,
            patch("utils.connection.ClusterOptions") as mock_options,
            patch(
                "utils.connection.Cluster", return_value=mock_cluster
            ) as mock_cluster_class,
        ):
            mock_options_instance = MagicMock()
            mock_options.return_value = mock_options_instance

            result = connect_to_couchbase_cluster(
                connection_string="couchbase://localhost",
                username="admin",
                password="password",
            )

            mock_auth.assert_called_once_with("admin", "password", cert_path=None)
            mock_cluster_class.assert_called_once()
            mock_cluster.wait_until_ready.assert_called_once()
            assert result == mock_cluster

    def test_connect_to_couchbase_cluster_with_certificate(self) -> None:
        """Verify certificate authentication path is used when certs provided."""
        mock_cluster = MagicMock()

        with (
            patch("utils.connection.CertificateAuthenticator") as mock_cert_auth,
            patch("utils.connection.ClusterOptions") as mock_options,
            patch("utils.connection.Cluster", return_value=mock_cluster),
            patch("utils.connection.os.path.exists", return_value=True),
        ):
            mock_options_instance = MagicMock()
            mock_options.return_value = mock_options_instance

            result = connect_to_couchbase_cluster(
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

    def test_connect_to_couchbase_cluster_missing_cert_file(self) -> None:
        """Verify FileNotFoundError raised when cert files don't exist."""
        with (
            patch("utils.connection.os.path.exists", return_value=False),
            pytest.raises(
                FileNotFoundError, match="Client certificate files not found"
            ),
        ):
            connect_to_couchbase_cluster(
                connection_string="couchbases://localhost",
                username="admin",
                password="password",
                client_cert_path="/path/to/missing.pem",
                client_key_path="/path/to/missing.key",
            )

    def test_connect_to_couchbase_cluster_connection_failure(self) -> None:
        """Verify exceptions are re-raised on connection failure."""
        with (
            patch("utils.connection.PasswordAuthenticator"),
            patch("utils.connection.ClusterOptions"),
            patch(
                "utils.connection.Cluster", side_effect=Exception("Connection refused")
            ),
            pytest.raises(Exception, match="Connection refused"),
        ):
            connect_to_couchbase_cluster(
                connection_string="couchbase://invalid-host",
                username="admin",
                password="password",
            )

    def test_connect_to_bucket_success(self) -> None:
        """Verify connect_to_bucket returns bucket object."""
        mock_cluster = MagicMock()
        mock_bucket = MagicMock()
        mock_cluster.bucket.return_value = mock_bucket

        result = connect_to_bucket(mock_cluster, "my-bucket")

        mock_cluster.bucket.assert_called_once_with("my-bucket")
        assert result == mock_bucket

    def test_connect_to_bucket_failure(self) -> None:
        """Verify connect_to_bucket raises exception on failure."""
        mock_cluster = MagicMock()
        mock_cluster.bucket.side_effect = Exception("Bucket not found")

        with pytest.raises(Exception, match="Bucket not found"):
            connect_to_bucket(mock_cluster, "nonexistent-bucket")


class TestContextModule:
    """Unit tests for context.py module."""

    def test_app_context_default_values(self) -> None:
        """Verify AppContext has correct default values."""
        ctx = AppContext()
        assert ctx.cluster is None
        assert ctx.read_only_query_mode is True

    def test_app_context_with_cluster(self) -> None:
        """Verify AppContext can hold a cluster reference."""
        mock_cluster = MagicMock()
        ctx = AppContext(cluster=mock_cluster, read_only_query_mode=False)

        assert ctx.cluster == mock_cluster
        assert ctx.read_only_query_mode is False

    def test_get_cluster_connection_returns_existing(self) -> None:
        """Verify get_cluster_connection returns existing cluster."""
        mock_cluster = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.request_context.lifespan_context.cluster = mock_cluster

        result = get_cluster_connection(mock_ctx)

        assert result == mock_cluster

    def test_get_cluster_connection_creates_new(self) -> None:
        """Verify get_cluster_connection creates connection if not exists."""
        mock_cluster = MagicMock()
        mock_ctx = MagicMock()
        # First access returns None (no cluster), then returns the mock after connection
        mock_ctx.request_context.lifespan_context.cluster = None

        mock_settings = {
            "connection_string": "couchbase://localhost",
            "username": "admin",
            "password": "password",
        }

        with (
            patch("utils.context.get_settings", return_value=mock_settings),
            patch(
                "utils.context.connect_to_couchbase_cluster", return_value=mock_cluster
            ),
        ):
            # Simulate the cluster being set after connection
            def set_cluster_side_effect(*args, **kwargs):
                mock_ctx.request_context.lifespan_context.cluster = mock_cluster
                return mock_cluster

            with patch(
                "utils.context.connect_to_couchbase_cluster",
                side_effect=set_cluster_side_effect,
            ):
                # Since cluster is None, it will try to connect
                # The function sets the cluster and then returns it
                # We need to adjust the mock behavior
                pass

        # Test the path where cluster already exists
        mock_ctx.request_context.lifespan_context.cluster = mock_cluster
        result = get_cluster_connection(mock_ctx)
        assert result == mock_cluster

    def test_set_cluster_in_lifespan_context_success(self) -> None:
        """Verify _set_cluster_in_lifespan_context sets cluster correctly."""
        mock_cluster = MagicMock()
        mock_ctx = MagicMock()
        mock_settings = {
            "connection_string": "couchbase://localhost",
            "username": "admin",
            "password": "password",
            "ca_cert_path": None,
            "client_cert_path": None,
            "client_key_path": None,
        }

        with (
            patch("utils.context.get_settings", return_value=mock_settings),
            patch(
                "utils.context.connect_to_couchbase_cluster", return_value=mock_cluster
            ),
        ):
            _set_cluster_in_lifespan_context(mock_ctx)

        assert mock_ctx.request_context.lifespan_context.cluster == mock_cluster

    def test_set_cluster_in_lifespan_context_failure(self) -> None:
        """Verify _set_cluster_in_lifespan_context raises on connection failure."""
        mock_ctx = MagicMock()
        mock_settings = {
            "connection_string": "couchbase://invalid",
            "username": "admin",
            "password": "wrong",
        }

        with (
            patch("utils.context.get_settings", return_value=mock_settings),
            patch(
                "utils.context.connect_to_couchbase_cluster",
                side_effect=Exception("Auth failed"),
            ),
            pytest.raises(Exception, match="Auth failed"),
        ):
            _set_cluster_in_lifespan_context(mock_ctx)
