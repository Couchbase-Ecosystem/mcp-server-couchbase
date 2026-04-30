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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cb_mcp.tools.index import (
    _fetch_indexes_via_query_service,
    _resolve_cluster_major_version,
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
    parse_major_version,
    process_index_data,
    process_query_index_data,
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
        """Empty/None inputs should return 0."""
        assert parse_major_version("") == 0
        assert parse_major_version(None) == 0

    def test_parse_major_version_malformed(self) -> None:
        """Malformed input should return 0."""
        assert parse_major_version("unknown") == 0
        assert parse_major_version("abc.def") == 0

    def test_process_query_index_data_basic(self) -> None:
        """Map a typical system:all_indexes row to the standard schema."""
        idx = {
            "name": "def_inventory_airport_city",
            "bucket_id": "travel-sample",
            "scope_id": "inventory",
            "keyspace_id": "airport",
            "state": "online",
            "using": "gsi",
            "metadata": {
                "definition": (
                    "CREATE INDEX `def_inventory_airport_city` ON "
                    "`travel-sample`.`inventory`.`airport`(`city`)"
                ),
            },
        }

        result = process_query_index_data(idx, include_raw_index_stats=False)

        assert result is not None
        assert result["name"] == "def_inventory_airport_city"
        assert result["bucket"] == "travel-sample"
        assert result["scope"] == "inventory"
        assert result["collection"] == "airport"
        assert result["status"] == "online"
        assert "city" in result["definition"]
        assert result["isPrimary"] is False
        assert "raw_index_stats" not in result

    def test_process_query_index_data_primary(self) -> None:
        """Primary index rows should set isPrimary=True."""
        idx = {
            "name": "def_inventory_airport_primary",
            "bucket_id": "travel-sample",
            "scope_id": "inventory",
            "keyspace_id": "airport",
            "is_primary": True,
            "state": "online",
            "metadata": {"definition": "CREATE PRIMARY INDEX ..."},
        }

        result = process_query_index_data(idx, include_raw_index_stats=False)

        assert result is not None
        assert result["isPrimary"] is True

    def test_process_query_index_data_with_raw_stats(self) -> None:
        """Raw stats should be included when requested."""
        idx = {
            "name": "idx",
            "bucket_id": "b",
            "scope_id": "s",
            "keyspace_id": "c",
            "state": "online",
            "metadata": {"definition": "CREATE INDEX idx ON b.s.c(x)"},
        }
        result = process_query_index_data(idx, include_raw_index_stats=True)
        assert result is not None
        assert result["raw_index_stats"] == idx

    def test_process_query_index_data_no_name(self) -> None:
        """Rows without a name should be filtered out."""
        idx = {"bucket_id": "b"}
        assert process_query_index_data(idx, include_raw_index_stats=False) is None

    def test_process_query_index_data_no_metadata(self) -> None:
        """Missing metadata should not crash and should omit definition."""
        idx = {
            "name": "idx",
            "bucket_id": "b",
            "scope_id": "s",
            "keyspace_id": "c",
            "state": "online",
        }
        result = process_query_index_data(idx, include_raw_index_stats=False)
        assert result is not None
        assert "definition" not in result


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
    """Unit tests for _fetch_indexes_via_query_service."""

    @pytest.mark.asyncio
    async def test_no_filters(self) -> None:
        """With no filters, query should only have the GSI clause."""
        mock_ctx = MagicMock()
        expected_query = (
            "SELECT RAW all_indexes FROM system:all_indexes WHERE `using` = 'gsi'"
        )

        with patch(
            "cb_mcp.tools.index.run_cluster_query",
            new_callable=AsyncMock,
            return_value=[{"name": "idx1"}, {"name": "idx2"}],
        ) as mock_query:
            result = await _fetch_indexes_via_query_service(
                mock_ctx, None, None, None, None
            )

        mock_query.assert_called_once_with(
            mock_ctx, expected_query, named_parameters={}
        )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_all_filters(self) -> None:
        """All filters should produce named_parameters with the right keys."""
        mock_ctx = MagicMock()

        with patch(
            "cb_mcp.tools.index.run_cluster_query",
            new_callable=AsyncMock,
            return_value=[{"name": "idx1"}],
        ) as mock_query:
            result = await _fetch_indexes_via_query_service(
                mock_ctx, "bucket", "scope", "collection", "idx1"
            )

        _, kwargs = mock_query.call_args
        params = kwargs["named_parameters"]
        assert params == {
            "bucket_id": "bucket",
            "scope_id": "scope",
            "keyspace_id": "collection",
            "index_name": "idx1",
        }
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
            result = await _fetch_indexes_via_query_service(
                mock_ctx, None, None, None, None
            )

        assert result == [{"name": "idx1"}]


class TestResolveClusterMajorVersion:
    """Unit tests for _resolve_cluster_major_version."""

    @pytest.mark.asyncio
    async def test_dict_nodes(self) -> None:
        """Version detection with nodes represented as dicts."""

        mock_ctx = MagicMock()
        mock_cluster = AsyncMock()
        info = MagicMock()
        info.nodes = [
            {"version": "8.0.0-1928-enterprise"},
            {"version": "8.0.1-2000-enterprise"},
        ]
        mock_cluster.cluster_info.return_value = info

        with patch(
            "cb_mcp.tools.index.get_cluster_connection",
            new_callable=AsyncMock,
            return_value=mock_cluster,
        ):
            result = await _resolve_cluster_major_version(mock_ctx)

        assert result == 8

    @pytest.mark.asyncio
    async def test_object_nodes(self) -> None:
        """Version detection with nodes represented as objects with attributes."""

        mock_ctx = MagicMock()
        mock_cluster = AsyncMock()
        info = MagicMock()
        node = MagicMock()
        node.version = "7.6.0"
        info.nodes = [node]
        mock_cluster.cluster_info.return_value = info

        with patch(
            "cb_mcp.tools.index.get_cluster_connection",
            new_callable=AsyncMock,
            return_value=mock_cluster,
        ):
            result = await _resolve_cluster_major_version(mock_ctx)

        assert result == 7

    @pytest.mark.asyncio
    async def test_mixed_versions_returns_min(self) -> None:
        """Mixed-version cluster returns the minimum major version."""

        mock_ctx = MagicMock()
        mock_cluster = AsyncMock()
        info = MagicMock()
        info.nodes = [
            {"version": "8.0.0-enterprise"},
            {"version": "7.6.11-enterprise"},
        ]
        mock_cluster.cluster_info.return_value = info

        with patch(
            "cb_mcp.tools.index.get_cluster_connection",
            new_callable=AsyncMock,
            return_value=mock_cluster,
        ):
            result = await _resolve_cluster_major_version(mock_ctx)

        assert result == 7

    @pytest.mark.asyncio
    async def test_cluster_info_exception_returns_zero(self) -> None:
        """If cluster_info() throws, fall back to 0."""
        mock_ctx = MagicMock()

        with patch(
            "cb_mcp.tools.index.get_cluster_connection",
            new_callable=AsyncMock,
            side_effect=Exception("connection refused"),
        ):
            result = await _resolve_cluster_major_version(mock_ctx)

        assert result == 0

    @pytest.mark.asyncio
    async def test_empty_nodes_returns_zero(self) -> None:
        """If cluster reports no nodes, return 0."""

        mock_ctx = MagicMock()
        mock_cluster = AsyncMock()
        info = MagicMock()
        info.nodes = []
        mock_cluster.cluster_info.return_value = info

        with patch(
            "cb_mcp.tools.index.get_cluster_connection",
            new_callable=AsyncMock,
            return_value=mock_cluster,
        ):
            result = await _resolve_cluster_major_version(mock_ctx)

        assert result == 0
