"""
Couchbase MCP Server
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import click
from fastmcp import FastMCP
from fastmcp.tools import FunctionTool

# Reusable tools and utilities from the cb_mcp package
from cb_mcp.auth import build_remote_auth, make_metadata_proxy_handler
from cb_mcp.tool_registration import prepare_tools_for_registration
from cb_mcp.tools import TOOL_ANNOTATIONS
from cb_mcp.utils import (
    ALLOWED_TRANSPORTS,
    DEFAULT_HOST,
    DEFAULT_LOG_LEVEL,
    DEFAULT_PORT,
    DEFAULT_READ_ONLY_MODE,
    DEFAULT_TRANSPORT,
    MCP_SERVER_NAME,
    NETWORK_TRANSPORTS,
    NETWORK_TRANSPORTS_SDK_MAPPING,
    AppContext,
)

# Standalone-host provider implementation
from providers.static import StaticClusterProvider

# Configure logging
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(MCP_SERVER_NAME)


@click.command()
@click.option(
    "--connection-string",
    envvar="CB_CONNECTION_STRING",
    help="Couchbase connection string (required for operations)",
)
@click.option(
    "--username",
    envvar="CB_USERNAME",
    help="Couchbase database user (required for operations)",
)
@click.option(
    "--password",
    envvar="CB_PASSWORD",
    help="Couchbase database password (required for operations)",
)
@click.option(
    "--ca-cert-path",
    envvar="CB_CA_CERT_PATH",
    help="Path to the server trust store (CA certificate) file. The certificate at this path is used to verify the server certificate during the authentication process.",
)
@click.option(
    "--client-cert-path",
    envvar="CB_CLIENT_CERT_PATH",
    help="Path to the client certificate file used for mTLS authentication.",
)
@click.option(
    "--client-key-path",
    envvar="CB_CLIENT_KEY_PATH",
    help="Path to the client certificate key file used for mTLS authentication.",
)
@click.option(
    "--read-only-mode",
    envvar="CB_MCP_READ_ONLY_MODE",
    type=bool,
    default=DEFAULT_READ_ONLY_MODE,
    help="Enable read-only mode. When True (default), all write operations (KV and Query) are disabled and KV write tools are not loaded. Set to False to enable write operations.",
)
@click.option(
    "--read-only-query-mode",
    envvar=[
        "CB_MCP_READ_ONLY_QUERY_MODE",
        "READ_ONLY_QUERY_MODE",  # Deprecated
    ],
    type=bool,
    deprecated=True,
    default=DEFAULT_READ_ONLY_MODE,
    help="[DEPRECATED: Use --read-only-mode instead] Enable read-only query mode. Set to True (default) to allow only read-only queries. Can be set to False to allow data modification queries.",
)
@click.option(
    "--transport",
    envvar=[
        "CB_MCP_TRANSPORT",
        "MCP_TRANSPORT",  # Deprecated
    ],
    type=click.Choice(ALLOWED_TRANSPORTS),
    default=DEFAULT_TRANSPORT,
    help="Transport mode for the server (stdio, http or sse). Default is stdio",
)
@click.option(
    "--host",
    envvar="CB_MCP_HOST",
    default=DEFAULT_HOST,
    help="Host to run the server on (default: 127.0.0.1)",
)
@click.option(
    "--port",
    envvar="CB_MCP_PORT",
    default=DEFAULT_PORT,
    help="Port to run the server on (default: 8000)",
)
@click.option(
    "--disabled-tools",
    "disabled_tools",
    envvar="CB_MCP_DISABLED_TOOLS",
    help="Tools to disable. Accepts comma-separated tool names (e.g., 'tool_1,tool_2') "
    "or a file path containing one tool name per line.",
)
@click.option(
    "--confirmation-required-tools",
    "confirmation_required_tools",
    envvar="CB_MCP_CONFIRMATION_REQUIRED_TOOLS",
    help="Comma-separated tool names that require user confirmation before execution. "
    "Also accepts a file path containing one tool name per line. "
    "Requires the MCP client to support elicitation.",
)
@click.option(
    "--mcp-base-url",
    envvar="MCP_BASE_URL",
    default=None,
    help="Public base URL of this MCP server (e.g. https://api.yourcompany.com). "
    "Advertised in OAuth 2.0 protected-resource metadata. "
    "Required to enable auth.",
)
@click.option(
    "--auth-jwks-uri",
    envvar="AUTH_JWKS_URI",
    default=None,
    help="JWKS endpoint of the upstream identity provider, used to verify "
    "bearer JWT signatures (e.g. https://test.stytch.com/v1/sessions/jwks/<project_id>).",
)
@click.option(
    "--auth-issuer",
    envvar="AUTH_ISSUER",
    default=None,
    help="Expected JWT 'iss' claim value (e.g. stytch.com/<project_id>).",
)
@click.option(
    "--auth-audience",
    envvar="AUTH_AUDIENCE",
    default=None,
    help="Expected JWT 'aud' claim value. Omit to skip audience checks.",
)
@click.option(
    "--auth-authorization-server",
    envvar="AUTH_AUTHORIZATION_SERVER",
    default=None,
    help="Upstream OAuth authorization server URL advertised to MCP clients "
    "for OAuth metadata discovery and Dynamic Client Registration.",
)
@click.option(
    "--auth-required-scopes",
    envvar="AUTH_REQUIRED_SCOPES",
    default=None,
    help="Comma-separated OAuth scopes a token must carry to access this server.",
)
@click.version_option(package_name="couchbase-mcp-server")
@click.option(
    "--auth-upstream-metadata-url",
    envvar="AUTH_UPSTREAM_METADATA_URL",
    default=None,
    help="Enable metadata-proxy mode by fetching upstream AS metadata from "
    "this URL (typically the provider's OIDC discovery doc). When set, this "
    "server advertises itself as the AS and serves an augmented metadata doc "
    "at /.well-known/oauth-authorization-server. Use together with "
    "--auth-registration-endpoint when the upstream omits it.",
)
@click.option(
    "--auth-registration-endpoint",
    envvar="AUTH_REGISTRATION_ENDPOINT",
    default=None,
    help="Dynamic Client Registration endpoint URL to inject into the "
    "metadata-proxy response. Use this when the upstream provider supports "
    "DCR but doesn't advertise registration_endpoint in its OIDC doc.",
)
@click.pass_context
def main(
    ctx,
    connection_string,
    username,
    password,
    ca_cert_path,
    client_cert_path,
    client_key_path,
    read_only_mode,
    read_only_query_mode,
    transport,
    host,
    port,
    disabled_tools,
    confirmation_required_tools,
    mcp_base_url,
    auth_jwks_uri,
    auth_issuer,
    auth_audience,
    auth_authorization_server,
    auth_required_scopes,
    auth_upstream_metadata_url,
    auth_registration_endpoint,
):
    """Couchbase MCP Server"""

    (
        final_tools,
        configured_confirmation_tool_names,
        disabled_tool_names,
    ) = prepare_tools_for_registration(
        read_only_mode=read_only_mode,
        disabled_tools=disabled_tools,
        confirmation_required_tools=confirmation_required_tools,
    )

    # CLI-resolved configuration lives on AppContext, not in a module global.
    # This lets FastMCP's threadpool workers read it through ``ctx``.
    settings = {
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "ca_cert_path": ca_cert_path,
        "client_cert_path": client_cert_path,
        "client_key_path": client_key_path,
        "read_only_mode": read_only_mode,
        "read_only_query_mode": read_only_query_mode,
        "transport": transport,
        "host": host,
        "port": port,
        "disabled_tools": disabled_tool_names,
        "confirmation_required_tools": configured_confirmation_tool_names,
    }
    ctx.obj = settings

    @asynccontextmanager
    async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
        """Build the lifespan AppContext with settings captured from the CLI."""
        logger.info(
            f"MCP server initialized in lazy mode for tool discovery. "
            f"Modes: (read_only_mode={read_only_mode}, "
            f"read_only_query_mode={read_only_query_mode})"
        )
        app_context = AppContext(
            cluster_provider=StaticClusterProvider(settings=settings),
            settings=settings,
            read_only_mode=read_only_mode,
            read_only_query_mode=read_only_query_mode,
        )
        try:
            yield app_context
        except Exception as e:
            logger.error(f"Error in app lifespan: {e}")
            raise
        finally:
            if app_context.cluster_provider:
                app_context.cluster_provider.close()
            logger.info("Closing MCP server")

    # Map user-friendly transport names to SDK transport names
    sdk_transport = NETWORK_TRANSPORTS_SDK_MAPPING.get(transport, transport)

    required_scopes_list = (
        [s.strip() for s in auth_required_scopes.split(",") if s.strip()]
        if auth_required_scopes
        else None
    )
    auth = build_remote_auth(
        base_url=mcp_base_url,
        jwks_uri=auth_jwks_uri,
        issuer=auth_issuer,
        audience=auth_audience,
        authorization_server=auth_authorization_server,
        required_scopes=required_scopes_list,
        upstream_metadata_url=auth_upstream_metadata_url,
    )
    if auth is not None and transport not in NETWORK_TRANSPORTS:
        logger.warning(
            "Auth was configured but transport=%s; "
            "auth is only meaningful for network transports (http, sse).",
            transport,
        )

    mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan, auth=auth)

    # Metadata-proxy mode: serve an augmented AS metadata doc at the
    # well-known paths so clients discover registration_endpoint (and any
    # other fields the upstream omits).
    if auth is not None and auth_upstream_metadata_url:
        if not auth_registration_endpoint:
            logger.warning(
                "--auth-upstream-metadata-url is set without "
                "--auth-registration-endpoint; the augmented metadata "
                "will pass through unchanged. DCR clients may still fail."
            )
        extra_fields = {}
        if auth_registration_endpoint:
            extra_fields["registration_endpoint"] = auth_registration_endpoint
        metadata_handler = make_metadata_proxy_handler(
            upstream_metadata_url=auth_upstream_metadata_url,
            extra_fields=extra_fields,
        )

        @mcp.custom_route("/.well-known/oauth-authorization-server", methods=["GET"])
        async def _as_metadata(request):
            return await metadata_handler(request)

        @mcp.custom_route("/.well-known/openid-configuration", methods=["GET"])
        async def _oidc_metadata(request):
            return await metadata_handler(request)

        logger.info(
            "Metadata-proxy routes mounted at "
            "/.well-known/oauth-authorization-server and "
            "/.well-known/openid-configuration (upstream=%s, injecting=%s)",
            auth_upstream_metadata_url,
            sorted(extra_fields),
        )

    logger.info(
        f"Registering {len(final_tools)} tool(s) with modes (read_only_mode={read_only_mode}, "
        f"read_only_query_mode={read_only_query_mode})"
    )

    # Register tools; FastMCP 3.x add_tool has no annotations kwarg, so wrap first.
    for tool in final_tools:
        annotations = TOOL_ANNOTATIONS.get(tool.__name__)
        tool_obj = FunctionTool.from_function(tool, annotations=annotations)
        mcp.add_tool(tool_obj)

    logger.info(f"Registered {len(final_tools)} tool(s)")

    run_kwargs = {"host": host, "port": port} if transport in NETWORK_TRANSPORTS else {}
    mcp.run(transport=sdk_transport, **run_kwargs)  # type: ignore


if __name__ == "__main__":
    main()
