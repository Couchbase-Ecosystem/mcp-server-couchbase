"""
Couchbase MCP Server
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import click
from fastmcp import FastMCP

# Import tools
from tools import get_tools

# Import utilities
from utils import (
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
    get_settings,
    parse_disabled_tools,
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(MCP_SERVER_NAME)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Initialize the MCP server context without establishing database connections."""
    # Get configuration from Click context
    settings = get_settings()
    read_only_mode = settings.get("read_only_mode", True)
    read_only_query_mode = settings.get("read_only_query_mode", True)

    # Note: We don't validate configuration here to allow tool discovery
    # Configuration will be validated when tools are actually used
    logger.info(
        f"MCP server initialized in lazy mode for tool discovery. "
        f"Modes: (read_only_mode={read_only_mode}, read_only_query_mode={read_only_query_mode})"
    )
    app_context = None
    try:
        app_context = AppContext(
            read_only_mode=read_only_mode, read_only_query_mode=read_only_query_mode
        )
        yield app_context

    except Exception as e:
        logger.error(f"Error in app lifespan: {e}")
        raise
    finally:
        # Close the cluster connection
        if app_context and app_context.cluster:
            app_context.cluster.close()
        logger.info("Closing MCP server")


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
    default=None,
    help="Path to the server trust store (CA certificate) file. The certificate at this path is used to verify the server certificate during the authentication process.",
)
@click.option(
    "--client-cert-path",
    envvar="CB_CLIENT_CERT_PATH",
    default=None,
    help="Path to the client certificate file used for mTLS authentication.",
)
@click.option(
    "--client-key-path",
    envvar="CB_CLIENT_KEY_PATH",
    default=None,
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
@click.version_option(package_name="couchbase-mcp-server")
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
):
    """Couchbase MCP Server"""
    # Store configuration in context
    ctx.obj = {
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
    }

    # Get tools based on mode settings
    # When read_only_mode is True, KV write tools are not loaded
    tools = get_tools(read_only_mode=read_only_mode)

    # Parse and validate disabled tools from CLI/environment variable
    all_tool_names = {tool.__name__ for tool in tools}
    disabled_tool_names = parse_disabled_tools(disabled_tools, all_tool_names)

    if disabled_tool_names:
        logger.info(
            f"Disabled {len(disabled_tool_names)} tool(s): {sorted(disabled_tool_names)}"
        )

    # Filter out disabled tools
    enabled_tools = [tool for tool in tools if tool.__name__ not in disabled_tool_names]

    # Map user-friendly transport names to SDK transport names
    sdk_transport = NETWORK_TRANSPORTS_SDK_MAPPING.get(transport, transport)

    # If the transport is network based, we need to pass the host and port to the MCP server
    config = (
        {
            "host": host,
            "port": port,
        }
        if transport in NETWORK_TRANSPORTS
        else {}
    )

    mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan, **config)

    logger.info(
        f"Registering {len(enabled_tools)} tool(s) with modes (read_only_mode={read_only_mode}, "
        f"read_only_query_mode={read_only_query_mode})"
    )

    # Register only enabled tools
    for tool in enabled_tools:
        mcp.tool()(tool)

    logger.info(f"Registered {len(enabled_tools)} tool(s)")

    # Run the server
    mcp.run(transport=sdk_transport)  # type: ignore


if __name__ == "__main__":
    main()
