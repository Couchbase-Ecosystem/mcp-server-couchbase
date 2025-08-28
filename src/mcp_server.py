"""
Couchbase MCP Server
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import click
from mcp.server.fastmcp import FastMCP

# Import tools
from tools import ALL_TOOLS

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
    read_only_query_mode = settings.get("read_only_query_mode", True)

    # Note: We don't validate configuration here to allow tool discovery
    # Configuration will be validated when tools are actually used
    logger.info("MCP server initialized in lazy mode for tool discovery.")
    app_context = None
    try:
        app_context = AppContext(read_only_query_mode=read_only_query_mode)
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
    "--bucket-name",
    envvar="CB_BUCKET_NAME",
    help="Couchbase bucket name (required for operations)",
)
@click.option(
    "--read-only-query-mode",
    envvar=[
        "CB_MCP_READ_ONLY_QUERY_MODE",
        "READ_ONLY_QUERY_MODE",  # Deprecated
    ],
    type=bool,
    default=DEFAULT_READ_ONLY_MODE,
    help="Enable read-only query mode. Set to True (default) to allow only read-only queries. Can be set to False to allow data modification queries.",
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
@click.version_option(package_name="couchbase-mcp-server")
@click.pass_context
def main(
    ctx,
    connection_string,
    username,
    password,
    bucket_name,
    read_only_query_mode,
    transport,
    host,
    port,
):
    """Couchbase MCP Server"""
    # Store configuration in context
    ctx.obj = {
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "bucket_name": bucket_name,
        "read_only_query_mode": read_only_query_mode,
        "transport": transport,
        "host": host,
        "port": port,
    }

    # Map user-friendly transport names to SDK transport names
    sdk_transport = NETWORK_TRANSPORTS_SDK_MAPPING.get(transport, transport)

    # If the transport is network based, we need to pass the host and port to the MCP server
    config = (
        {
            "host": host,
            "port": port,
            "transport": sdk_transport,
        }
        if transport in NETWORK_TRANSPORTS
        else {}
    )

    mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan, **config)

    # Register all tools
    for tool in ALL_TOOLS:
        mcp.add_tool(tool)

    # Run the server
    mcp.run(transport=sdk_transport)  # type: ignore


if __name__ == "__main__":
    main()
