"""
Couchbase MCP Server
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator
import click
from mcp.server.fastmcp import FastMCP

# Import utilities
from utils import (
    get_settings, 
    AppContext,
    MCP_SERVER_NAME,
    DEFAULT_TRANSPORT,
    DEFAULT_READ_ONLY_MODE,
    DEFAULT_LOG_LEVEL,
)

# Import tools
from tools import ALL_TOOLS

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
    envvar="READ_ONLY_QUERY_MODE",
    type=bool,
    default=DEFAULT_READ_ONLY_MODE,
    help="Enable read-only query mode. Set to True (default) to allow only read-only queries. Can be set to False to allow data modification queries.",
)
@click.option(
    "--transport",
    envvar="MCP_TRANSPORT",
    type=click.Choice(["stdio", "sse"]),
    default=DEFAULT_TRANSPORT,
    help="Transport mode for the server (stdio or sse)",
)
@click.pass_context
def main(
    ctx,
    connection_string,
    username,
    password,
    bucket_name,
    read_only_query_mode,
    transport,
):
    """Couchbase MCP Server"""
    # Store configuration in context
    ctx.obj = {
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "bucket_name": bucket_name,
        "read_only_query_mode": read_only_query_mode,
    }
    
    # Create MCP server inside main()
    mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan)
    
    # Register all tools
    for tool in ALL_TOOLS:
        mcp.add_tool(tool)
    
    # Run the server
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
