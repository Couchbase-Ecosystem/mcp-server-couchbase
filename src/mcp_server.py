"""
Couchbase MCP Server
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Optional

import click
from mcp.server.fastmcp import FastMCP
from mcp.server.session import ServerSession
from mcp.types import Tool as MCPTool

# Import catalog manager (background thread)
from catalog_manager import (
    start_catalog_thread,
    stop_catalog_thread,
)


# Import enrichment functions (MCP server thread component)
from enrichment import start_enrichment_cron, stop_enrichment_cron

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
    set_settings,
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(MCP_SERVER_NAME)

_cached_session: Optional[ServerSession] = None

class MCPServer(FastMCP):
    """Extended FastMCP server with session caching for sampling support."""
    
    def set_transport(self, transport: str) -> None:
        """Set the transport mode for this server."""
        self._transport = transport

    async def list_tools(self) -> list[MCPTool]:
        """List all available tools and cache the session for sampling support (stdio only)."""
        # Cache the session from the request context only for stdio transport
        global _cached_session
        if self._transport == "stdio" and _cached_session is None:
            try:
                # Access the session from the request context
                ctx = self.get_context()
                _cached_session = ctx.session  # type: ignore
                # Start enrichment cron in the background
                start_enrichment_cron(_cached_session)

            except LookupError:
                # Context not available (shouldn't happen in normal flow)
                logger.error("RequestContext not available in list_tools")
        
        # Call the parent implementation
        return await super().list_tools()


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Initialize the MCP server context and start the catalog background thread."""
    # Get configuration from Click context
    settings = get_settings()
    read_only_query_mode = settings.get("read_only_query_mode", True)

    # Note: We don't validate configuration here to allow tool discovery
    # Configuration will be validated when tools are actually used
    logger.info("MCP server initialized in lazy mode for tool discovery.")
    
    # Start the catalog background thread
    logger.info("Starting catalog background thread")
    start_catalog_thread()
    
    app_context = None
    try:
        app_context = AppContext(read_only_query_mode=read_only_query_mode)
        yield app_context

    except Exception as e:
        logger.error(f"Error in app lifespan: {e}")
        raise
    finally:
        # Stop the enrichment cron
        logger.info("Stopping catalog enrichment cron")
        try:
            await stop_enrichment_cron()
        except Exception as e:
            logger.error(f"Error stopping enrichment cron: {e}")
        
        # Stop the catalog background thread
        logger.info("Stopping catalog background thread")
        stop_catalog_thread()
        
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
    ca_cert_path,
    client_cert_path,
    client_key_path,
    read_only_query_mode,
    transport,
    host,
    port,
):
    """Couchbase MCP Server"""
    # Store configuration in context
    set_settings({
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "ca_cert_path": ca_cert_path,
        "client_cert_path": client_cert_path,
        "client_key_path": client_key_path,
        "read_only_query_mode": read_only_query_mode,
        "transport": transport,
        "host": host,
        "port": port,
    })

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

    mcp = MCPServer(MCP_SERVER_NAME, lifespan=app_lifespan, **config)

    # Set the transport mode for session caching
    mcp.set_transport(transport)

    # Register all tools
    for tool in ALL_TOOLS:
        mcp.add_tool(tool)

    # For stdio transport, we'll cache the session for sampling support
    if transport == "stdio":
        logger.info("Running in stdio mode with session caching enabled for sampling")
    
    # Run the server
    mcp.run(transport=sdk_transport)  # type: ignore


if __name__ == "__main__":
    main()
