from datetime import timedelta
from typing import Any
from mcp.server.fastmcp import FastMCP, Context
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import AsyncIterator
import click
from couchbase_mcp import logger, set_mcp, MCP_SERVER_NAME
import json
import importlib
import os


# Configure logging


@dataclass
class AppContext:
    """Context for the MCP server."""

    cluster: Cluster | None = None
    read_only_query_mode: bool = True


def validate_required_param(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> str:
    """Validate that a required parameter is not empty."""
    if not value or value.strip() == "":
        raise click.BadParameter(f"{param.name} cannot be empty")
    return value


def get_settings() -> dict:
    """Get settings from Click context."""
    ctx = click.get_current_context()
    return ctx.obj or {}


@click.command()
@click.option(
    "--connection-string",
    envvar="CB_CONNECTION_STRING",
    help="Couchbase connection string",
    callback=validate_required_param,
)
@click.option(
    "--username",
    envvar="CB_USERNAME",
    help="Couchbase database user",
    callback=validate_required_param,
)
@click.option(
    "--password",
    envvar="CB_PASSWORD",
    help="Couchbase database password",
    callback=validate_required_param,
)

@click.option(
    "--read-only-query-mode",
    envvar="READ_ONLY_QUERY_MODE",
    type=bool,
    default=True,
    help="Enable read-only query mode. Set to True (default) to allow only read-only queries. Can be set to False to allow data modification queries.",
)
@click.option(
    "--transport",
    envvar="MCP_TRANSPORT",
    type=click.Choice(["stdio", "sse"]),
    default="stdio",
    help="Transport mode for the server (stdio or sse)",
)
@click.pass_context
def main(
    ctx,
    connection_string,
    username,
    password,
    read_only_query_mode,
    transport,
):
    """Couchbase MCP Server"""
    ctx.obj = {
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "read_only_query_mode": read_only_query_mode,
    }
    mcp.run(transport=transport)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Initialize the Couchbase cluster and bucket for the MCP server."""
    # Get configuration from Click context
    settings = get_settings()

    connection_string = settings.get("connection_string")
    username = settings.get("username")
    password = settings.get("password")
    read_only_query_mode = settings.get("read_only_query_mode")

    # Validate configuration
    missing_vars = []
    if not connection_string:
        logger.error("Couchbase connection string is not set")
        missing_vars.append("connection_string")
    if not username:
        logger.error("Couchbase database user is not set")
        missing_vars.append("username")
    if not password:
        logger.error("Couchbase database password is not set")
        missing_vars.append("password")


    if missing_vars:
        error_msg = f"Missing required configuration: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        logger.info("Creating Couchbase cluster connection...")
        auth = PasswordAuthenticator(username, password)

        options = ClusterOptions(auth)
        options.apply_profile("wan_development")

        cluster = Cluster(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=5))
        logger.info("Successfully connected to Couchbase cluster")

        yield AppContext(
            cluster=cluster, 
            read_only_query_mode=read_only_query_mode
        )

    except Exception as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise


# Initialize MCP server
mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan)
set_mcp(mcp)




def load_config(path="mcp_config.json"):

    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(SCRIPT_DIR, path)
    try:
        with open(file_path, "r") as f:
            config = json.load(f)
            return config
    except FileNotFoundError as e:
        logger.error(f"Config file not found: {file_path}")
        raise e
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        raise e

def load_tool_modules(tools):
    for mod_name in tools:
        try:
            importlib.import_module(mod_name)
            logger.info(f"Loaded tool module: {mod_name}")
        except ModuleNotFoundError as e:
            logger.warning(f"Could not find tool module '{mod_name}': {e}")
        except Exception as e:
            logger.error(f"Error importing tool module '{mod_name}': {e}")


if __name__ == "__main__":
    importlib.import_module("tools") #load default tools
    config = load_config()
    tools = config.get("tools", [])
    load_tool_modules(tools)
    main()
