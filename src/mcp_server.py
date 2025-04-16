from datetime import timedelta
from typing import Any, Dict, List
from mcp.server.fastmcp import FastMCP, Context
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
import os
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import AsyncIterator

# Import the helper functions from meta.py
from .meta import (
    _get_cluster_info,
    _get_bucket_info,
    _list_fts_indexes,
    _list_n1ql_indexes,
)

MCP_SERVER_NAME = "couchbase"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(MCP_SERVER_NAME)


@dataclass
class AppContext:
    cluster: Cluster | None = None
    bucket: Any | None = None


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Initialize the Couchbase cluster and bucket for the MCP server."""
    # Get environment variables
    connection_string = os.getenv("CB_CONNECTION_STRING")
    username = os.getenv("CB_USERNAME")
    password = os.getenv("CB_PASSWORD")
    bucket_name = os.getenv("CB_BUCKET_NAME")

    # Validate environment variables
    missing_vars = []
    if not connection_string:
        logger.error(
            "Environment variable CB_CONNECTION_STRING with Couchbase connection string is not set"
        )
        missing_vars.append("CB_CONNECTION_STRING")
    if not username:
        logger.error(
            "Environment variable CB_USERNAME with Database username is not set"
        )
        missing_vars.append("CB_USERNAME")
    if not password:
        logger.error(
            "Environment variable CB_PASSWORD with Database password is not set"
        )
        missing_vars.append("CB_PASSWORD")
    if not bucket_name:
        logger.error(
            "Environment variable CB_BUCKET_NAME with Database bucket name is not set"
        )
        missing_vars.append("CB_BUCKET_NAME")
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
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

        bucket = cluster.bucket(bucket_name)
        yield AppContext(cluster=cluster, bucket=bucket)

    except Exception as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise


# Initialize MCP server
mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan)


# --- Metadata Tools ---

@mcp.tool()
def get_cluster_info(ctx: Context) -> Dict[str, Any]:
    """Get diagnostic information about the Couchbase cluster."""
    cluster = ctx.request_context.lifespan_context.cluster
    if not cluster:
        raise ValueError("Cluster connection not available in context.")
    try:
        return _get_cluster_info(cluster)
    except Exception as e:
        logger.error(f"Tool error getting cluster info: {type(e).__name__} - {e}")
        # Re-raise to signal failure to the MCP framework/caller
        raise


@mcp.tool()
def get_bucket_info(ctx: Context) -> Dict[str, Any]:
    """Get configuration settings for the current Couchbase bucket."""
    bucket = ctx.request_context.lifespan_context.bucket
    if not bucket:
        raise ValueError("Bucket connection not available in context.")
    try:
        return _get_bucket_info(bucket)
    except Exception as e:
        logger.error(f"Tool error getting bucket info: {type(e).__name__} - {e}")
        raise


@mcp.tool()
def list_fts_indexes(ctx: Context) -> List[Dict[str, Any]]:
    """List all Full-Text Search (FTS) indexes in the cluster."""
    cluster = ctx.request_context.lifespan_context.cluster
    if not cluster:
        raise ValueError("Cluster connection not available in context.")
    try:
        return _list_fts_indexes(cluster)
    except Exception as e:
        logger.error(f"Tool error listing FTS indexes: {type(e).__name__} - {e}")
        raise


@mcp.tool()
def list_n1ql_indexes(ctx: Context) -> List[Dict[str, Any]]:
    """List all N1QL (Query) indexes for the current bucket."""
    cluster = ctx.request_context.lifespan_context.cluster
    bucket = ctx.request_context.lifespan_context.bucket
    if not cluster:
        raise ValueError("Cluster connection not available in context.")
    if not bucket:
        raise ValueError("Bucket connection not available in context.")
    try:
        return _list_n1ql_indexes(cluster, bucket.name)
    except Exception as e:
        logger.error(f"Tool error listing N1QL indexes: {type(e).__name__} - {e}")
        raise


# --- Existing Tools ---

@mcp.tool()
def get_scopes_and_collections_in_bucket(ctx: Context) -> dict[str, list[str]]:
    """Get the names of all scopes and collections in the bucket.
    Returns a dictionary with scope names as keys and lists of collection names as values.
    """
    bucket = ctx.request_context.lifespan_context.bucket
    try:
        scopes_collections = {}
        collection_manager = bucket.collections()
        scopes = collection_manager.get_all_scopes()
        for scope in scopes:
            collection_names = [c.name for c in scope.collections]
            scopes_collections[scope.name] = collection_names
        return scopes_collections
    except Exception as e:
        logger.error(f"Error getting scopes and collections: {e}")
        raise


@mcp.tool()
def get_schema_for_collection(
    ctx: Context, scope_name: str, collection_name: str
) -> dict[str, Any]:
    """Get the schema for a collection in the specified scope.
    Returns a dictionary with the schema returned by running INFER on the Couchbase collection.
    """
    try:
        query = f"INFER {collection_name}"
        result = run_sql_plus_plus_query(ctx, scope_name, query)
        return result
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise


@mcp.tool()
def run_sql_plus_plus_query(
    ctx: Context, scope_name: str, query: str
) -> list[dict[str, Any]]:
    """Run a SQL++ query on a scope and return the results as a list of JSON objects."""
    bucket = ctx.request_context.lifespan_context.bucket

    try:
        scope = bucket.scope(scope_name)
        result = scope.query(query)
        results = []
        for row in result:
            results.append(row)
        return results
    except Exception as e:
        logger.error(f"Error running query: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    mcp.run(transport="stdio")
