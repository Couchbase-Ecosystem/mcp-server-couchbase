from datetime import timedelta
from typing import Any
from mcp.server.fastmcp import FastMCP, Context
from couchbase.cluster import Cluster, Bucket 
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import AsyncIterator
from lark_sqlpp import modifies_data, modifies_structure, parse_sqlpp
import click

MCP_SERVER_NAME = "couchbase"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(MCP_SERVER_NAME)


@dataclass
class AppContext:
    """Context for the MCP server."""

    cluster: Cluster | None = None
    bucket: Bucket | None = None
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


def validate_connection_config() -> None:
    """Validate that all required parameters for the MCP server are available when needed."""
    settings = get_settings()
    missing_vars = []
    
    if not settings.get("connection_string"):
        missing_vars.append("connection_string")
    if not settings.get("username"):
        missing_vars.append("username") 
    if not settings.get("password"):
        missing_vars.append("password")
    if not settings.get("bucket_name"):
        missing_vars.append("bucket_name")
    
    if missing_vars:
        error_msg = f"Missing required parameters for the MCP server: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)


def ensure_bucket_connection(ctx: Context) -> Bucket:
    """Ensure bucket connection is established and return the bucket object."""
    validate_connection_config()
    app_context = ctx.request_context.lifespan_context
    if not app_context.bucket:
        set_bucket_in_lifespan_context(ctx)
    return app_context.bucket


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
    "--bucket-name",
    envvar="CB_BUCKET_NAME",
    help="Couchbase bucket name",
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
    bucket_name,
    read_only_query_mode,
    transport,
):
    """Couchbase MCP Server"""
    ctx.obj = {
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "bucket_name": bucket_name,
        "read_only_query_mode": read_only_query_mode,
    }
    mcp.run(transport=transport)

def connect_to_couchbase_cluster(connection_string:str, username:str, password:str) -> Cluster | None:
    """Connect to Couchbase cluster and return the cluster object if successful, None otherwise.
    If the connection fails, it will raise an exception.
    """ 
    
    try:
        logger.info("Connecting to Couchbase cluster...")
        auth = PasswordAuthenticator(username, password)
        options = ClusterOptions(auth)
        options.apply_profile("wan_development")

        cluster = Cluster(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=5))

        logger.info("Successfully connected to Couchbase cluster")
        return cluster
    except Exception as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise 
    
def connect_to_bucket(cluster:Cluster, bucket_name:str) -> Bucket | None:
    """Connect to a bucket and return the bucket object if successful, None otherwise.
    If the operation fails, it will raise an exception.
    """
    try:
        logger.info(f"Connecting to bucket: {bucket_name}")
        bucket = cluster.bucket(bucket_name)
        return bucket
    except Exception as e:
        logger.error(f"Failed to connect to bucket: {e}")
        raise

def set_cluster_in_lifespan_context(ctx: Context) -> None:
    """Set the cluster in the lifespan context.
    If the cluster is not set, it will try to connect to the cluster using the connection string, username, and password.
    If the connection fails, it will raise an exception.
    """
    if not ctx.request_context.lifespan_context.cluster:
        try:
            settings = get_settings()
            connection_string = settings.get("connection_string")
            username = settings.get("username")
            password = settings.get("password")
            cluster = connect_to_couchbase_cluster(connection_string, username, password)
            ctx.request_context.lifespan_context.cluster = cluster
        except Exception as e:
            logger.error(f"Failed to connect to Couchbase: {e} \n Please check your connection string, username, password, and bucket name.")
            raise

def set_bucket_in_lifespan_context(ctx: Context) -> None:
    """Set the bucket in the lifespan context.
    If the bucket is not set, it will try to connect to the bucket using the cluster object in the lifespan context.
    If the cluster is not set, it will try to connect to the cluster using the connection string, username, and password.
    If the connection fails, it will raise an exception.
    """
    settings = get_settings()
    bucket_name = settings.get("bucket_name")
    connection_string = settings.get("connection_string")
    username = settings.get("username")
    password = settings.get("password")

    # If the bucket is not set, try to connect to the bucket using the cluster object in the lifespan context
    app_context = ctx.request_context.lifespan_context

    try:
        # If the cluster is not set, try to connect to the cluster using the connection string, username, and password
        if app_context.cluster:
            cluster = app_context.cluster
        else:
            cluster = connect_to_couchbase_cluster(connection_string, username, password)
            app_context.cluster = cluster

        # Try to connect to the bucket using the cluster object
        bucket = connect_to_bucket(cluster, bucket_name)
        app_context.bucket = bucket
    except Exception as e:
        logger.error(f"Failed to connect to bucket: {e} \n Please check your bucket name and credentials.")
        raise 

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
        if app_context.cluster:
            app_context.cluster.close()
        logger.info("Closing MCP server")


# Initialize MCP server
mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan)


# Tools
@mcp.tool()
def get_server_configuration_status(ctx: Context) -> dict[str, Any]:
    """Get the server status and configuration without establishing connections.
    This tool can be used to verify the server is running and check configuration.
    """
    settings = get_settings()
    
    # Don't expose sensitive information like passwords
    configuration = {
        "connection_string": settings.get("connection_string", "Not set"),
        "username": settings.get("username", "Not set"),
        "bucket_name": settings.get("bucket_name", "Not set"),
        "read_only_query_mode": settings.get("read_only_query_mode", True),
        "password_configured": bool(settings.get("password")),
    }
    
    app_context = ctx.request_context.lifespan_context
    connection_status = {
        "cluster_connected": app_context.cluster is not None,
        "bucket_connected": app_context.bucket is not None,
    }
    
    return {
        "server_name": MCP_SERVER_NAME,
        "status": "running",
        "configuration": configuration,
        "connections": connection_status,
    }


@mcp.tool()
def test_connection(ctx: Context) -> dict[str, Any]:
    """Test the connection to Couchbase cluster and bucket.
    Returns connection status and basic cluster information.
    """
    try:
        bucket = ensure_bucket_connection(ctx)
        
        # Test basic connectivity by getting bucket name
        bucket_name = bucket.name
        
        return {
            "status": "success",
            "cluster_connected": True,
            "bucket_connected": True,
            "bucket_name": bucket_name,
            "message": "Successfully connected to Couchbase cluster and bucket",
        }
    except Exception as e:
        return {
            "status": "error",
            "cluster_connected": False,
            "bucket_connected": False,
            "error": str(e),
            "message": "Failed to connect to Couchbase",
        }


@mcp.tool()
def get_scopes_and_collections_in_bucket(ctx: Context) -> dict[str, list[str]]:
    """Get the names of all scopes and collections in the bucket.
    Returns a dictionary with scope names as keys and lists of collection names as values.
    """
    bucket = ensure_bucket_connection(ctx)
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
def get_document_by_id(
    ctx: Context, scope_name: str, collection_name: str, document_id: str
) -> dict[str, Any]:
    """Get a document by its ID from the specified scope and collection."""
    bucket = ensure_bucket_connection(ctx)
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        result = collection.get(document_id)
        return result.content_as[dict]
    except Exception as e:
        logger.error(f"Error getting document {document_id}: {e}")
        raise


@mcp.tool()
def upsert_document_by_id(
    ctx: Context,
    scope_name: str,
    collection_name: str,
    document_id: str,
    document_content: dict[str, Any],
) -> bool:
    """Insert or update a document by its ID.
    Returns True on success, False on failure."""
    bucket = ensure_bucket_connection(ctx)
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        collection.upsert(document_id, document_content)
        logger.info(f"Successfully upserted document {document_id}")
        return True
    except Exception as e:
        logger.error(f"Error upserting document {document_id}: {e}")
        return False


@mcp.tool()
def delete_document_by_id(
    ctx: Context, scope_name: str, collection_name: str, document_id: str
) -> bool:
    """Delete a document by its ID.
    Returns True on success, False on failure."""
    bucket = ensure_bucket_connection(ctx)
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        collection.remove(document_id)
        logger.info(f"Successfully deleted document {document_id}")
        return True
    except Exception as e:
        logger.error(f"Error deleting document {document_id}: {e}")
        return False


@mcp.tool()
def run_sql_plus_plus_query(
    ctx: Context, scope_name: str, query: str
) -> list[dict[str, Any]]:
    """Run a SQL++ query on a scope and return the results as a list of JSON objects."""
    bucket = ensure_bucket_connection(ctx)
    app_context = ctx.request_context.lifespan_context
    read_only_query_mode = app_context.read_only_query_mode
    logger.info(f"Running SQL++ queries in read-only mode: {read_only_query_mode}")

    try:
        scope = bucket.scope(scope_name)

        results = []
        # If read-only mode is enabled, check if the query is a data or structure modification query
        if read_only_query_mode:
            data_modification_query = modifies_data(parse_sqlpp(query))
            structure_modification_query = modifies_structure(parse_sqlpp(query))

            if data_modification_query:
                logger.error("Data modification query is not allowed in read-only mode")
                raise ValueError(
                    "Data modification query is not allowed in read-only mode"
                )
            if structure_modification_query:
                logger.error(
                    "Structure modification query is not allowed in read-only mode"
                )
                raise ValueError(
                    "Structure modification query is not allowed in read-only mode"
                )

        # Run the query if it is not a data or structure modification query
        if not read_only_query_mode or not (
            data_modification_query or structure_modification_query
        ):
            result = scope.query(query)
            for row in result:
                results.append(row)
            return results
    except Exception as e:
        logger.error(f"Error running query: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
