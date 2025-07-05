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
    cluster: Cluster | None = None
    bucket: Any | None = None
    read_only_query_mode: bool = True


def validate_required_param(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> str:
    if not value or value.strip() == "":
        raise click.BadParameter(f"{param.name} cannot be empty")
    return value


def get_settings() -> dict:
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
    ctx.obj = {
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "bucket_name": bucket_name,
        "read_only_query_mode": read_only_query_mode,
    }
    mcp.run(transport=transport)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    settings = get_settings()
    connection_string = settings.get("connection_string")
    username = settings.get("username")
    password = settings.get("password")
    bucket_name = settings.get("bucket_name")
    read_only_query_mode = settings.get("read_only_query_mode")

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
    if not bucket_name:
        logger.error("Couchbase bucket name is not set")
        missing_vars.append("bucket_name")

    if missing_vars:
        raise ValueError(f"Missing required configuration: {', '.join(missing_vars)}")

    app_context = AppContext(read_only_query_mode=read_only_query_mode)
    cluster = None

    try:
        logger.info("Creating Couchbase cluster connection (once)...")
        auth = PasswordAuthenticator(username, password)
        options = ClusterOptions(auth)
        options.apply_profile("wan_development")
        cluster = Cluster(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=5))
        app_context.cluster = cluster
        app_context.bucket = cluster.bucket(bucket_name)
        logger.info("Successfully connected to Couchbase cluster")
    except Exception as e:
        logger.warning(
            f"Could not connect to Couchbase during startup: {e}. "
            "Connection will be attempted on first use."
        )

    try:
        yield app_context
    finally:
        if cluster:
            try:
                logger.info("Closing Couchbase cluster connection.")
                cluster.close()
            except Exception as e:
                logger.error(f"Error closing Couchbase cluster: {e}")


def get_cluster(ctx: Context) -> Cluster:
    app_ctx = ctx.request_context.lifespan_context
    if app_ctx.cluster:
        return app_ctx.cluster

    settings = get_settings()
    connection_string = settings.get("connection_string")
    username = settings.get("username")
    password = settings.get("password")

    try:
        logger.info("Creating Couchbase cluster connection...")
        auth = PasswordAuthenticator(username, password)
        options = ClusterOptions(auth)
        options.apply_profile("wan_development")
        cluster = Cluster(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=5))
        app_ctx.cluster = cluster
        return cluster
    except Exception as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise


def _get_bucket(ctx: Context) -> Any:
    app_ctx = ctx.request_context.lifespan_context
    if app_ctx.bucket:
        return app_ctx.bucket

    if not app_ctx.cluster:
        app_ctx.cluster = get_cluster(ctx)

    settings = get_settings()
    bucket_name = settings.get("bucket_name")
    app_ctx.bucket = app_ctx.cluster.bucket(bucket_name)
    return app_ctx.bucket


# Initialize MCP server
mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan)


@mcp.tool()
def get_scopes_and_collections_in_bucket(ctx: Context) -> dict[str, list[str]]:
    bucket = _get_bucket(ctx)
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
    try:
        bucket = _get_bucket(ctx)
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
    try:
        bucket = _get_bucket(ctx)
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
    try:
        bucket = _get_bucket(ctx)
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
    read_only_query_mode = ctx.request_context.lifespan_context.read_only_query_mode
    logger.info(f"Running SQL++ queries in read-only mode: {read_only_query_mode}")

    try:
        bucket = _get_bucket(ctx)
        scope = bucket.scope(scope_name)

        results = []

        if read_only_query_mode:
            data_modification_query = modifies_data(parse_sqlpp(query))
            structure_modification_query = modifies_structure(parse_sqlpp(query))

            if data_modification_query:
                logger.error("Data modification query is not allowed in read-only mode")
                raise ValueError("Data modification query is not allowed in read-only mode")
            if structure_modification_query:
                logger.error("Structure modification query is not allowed in read-only mode")
                raise ValueError("Structure modification query is not allowed in read-only mode")

        result = scope.query(query)
        for row in result:
            results.append(row)
        return results
    except Exception as e:
        logger.error(f"Error running query: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
