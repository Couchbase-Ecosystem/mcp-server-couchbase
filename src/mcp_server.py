from datetime import timedelta
from typing import Any
from mcp.server.fastmcp import FastMCP, Context
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator, CertificateAuthenticator
from couchbase.options import ClusterOptions
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import AsyncIterator
from lark_sqlpp import modifies_data, modifies_structure, parse_sqlpp
import click
import os

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
    read_only_query_mode: bool = True
    username: str = None
    password: str = None
    connection_string: str = None
    ca_cert_path : str = None
    use_tls : bool = True
    client_cert_path : str = None


def validate_required_param(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> str:
    """Validate that a required parameter is not empty."""
    if not value or value.strip() == "":
        raise click.BadParameter(f"{param.name} cannot be empty")
    return value

def validate_authentication_method(params : dict ) -> bool:
    """Util function to verify either user/password combination OR client certificates have been included"""
    username = params.get("username")
    password = params.get("password")
    client_cert_path = params.get("client_cert_path")
    ca_cert_path = params.get("ca_cert_path")

    # Strip values to check for empty strings
    if username is not None:
        username = username.strip()
    if password is not None:
        password = password.strip()

    if client_cert_path:
        client_cert = os.path.join(client_cert_path, "client.pem")
        client_key = os.path.join(client_cert_path, "client.key")

        if not os.path.isfile(client_cert) or not os.path.isfile(client_key):
            raise click.BadParameter(
                f"Client certificate files not found in {client_cert_path}. Required: client.pem and client.key."
            )

        if username or password or username == "" or password =="":
            raise click.BadParameter(
                "You must use either a client certificate or username/password, not both."
            )

    elif username or password:
        if not username or not password:
            raise click.BadParameter(
                "Both username and password must be provided and non-empty if using basic authentication."
            )
    else:
        raise click.BadParameter(
            "You must provide either a client certificate path or username/password combination, neither received."
        )

    if not ca_cert_path:
        logger.warning(f"A trusted CA certificate has not been provided, using local trust store for TLS connections")



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
    #callback=validate_required_param,
)
@click.option(
    "--password",
    envvar="CB_PASSWORD",
    help="Couchbase database password",
    #callback=validate_required_param,
)

@click.option(
    '--ca-cert-path',
    envvar="CA_CERT_PATH",
    type=click.Path(exists=True),
    default=None,
    help='Path to Server TLS certificate, required for API calls.')

@click.option(
    "--client-cert-path",
    envvar="CLIENT_CERT_PATH",
    default=None,
    help="Path to client.key and client.pem files for mtls client authentication",
    #callback=validate_required_param,
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
    ca_cert_path,
    client_cert_path,
    transport,
):
    """Couchbase MCP Server"""
    ctx.obj = {
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "read_only_query_mode": read_only_query_mode,
        "ca_cert_path" : ca_cert_path,
        "client_cert_path" : client_cert_path
    }
    try:
        validate_authentication_method(ctx.obj)
    except Exception as e:
        logger.error(f"Failed to validate auth method params: {e}")
        raise 
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
    ca_cert_path = settings.get("ca_cert_path")
    client_cert_path = settings.get("client_cert_path")
    use_tls = True
    if "://" in connection_string:
        protocol = connection_string.split("://", 1)[0]
        use_tls = (protocol[-1] == 's')


    try:
        logger.info("Creating Couchbase cluster connection...")
        #use client cert if provided, else user/password
        if client_cert_path:
                    
            tls_conf = {
                "cert_path" :  os.path.join(client_cert_path, "client.pem"),
                "key_path" :  os.path.join(client_cert_path, "client.key"),
            }
            #set ca cert as trust store if provided
            if ca_cert_path:
                tls_conf["trust_store_path"] = ca_cert_path
            auth = CertificateAuthenticator(**tls_conf)
        else:
            auth = PasswordAuthenticator(username, password, cert_path = ca_cert_path)

        options = ClusterOptions(auth)
        options.apply_profile("wan_development")

        cluster = Cluster(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=5))
        logger.info("Successfully connected to Couchbase cluster")

        yield AppContext(
            cluster=cluster, 
            ca_cert_path = ca_cert_path,
            connection_string = connection_string,
            username=username,
            password=password,
            read_only_query_mode=read_only_query_mode,
            use_tls=use_tls,
            client_cert_path=client_cert_path
        )

    except Exception as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise


# Initialize MCP server
mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan)



# Tools

@mcp.tool()
def get_list_of_buckets_with_settings(
    ctx: Context
) -> list[str]:
    """Get the list of buckets from the Couchbase cluster, including their bucket settings.
    Returns a list of bucket setting objects.
    """
    cluster = ctx.request_context.lifespan_context.cluster
    result=[]
    try:
        bucket_manager = cluster.buckets()
        buckets = bucket_manager.get_all_buckets()
        for b in buckets:
            result.append(b)
        return result
    except Exception as e:
        logger.error(f"Error getting bucket names: {e}")
        raise 


@mcp.tool()
def get_scopes_and_collections_in_bucket(ctx: Context, bucket_name: str) -> dict[str, list[str]]:
    """Get the names of all scopes and collections for a specified bucket.
    Returns a dictionary with scope names as keys and lists of collection names as values.
    """
    cluster = ctx.request_context.lifespan_context.cluster

    try:
        bucket = cluster.bucket(bucket_name)
    except Exception as e:
        logger.error(f"Error accessing bucket: {e}")
        raise ValueError("Tool does not have access to bucket, or bucket does not exist.")
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
    ctx: Context, bucket_name: str, scope_name: str, collection_name: str
) -> dict[str, Any]:
    """Get the schema for a collection in the specified scope of a specified bucket.
    Returns a dictionary with the schema returned by running INFER on the Couchbase collection.
    """
    try:
        query = f"INFER {collection_name}"
        result = run_sql_plus_plus_query(ctx, bucket_name, scope_name, query)
        return result
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise


@mcp.tool()
def get_document_by_id(
    ctx: Context, bucket_name: str, scope_name: str, collection_name: str, document_id: str
) -> dict[str, Any]:
    """Get a document by its ID from the specified bucket, scope and collection."""
    cluster = ctx.request_context.lifespan_context.cluster
    try:
        bucket = cluster.bucket(bucket_name)
    except Exception as e:
        logger.error(f"Error accessing bucket: {e}")
        raise ValueError("Tool does not have access to bucket, or bucket does not exist.")
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
    bucket_name: str,
    scope_name: str,
    collection_name: str,
    document_id: str,
    document_content: dict[str, Any],
) -> bool:
    """Insert or update a document in a bucket, scope and collection by its ID.
    Returns True on success, False on failure."""
    cluster = ctx.request_context.lifespan_context.cluster
    try:
        bucket = cluster.bucket(bucket_name)
    except Exception as e:
        logger.error(f"Error accessing bucket: {e}")
        raise ValueError("Tool does not have access to bucket, or bucket does not exist.")
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
    ctx: Context, bucket_name: str, scope_name: str, collection_name: str, document_id: str
) -> bool:
    """Delete a document in a bucket, scope and collection by its ID.
    Returns True on success, False on failure."""
    cluster = ctx.request_context.lifespan_context.cluster
    try:
        bucket = cluster.bucket(bucket_name)
    except Exception as e:
        logger.error(f"Error accessing bucket: {e}")
        raise ValueError("Tool does not have access to bucket, or bucket does not exist.")
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        collection.remove(document_id)
        logger.info(f"Successfully deleted document {document_id}")
        return True
    except Exception as e:
        logger.error(f"Error deleting document {document_id}: {e}")
        return False

@mcp.tool()
def advise_index_for_sql_plus_plus_query(
    ctx: Context, bucket_name: str, scope_name: str, query: str
) -> dict[str, Any]:
    """Get an index recommendation from the SQL++ index advisor for a specified query on a specified bucket and scope.
    Returns a dictionary with the query advised on, as well as:
    1. an array of the current indexes used and their status (or a string indicating no existing indexes available)
    2. an array of recommended indexes and/or covering indexes with reasoning (or a string indicating no possible index improvements)
    """
    response = {}

    try:
        advise_query = f"ADVISE {query}"
        result = run_sql_plus_plus_query(ctx, bucket_name, scope_name, advise_query)

        if result and (advice := result[0].get("advice")):
            if (advice is not None):
                advise_info = advice.get("adviseinfo")
                if ( advise_info is not None):
                    response["current_indexes"] = advise_info.get("current_indexes", "No current indexes")
                    response["recommended_indexes"] = advise_info.get("recommended_indexes","No index recommendations available")
                    response["query"]=result[0].get("query","Query statement unavailable")
        return response
    except Exception as e:
        logger.error(f"Error running Advise on query: {e}")
        raise ValueError(f"Unable to run ADVISE on: {query} for keyspace {bucket_name}.{scope_name}") from e

@mcp.tool()
def run_sql_plus_plus_query(
    ctx: Context, bucket_name: str, scope_name: str, query: str
) -> list[dict[str, Any]]:
    """Run a SQL++ query on a scope in a specified bucket and return the results as a list of JSON objects."""
    cluster = ctx.request_context.lifespan_context.cluster
    try:
        bucket = cluster.bucket(bucket_name)
    except Exception as e:
        logger.error(f"Error accessing bucket: {e}")
        raise ValueError("Tool does not have access to bucket, or bucket does not exist.")
    read_only_query_mode = ctx.request_context.lifespan_context.read_only_query_mode
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
