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
import requests
from requests.auth import HTTPBasicAuth

MCP_SERVER_NAME = "couchbase"

SERVICE_PORT_MAPPING = {
    "tls" : {
        "admin" : "18091",
        "index" : "19012",
        "query" : "18093",
        "fts" : "18094",
        "cbas" : "18095",
        "eventing" : "18097"

    },
    "no_tls" : {
        "admin" : "8091",
        "index" : "9012",
        "query" : "8093",
        "fts" : "8094",
        "cbas" : "8095",
        "eventing" : "8097"

    }
}

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
    '--ca-cert-path',
    envvar="CA_CERT_PATH",
    type=click.Path(exists=True),
    default=None,
    help='Path to Server TLS certificate, required for API calls.')

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
    transport,
):
    """Couchbase MCP Server"""
    ctx.obj = {
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "read_only_query_mode": read_only_query_mode,
        "ca_cert_path" : ca_cert_path
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
    ca_cert_path = settings.get("ca_cert_path")
    use_tls = True
    if "://" in connection_string:
        protocol = connection_string.split("://", 1)[0]
        use_tls = (protocol[-1] == 's')

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
            use_tls=use_tls
        )

    except Exception as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise


# Initialize MCP server
mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan)



# Tools
@mcp.tool()
def get_cluster_health_check(
    ctx: Context) -> list[dict[str,Any]]:
    """Runs a healthcheck (ping report) on the Couchbase cluster. Returns an array of json objects with pintreport results and node IPs.
    Also useful for discovering available services in the cluster. Multiple services may reside on each node.
    Returns: Array of service objects, each having the URL (consisting of IP and port for the service) and state of the service"""
    cluster = ctx.request_context.lifespan_context.cluster

    services = []
    try:
        ping_report = cluster.ping()
    except Exception as e:
        logger.error(f"Unable to reach cluster for health check: {e}")
        raise e
    try:
        services = []
        for service_type, endpoints in ping_report.endpoints.items():
            for ep in endpoints:
                services.append({
                    "service": service_type.value,
                    "state": ep.state.value,
                    "id": ep.id,
                    "ip": ep.remote,
                    "latency_ms": ep.latency.total_seconds() * 1000 if ep.latency else None,
                    "error": str(ep.error) if ep.error else None
                })
    except Exception as e:
        logger.error(f"Unable to parse ping report: {e}")
        raise e

    return services

def call_api(ctx: Context, url: str ) -> requests.Response:
    """Call Couchbase API for given URL. Returns raw Response object."""

    username = ctx.request_context.lifespan_context.username
    password = ctx.request_context.lifespan_context.password
    ca_cert_path = ctx.request_context.lifespan_context.ca_cert_path or True

    try:
        response = requests.get(
            url,
            auth=HTTPBasicAuth(username, password),
            verify=ca_cert_path,  
            timeout=10
        )
        response.raise_for_status()
        return response
    except requests.exceptions.SSLError as e:
        logger.error(f"SSL verification failed: {e}")
        raise e
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching api: {e}")
        raise e
    except Exception as e:
        logger.error(f"Error fetching api: {e}")
        raise e

def build_api_url(ctx: Context, hostname: str, service: str, endpoint: str) -> str:
    """Return full url for an api call to given hostname, service and endpoint"""
    protocol = "http"
    port = SERVICE_PORT_MAPPING["no_tls"].get(service) or "8091"
    if( ctx.request_context.lifespan_context.use_tls):
        protocol = "https"
        port = SERVICE_PORT_MAPPING["tls"].get(service) or "18091"
    return f'{protocol}://{hostname}:{port}/{endpoint}'


def strip_protocol(url: str) -> str:
    parts = url.split("://", 1)
    return parts[1] if len(parts) == 2 else url

 
@mcp.tool()
def get_cluster_prometheus_metrics_endpoints(
    ctx: Context) -> list[str]:
    """Runs an API call to the Ccouchbase Prometheus Endpoint Service Discovery. Retrieves the hostnames and ports of all metrics endpoints for the cluster nodes.
    Required for calling the get_cluster_metrics function in a cluster.
    Accepts the hostname of the node to get metrics from.
    Returns: Array of hostname+port as strings"""

    hostname = strip_protocol(ctx.request_context.lifespan_context.connection_string)
    url =  build_api_url(ctx, hostname, "admin", "prometheus_sd_config" )
    try:
        response = call_api(ctx, url)
    except Exception as e:
        logger.error(f"Error calling API to fetch endpoints: {e}")
        raise e
    json_data = response.json()
    if (json_data and isinstance(json_data,list)):
        return json_data[0].get("targets")
    else:
        logger.error(f"Unexpected response from fetch endpoints API {response.text}")
        raise TypeError(f"Unexpected response from fetch endpoints API {response.text}")


@mcp.tool()
def get_cluster_node_metrics(
    ctx: Context, hostname: str) -> str:
    """Runs an API call to the metrics endpoint of a given couchbase node hostname. 
    Metrics contain info on nodes performance, resources and services.
    Accepts the node hostname as a string. 
    Returns: String representing the prometheus formatted metrics returned by couchbase."""

    hostname = strip_protocol(hostname)
    parts = hostname.split(":",1)
    hostname = parts[0]
    url = build_api_url(ctx, hostname, "admin", "metrics" )
    try:
        response = call_api(ctx, url)
        return response.text
    except Exception as e:
        logger.error(f"Error calling API to fetch endpoints: {e}")
        raise e

@mcp.tool()
def get_list_of_buckets_with_settings(
    ctx: Context
) -> list[Any]:
    """Get the list of buckets from the Couchbase cluster, including their bucket settings.
    Returns a list of bucket setting objects.
    """
    cluster = ctx.request_context.lifespan_context.cluster

    try:
        bucket_manager = cluster.buckets()
        buckets = bucket_manager.get_all_buckets()
        return buckets
    except Exception as e:
        logger.error(f"Error getting bucket names: {e}")
        raise e


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
