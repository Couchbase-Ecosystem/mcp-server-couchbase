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

#Slow Query tools

@mcp.tool()
def advanced_aggregate_slow_queries_stats_by_pattern(ctx: Context, query_limit:int = 10) -> list[dict[str, Any]]:
    """Run an in depth analysis query on the completed_requests system catalog to discover potential slow running queries along with index scan and fetch counts and times. 
    This query attempts to reduce the statements of logged queries into patterns by removing any values and only looking at the query structure with regard to returned fields,
    filtered predicates, sorts and aggregations. 
    For each query pattern it returns execution durations, counts and example queries.
    
    Accepts an optional integer as a limit to the number of results returned (default 10).
    Returns an object array of query patterns with:
    1. total count and count per user running the query.
    2. min, max and average duration of the full execution.
    3. min, max and average duration of each of the sub-operations index scan (or primary scan) and Fetch.
    4. Count of number of documents found in index scans and the total fetched.
    5. An example instance of one of the queries run matching this pattern.
    """

    query_template = """
    SELECT grouped.query_pattern,
       grouped.statement_example,
       (OBJECT u: ARRAY_LENGTH(ARRAY v FOR v IN grouped.users_agg WHEN v = u END) FOR u IN ARRAY_DISTINCT(grouped.users_agg) END) AS user_query_counts,
       grouped.total_count,
       ROUND(grouped.min_duration_in_seconds, 3) AS min_duration_in_seconds,
       ROUND(grouped.max_duration_in_seconds, 3) AS max_duration_in_seconds,
       ROUND(grouped.avg_duration_in_seconds, 3) AS avg_duration_in_seconds,
       ROUND((grouped.sorted_durations[FLOOR(grouped.total_count / 2)] + 
              grouped.sorted_durations[CEIL(grouped.total_count / 2) - 1]) / 2, 3) AS median_duration_in_seconds,
       ROUND(grouped.avg_fetch_count, 3) AS avg_fetch_docs_count,
       ROUND(grouped.avg_primaryScan_count, 3) AS avg_primaryScan_docs_count,
       ROUND(grouped.avg_indexScan_count, 3) AS avg_indexScan_docs_count,
       ROUND(grouped.avg_fetch_time, 3) AS avg_fetch_duration_in_seconds,
       ROUND(grouped.avg_primaryScan_time, 3) AS avg_primaryScan_duration_in_seconds,
       ROUND(grouped.avg_indexScan_time, 3) AS avg_indexScan_duration_in_seconds
FROM (
    SELECT query_pattern,
           ARRAY_AGG(sub.users) AS users_agg,
           ARRAY_AGG(sub.statement)[0] as statement_example, 
           COUNT(*) AS total_count,
           MIN(duration_in_seconds) AS min_duration_in_seconds,
           MAX(duration_in_seconds) AS max_duration_in_seconds,
           AVG(duration_in_seconds) AS avg_duration_in_seconds,
           ARRAY_SORT(ARRAY_AGG(duration_in_seconds)) AS sorted_durations,
           AVG(sub.`fetch_count`) AS avg_fetch_count,
           AVG(sub.`primaryScan_count`) AS avg_primaryScan_count,
           AVG(sub.`indexScan_count`) AS avg_indexScan_count,
           AVG(sub.`fetch_time`) AS avg_fetch_duration_in_seconds,
           AVG(sub.`primaryScan_time`) AS avg_primaryScan_duration_in_seconds,
           AVG(sub.`indexScan_time`) AS avg_indexScan_duration_in_seconds           
    FROM (
        SELECT query_pattern,
               statement,
               users,
               STR_TO_DURATION(serviceTime) / 1000000000 AS duration_in_seconds,
               phaseCounts.`fetch` AS `fetch_count`,
               phaseCounts.`primaryScan` AS `primaryScan_count`,
               phaseCounts.`indexScan` AS `indexScan_count`,
               STR_TO_DURATION(phaseTimes.`fetch`)/ 1000000000 AS `fetch_duration_in_seconds`,
               STR_TO_DURATION(phaseTimes.`primaryScan`)/ 1000000000 AS `primaryScan_duration_in_seconds`,
               STR_TO_DURATION(phaseTimes.`indexScan`)/ 1000000000 AS `indexScan_duration_in_seconds`
        FROM system:completed_requests
        LET query_pattern = IFMISSING(preparedText, REGEX_REPLACE(
  REGEX_REPLACE(
  REGEX_REPLACE(
  REGEX_REPLACE(
  REGEX_REPLACE(
  REGEX_REPLACE(statement,
    "\\\\s+", " "),
    '"(?:[^"]|"")*"', "?"),
    "'(?:[^']|'')*'", "?"),
    "\\\\b-?\\\\d+\\\\.?\\\\d*\\\\b", "?"),
    "(?i)\\\\b(NULL|TRUE|FALSE)\\\\b", "?"),
    "(\\\\?\\\\s*,\\\\s*)+\\\\?", "?"))
        WHERE  UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'INFER %'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'ADVISE %'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'CREATE %'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'CREATE INDEX%'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'ALTER INDEX%'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE '% SYSTEM:%'
    ) AS sub
    GROUP BY query_pattern
) AS grouped
ORDER BY grouped.total_count DESC
    LIMIT {limit}
    """

    if (not isinstance(query_limit, int)):
        raise TypeError(f"Param query_limit must be an integer value, received {type(query_limit)} - {query_limit}")
    query = query_template.format(limit=query_limit)
    try:
        result = system_catalog_query(ctx,query)
        return result
    except Exception as e:
        logger.error(f"Error completed_request query: {str(e)}", exc_info=True)
        raise e
    
@mcp.tool()
def retreive_single_slow_query_plan(ctx: Context, query_statement : str, query_limit:int = 10) -> list[dict[str, Any]]:
    """Retrieve the query execution report and execution plan of all executions of the given query saved in the completed_requests catalog.
    The query statement must be an exact match to the statement executed, including values or placeholders where applicable.
    Accepts a query statement and an optional integer as a limit to the number of results returned (default 10).
    Returns an object array of execution plans for all instances of the query, ordered by execution time from highest to lowest.
    """

    query_template = """
    SELECT r.*, meta(r).plan
    FROM system:completed_requests AS r
    WHERE UPPER(IFMISSING(preparedText, statement)) = UPPER('{query_statement}')
    ORDER BY STR_TO_DURATION(r.elapsedTime) DESC
    LIMIT {limit}
    """

    if (not isinstance(query_limit, int)):
        raise TypeError(f"Param query_limit must be an integer value, received {type(query_limit)} - {query_limit}")
    query = query_template.format(limit=query_limit,query_statement=query_statement)
    try:
        result = system_catalog_query(ctx,query)
        return result
    except Exception as e:
        logger.error(f"Error completed_request query: {str(e)}", exc_info=True)
        raise e
    
@mcp.tool()
def retreive_list_of_similar_queries_from_completed_requests_catalog(ctx: Context, query_statement : str, query_limit:int = 10) -> list[dict[str, Any]]:
    """Retrieve a list of all recorded query statements matching the pattern of the specified query (i.e. similar queries differing in predicate values and capitalizations).
    The query statement must be an exact match to the statement executed, including values or placeholders where applicable.
    Accepts a query statement and an optional integer as a limit to the number of results returned (default 10).
    Returns a list of query statements matching the pattern.
    """

    query_template = """
    SELECT raw r.statement
    FROM system:completed_requests AS r
    LET query_pattern = UPPER(IFMISSING(preparedText, REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(statement,
        "\\\\s+", " "),
        '"(?:[^"]|"")*"', "?"),
        "'(?:[^']|'')*'", "?"),
        "\\\\b-?\\\\d+\\\\.?\\\\d*\\\\b", "?"),
        "(?i)\\\\b(NULL|TRUE|FALSE)\\\\b", "?"),
        "(\\\\?\\\\s*,\\\\s*)+\\\\?", "?")))
    WHERE query_pattern = REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE({query_statement},
        "\\\\s+", " "),
        '"(?:[^"]|"")*"', "?"),
        "'(?:[^']|'')*'", "?"),
        "\\\\b-?\\\\d+\\\\.?\\\\d*\\\\b", "?"),
        "(?i)\\\\b(NULL|TRUE|FALSE)\\\\b", "?"),
        "(\\\\?\\\\s*,\\\\s*)+\\\\?", "?")
    LIMIT {limit}
    """

    if (not isinstance(query_limit, int)):
        raise TypeError(f"Param query_limit must be an integer value, received {type(query_limit)} - {query_limit}")
    query = query_template.format(limit=query_limit,query_statement=query_statement)
    try:
        result = system_catalog_query(ctx,query)
        return result
    except Exception as e:
        logger.error(f"Error completed_request query: {str(e)}", exc_info=True)
        raise e


# Util Functions
def system_catalog_query(ctx: Context, query: str) -> list[dict[str, Any]]:
    cluster = ctx.request_context.lifespan_context.cluster
    try:

        results = []
        result = cluster.query(query)
        for row in result:
            results.append(row)
        return results
    except Exception as e:
        logger.error(f"Error running query: {str(e)}", exc_info=True)
        raise e



if __name__ == "__main__":
    main()
