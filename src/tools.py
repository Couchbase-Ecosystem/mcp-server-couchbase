from typing import Any
from mcp.server.fastmcp import  Context
from lark_sqlpp import modifies_data, modifies_structure, parse_sqlpp
from couchbase_mcp import get_mcp, logger


mcp=get_mcp()
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