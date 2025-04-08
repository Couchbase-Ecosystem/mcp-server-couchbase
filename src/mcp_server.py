from datetime import timedelta
from typing import Any
from mcp.server.fastmcp import FastMCP, Context
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
import os
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import AsyncIterator


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


# Tools
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
def list_documents_in_collection(
    ctx: Context, scope_name: str, collection_name: str, limit: int = 100
) -> list[dict[str, Any]]:
    """List documents (ID and content) in a specified collection.
    Returns a list of documents. Be cautious as collections can be large.
    Uses a default limit of 100 documents, which can be adjusted.
    """
    bucket = ctx.request_context.lifespan_context.bucket
    cluster = ctx.request_context.lifespan_context.cluster # Cluster needed for query
    # Ensure cluster is available
    if not cluster:
        raise ValueError("Cluster connection not available in context.")

    try:
        # We need to query the cluster, specifying the scope.collection namespace
        # Using f-string requires backticks for names containing special chars, safer to use fully qualified name
        fully_qualified_name = f"`{bucket.name}`.`{scope_name}`.`{collection_name}`"
        query = f"SELECT META().id as doc_id, * FROM {fully_qualified_name} LIMIT {limit}"

        logger.info(f"Running query to list documents: {query}")
        result = cluster.query(query) # Use cluster.query for N1QL

        results = []
        for row in result.rows():
            # The query returns the collection name as a key, holding the document content.
            # We want to flatten this slightly for a cleaner output.
            doc_content = row.get(collection_name, {})
            doc_content['doc_id'] = row.get('doc_id') # Ensure doc_id is included
            results.append(doc_content)
        return results
    except Exception as e:
        logger.error(f"Error listing documents in {scope_name}.{collection_name}: {e}")
        raise

@mcp.tool()
def get_document(
    ctx: Context, scope_name: str, collection_name: str, document_id: str
) -> dict[str, Any]:
    """Get a document by its ID from the specified scope and collection."""
    bucket = ctx.request_context.lifespan_context.bucket
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        result = collection.get(document_id)
        # The content_as[dict] method provides the document content as a dictionary
        return result.content_as[dict]
    except Exception as e:
        logger.error(f"Error getting document {document_id}: {e}")
        raise


@mcp.tool()
def upsert_document(
    ctx: Context,
    scope_name: str,
    collection_name: str,
    document_id: str,
    document_content: dict[str, Any],
) -> dict[str, str]:
    """Insert or update a document with the given ID and content in the specified scope and collection."""
    bucket = ctx.request_context.lifespan_context.bucket
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        collection.upsert(document_id, document_content)
        logger.info(f"Successfully upserted document {document_id}")
        return {"status": "success", "document_id": document_id}
    except Exception as e:
        logger.error(f"Error upserting document {document_id}: {e}")
        raise


@mcp.tool()
def delete_document(
    ctx: Context, scope_name: str, collection_name: str, document_id: str
) -> dict[str, str]:
    """Delete a document by its ID from the specified scope and collection."""
    bucket = ctx.request_context.lifespan_context.bucket
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        collection.remove(document_id)
        logger.info(f"Successfully deleted document {document_id}")
        return {"status": "success", "document_id": document_id}
    except Exception as e:
        logger.error(f"Error deleting document {document_id}: {e}")
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
