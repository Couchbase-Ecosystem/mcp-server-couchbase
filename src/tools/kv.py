"""
Tools for key-value operations.

This module contains tools for getting a document by its ID, upserting a document by its ID, and deleting a document by its ID.
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.constants import MCP_SERVER_NAME
from utils.context import ensure_bucket_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.kv")


def get_document_by_id(
    ctx: Context, scope_name: str, collection_name: str, document_id: str
) -> dict[str, Any]:
    """Get a document by its ID from the specified scope and collection.
    If the document is not found, it will raise an exception."""
    bucket = ensure_bucket_connection(ctx)
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        result = collection.get(document_id)
        return result.content_as[dict]
    except Exception as e:
        logger.error(f"Error getting document {document_id}: {e}")
        raise


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
