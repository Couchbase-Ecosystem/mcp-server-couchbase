"""
Tools for key-value operations.

This module contains tools for document operations by ID:
- get: Retrieve a document
- upsert: Insert or update a document (creates if not exists, updates if exists)
- insert: Create a document only if it does NOT exist (fails if exists)
- replace: Update a document only if it exists (fails if missing)
- delete: Remove a document
"""

import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.connection import connect_to_bucket
from utils.constants import MCP_SERVER_NAME
from utils.context import get_cluster_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.kv")


def get_document_by_id(
    ctx: Context,
    bucket_name: str,
    scope_name: str,
    collection_name: str,
    document_id: str,
) -> dict[str, Any]:
    """Get a document by its ID from the specified scope and collection.
    If the document is not found, it will raise an exception."""

    cluster = get_cluster_connection(ctx)
    bucket = connect_to_bucket(cluster, bucket_name)
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        result = collection.get(document_id)
        return result.content_as[dict]
    except Exception as e:
        logger.error(f"Error getting document {document_id}: {e}")
        raise


def upsert_document_by_id(
    ctx: Context,
    bucket_name: str,
    scope_name: str,
    collection_name: str,
    document_id: str,
    document_content: dict[str, Any],
) -> bool:
    """Insert or update a document by its ID.
    Returns True on success, False on failure."""
    cluster = get_cluster_connection(ctx)
    bucket = connect_to_bucket(cluster, bucket_name)
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        collection.upsert(document_id, document_content)
        logger.info(f"Successfully upserted document {document_id}")
        return True
    except Exception as e:
        logger.error(f"Error upserting document {document_id}: {e}")
        return False


def delete_document_by_id(
    ctx: Context,
    bucket_name: str,
    scope_name: str,
    collection_name: str,
    document_id: str,
) -> bool:
    """Delete a document by its ID.
    Returns True on success, False on failure."""
    cluster = get_cluster_connection(ctx)
    bucket = connect_to_bucket(cluster, bucket_name)
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        collection.remove(document_id)
        logger.info(f"Successfully deleted document {document_id}")
        return True
    except Exception as e:
        logger.error(f"Error deleting document {document_id}: {e}")
        return False


def insert_document_by_id(
    ctx: Context,
    bucket_name: str,
    scope_name: str,
    collection_name: str,
    document_id: str,
    document_content: dict[str, Any],
) -> bool:
    """Insert a new document by its ID.
    Only creates the document if it does NOT already exist.
    Returns True on success, False on failure (including if document already exists)."""
    cluster = get_cluster_connection(ctx)
    bucket = connect_to_bucket(cluster, bucket_name)
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        collection.insert(document_id, document_content)
        logger.info(f"Successfully inserted document {document_id}")
        return True
    except Exception as e:
        logger.error(f"Error inserting document {document_id}: {e}")
        return False


def replace_document_by_id(
    ctx: Context,
    bucket_name: str,
    scope_name: str,
    collection_name: str,
    document_id: str,
    document_content: dict[str, Any],
) -> bool:
    """Replace an existing document by its ID.
    Only replaces the document if it already exists.
    Returns True on success, False on failure (including if document does not exist)."""
    cluster = get_cluster_connection(ctx)
    bucket = connect_to_bucket(cluster, bucket_name)
    try:
        collection = bucket.scope(scope_name).collection(collection_name)
        collection.replace(document_id, document_content)
        logger.info(f"Successfully replaced document {document_id}")
        return True
    except Exception as e:
        logger.error(f"Error replacing document {document_id}: {e}")
        return False
