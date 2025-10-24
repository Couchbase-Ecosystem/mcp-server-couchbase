"""
Utility functions for index operations.

This module contains helper functions for working with Couchbase indexes.
"""

import logging
from typing import Any

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.index_utils")


def _is_vector_index(index_keys: list[Any]) -> bool:
    """Check if the index is a vector index by looking for VECTOR keyword."""
    return any("VECTOR" in str(key).upper() for key in index_keys)


def _get_index_type_clause(is_primary: bool, is_vector: bool) -> str:
    """Get the appropriate CREATE INDEX clause based on index type."""
    if is_primary:
        return "CREATE PRIMARY INDEX"
    if is_vector:
        return "CREATE VECTOR INDEX"
    return "CREATE INDEX"


def _build_keyspace_path(bucket: str, scope: str | None, collection: str | None) -> str:
    """Build the keyspace path (bucket.scope.collection)."""
    path = f"`{bucket}`"
    if scope and collection:
        path += f".`{scope}`.`{collection}`"
    return path


def _build_with_clause(with_clause: dict[str, Any]) -> str:
    """Build the WITH clause for vector indexes."""
    with_parts = []

    if "dimension" in with_clause:
        with_parts.append(f'"dimension":{with_clause["dimension"]}')

    if "similarity" in with_clause:
        with_parts.append(f'"similarity":"{with_clause["similarity"]}"')

    if "description" in with_clause:
        with_parts.append(f'"description":"{with_clause["description"]}"')

    if with_parts:
        return " WITH { " + ", ".join(with_parts) + " }"
    return ""


def generate_index_definition(index_data: dict[str, Any]) -> str | None:
    """Generate CREATE INDEX statement for GSI indexes, including vector indexes.

    Args:
        index_data: Dictionary containing index information with keys:
            - name: Index name
            - bucket: Bucket name
            - scope: Scope name (optional)
            - collection: Collection name (optional)
            - is_primary: Boolean indicating if it's a primary index
            - index_type: Index type (must be "gsi" for definition generation)
            - index_key: List of index keys
            - condition: WHERE condition (optional)
            - partition: PARTITION BY clause (optional)
            - with_clause: Dictionary containing WITH clause properties (for vector indexes)
            - include_fields: List of fields to include (for vector indexes)

    Returns:
        CREATE INDEX statement string for GSI indexes, None for other types
    """
    # Only generate definition for GSI indexes
    if index_data.get("index_type") != "gsi":
        return None

    try:
        index_keys = index_data.get("index_key", [])
        is_vector = _is_vector_index(index_keys)

        # Start building the definition
        query_definition = _get_index_type_clause(
            index_data.get("is_primary", False), is_vector
        )

        # Add index name and keyspace path
        query_definition += f" `{index_data['name']}`"
        query_definition += f" ON {_build_keyspace_path(index_data['bucket'], index_data.get('scope'), index_data.get('collection'))}"

        # Add index keys for non-primary indexes
        if index_keys:
            keys_str = ", ".join(str(key) for key in index_keys)
            query_definition += f"({keys_str})"

        # Add INCLUDE clause if present (typically for vector indexes)
        include_fields = index_data.get("include_fields", [])
        if include_fields:
            include_str = ", ".join(f"`{field}`" for field in include_fields)
            query_definition += f" INCLUDE({include_str})"

        # Add WHERE condition if exists
        if condition := index_data.get("condition"):
            query_definition += f" WHERE {condition}"

        # Add PARTITION BY if exists
        if partition := index_data.get("partition"):
            query_definition += f" PARTITION BY {partition}"

        # Add WITH clause for vector indexes
        if is_vector and (with_clause := index_data.get("with_clause", {})):
            query_definition += _build_with_clause(with_clause)

        return query_definition
    except Exception as e:
        logger.warning(f"Error generating index definition: {e}")
        return None
