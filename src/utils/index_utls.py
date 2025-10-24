"""
Utility functions for index operations.

This module contains helper functions for working with Couchbase indexes.
"""

import logging
from typing import Any

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.index_utils")


def generate_index_definition(index_data: dict[str, Any]) -> str | None:
    """Generate CREATE INDEX statement for GSI indexes.

    Args:
        index_data: Dictionary containing index information with keys:
            - name: Index name
            - bucket: Bucket name
            - scope: Scope name (optional)
            - collection: Collection name (optional)
            - is_primary: Boolean indicating if it's a primary index
            - using: Index type (must be "gsi" for definition generation)
            - index_key: List of index keys
            - condition: WHERE condition (optional)
            - partition: PARTITION BY clause (optional)

    Returns:
        CREATE INDEX statement string for GSI indexes, None for other types
    """
    # Only generate definition for GSI indexes
    if index_data.get("using") != "gsi":
        return None

    try:
        # Start building the definition
        if index_data.get("is_primary"):
            query_definition = "CREATE PRIMARY INDEX"
        else:
            query_definition = "CREATE INDEX"

        # Add index name
        query_definition += f" `{index_data['name']}`"

        # Add bucket name
        query_definition += f" ON `{index_data['bucket']}`"

        # Add scope and collection if they exist
        scope = index_data.get("scope")
        collection = index_data.get("collection")
        if scope and collection:
            query_definition += f".`{scope}`.`{collection}`"

        # Add index keys for non-primary indexes
        index_keys = index_data.get("index_key", [])
        if index_keys and len(index_keys) > 0:
            keys_str = ", ".join(str(key) for key in index_keys)
            query_definition += f"({keys_str})"

        # Add WHERE condition if exists
        condition = index_data.get("condition")
        if condition:
            query_definition += f" WHERE {condition}"

        # Add PARTITION BY if exists
        partition = index_data.get("partition")
        if partition:
            query_definition += f" PARTITION BY {partition}"

        return query_definition
    except Exception as e:
        logger.warning(f"Error generating index definition: {e}")
        return None
