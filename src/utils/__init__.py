"""
Couchbase MCP Utilities

This module contains utility functions for configuration, connection, and context management.
"""

# Configuration utilities
from .config import (
    get_settings,
    validate_required_param,
    validate_connection_config,
)

# Connection utilities  
from .connection import (
    connect_to_couchbase_cluster,
    connect_to_bucket,
)

# Context utilities
from .context import (
    AppContext,
    set_cluster_in_lifespan_context,
    set_bucket_in_lifespan_context,
    ensure_bucket_connection,
)

# Constants
from .constants import (
    MCP_SERVER_NAME,
    DEFAULT_READ_ONLY_MODE,
    DEFAULT_TRANSPORT,
    DEFAULT_LOG_LEVEL,
)

# Note: Individual modules create their own hierarchical loggers using:
# logger = logging.getLogger(f"{MCP_SERVER_NAME}.module.name")

__all__ = [
    # Config
    "get_settings",
    "validate_required_param",
    "validate_connection_config",
    # Connection
    "connect_to_couchbase_cluster", 
    "connect_to_bucket",
    # Context
    "AppContext",
    "set_cluster_in_lifespan_context",
    "set_bucket_in_lifespan_context", 
    "ensure_bucket_connection",
    # Constants
    "MCP_SERVER_NAME",
    "DEFAULT_READ_ONLY_MODE",
    "DEFAULT_TRANSPORT",
    "DEFAULT_LOG_LEVEL",
] 