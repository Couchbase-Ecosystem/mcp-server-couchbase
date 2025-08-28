"""
Couchbase MCP Utilities

This module contains utility functions for configuration, connection, and context management.
"""

# Configuration utilities
from .config import (
    get_settings,
    validate_connection_config,
    validate_required_param,
)

# Connection utilities
from .connection import (
    connect_to_bucket,
    connect_to_couchbase_cluster,
)

# Constants
from .constants import (
    ALLOWED_TRANSPORTS,
    DEFAULT_HOST,
    DEFAULT_LOG_LEVEL,
    DEFAULT_PORT,
    DEFAULT_READ_ONLY_MODE,
    DEFAULT_TRANSPORT,
    MCP_SERVER_NAME,
    NETWORK_TRANSPORTS,
    NETWORK_TRANSPORTS_SDK_MAPPING,
)

# Context utilities
from .context import (
    AppContext,
    ensure_bucket_connection,
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
    "ensure_bucket_connection",
    # Constants
    "MCP_SERVER_NAME",
    "DEFAULT_READ_ONLY_MODE",
    "DEFAULT_TRANSPORT",
    "DEFAULT_LOG_LEVEL",
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "ALLOWED_TRANSPORTS",
    "NETWORK_TRANSPORTS",
    "NETWORK_TRANSPORTS_SDK_MAPPING",
]
