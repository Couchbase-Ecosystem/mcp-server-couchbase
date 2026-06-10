"""
Couchbase MCP Utilities

This module contains utility functions for configuration, connection, and context management.
"""

# Configuration utilities
from .config import (
    get_settings,
    parse_tool_names,
)

# Connection utilities
from .connection import (
    connect_to_bucket,
    connect_to_couchbase_cluster,
)

# Constants
from .constants import (
    ALLOWED_OAUTH_ALGORITHMS,
    ALLOWED_TRANSPORTS,
    DEFAULT_HOST,
    DEFAULT_LOG_LEVEL,
    DEFAULT_OAUTH_ALGORITHM,
    DEFAULT_PORT,
    DEFAULT_READ_ONLY_MODE,
    DEFAULT_TRANSPORT,
    MCP_SERVER_NAME,
    NETWORK_TRANSPORTS,
    NETWORK_TRANSPORTS_SDK_MAPPING,
    SCOPE_READ,
    SCOPE_WRITE,
    STREAMABLE_HTTP_TRANSPORT,
)

# Context utilities
from .context import (
    AppContext,
    get_cluster_connection,
    get_cluster_provider,
)

# Elicitation utilities
from .elicitation import wrap_with_confirmation

# Index utilities
from .index_utils import (
    fetch_indexes_from_rest_api,
)

# OAuth scope enforcement
from .scope_enforcement import required_scopes_for_tool, wrap_with_scope_check

# Note: Individual modules create their own hierarchical loggers using:
# logger = logging.getLogger(f"{MCP_SERVER_NAME}.module.name")

__all__ = [
    # Config
    "get_settings",
    "parse_tool_names",
    # Connection
    "connect_to_couchbase_cluster",
    "connect_to_bucket",
    # Context
    "AppContext",
    "get_cluster_connection",
    "get_cluster_provider",
    # Index utilities
    "fetch_indexes_from_rest_api",
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
    "STREAMABLE_HTTP_TRANSPORT",
    "SCOPE_READ",
    "SCOPE_WRITE",
    "ALLOWED_OAUTH_ALGORITHMS",
    "DEFAULT_OAUTH_ALGORITHM",
    # Elicitation
    "wrap_with_confirmation",
    # OAuth scope enforcement
    "required_scopes_for_tool",
    "wrap_with_scope_check",
]
