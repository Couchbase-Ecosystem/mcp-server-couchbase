# MCP Server Constants
MCP_SERVER_NAME = "couchbase"

# Default Configuration Values
DEFAULT_READ_ONLY_MODE = True
DEFAULT_TRANSPORT = "stdio"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000

# Allowed Transport Types
ALLOWED_TRANSPORTS = ["stdio", "http", "sse"]
NETWORK_TRANSPORTS = ["http", "sse"]
NETWORK_TRANSPORTS_SDK_MAPPING = {
    "http": "streamable-http",
    "sse": "sse",
}

# Index Service Configuration
# Cluster major version at which list_indexes prefers the query service over
# the Index Service REST API. From this version, system:indexes exposes the
# original CREATE INDEX statement in metadata.definition, so we query it
# instead of the /getIndexStatus REST endpoint.
QUERY_SERVICE_LIST_INDEXES_MIN_MAJOR_VERSION = 8

# Logging Configuration
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_MAX_BYTES = 1 * 1024 * 1024  # 1 MiB
DEFAULT_LOG_BACKUP_COUNT = 5
ALLOWED_LOG_LEVELS = ("OFF", "DEBUG", "INFO", "WARNING", "ERROR")
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# ISO 8601 local time with UTC offset (e.g. 2026-06-09T18:08:49+0530).
# Milliseconds are intentionally omitted; we can switch to a sub-second
# format later via a custom Formatter if support diagnostics need it.
DEFAULT_LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"
ALLOWED_LOG_SINKS = ("stderr", "file")
DEFAULT_LOG_SINKS = "stderr"
# CWD-relative filenames used when file logging is active and the caller
# omits --log-file / --error-log-file. Referenced from both the CLI help
# text and the fallback inside configure_logging so the two stay in sync.
DEFAULT_LOG_FILE = "mcp_server.log"
DEFAULT_ERROR_LOG_FILE = "mcp_server.error.log"
