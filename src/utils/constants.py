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

# Logging Configuration
# Change this to DEBUG, WARNING, ERROR as needed
DEFAULT_LOG_LEVEL = "INFO"
