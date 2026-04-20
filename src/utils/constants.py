# MCP Server Constants
MCP_SERVER_NAME = "couchbase"

# Default Configuration Values
DEFAULT_READ_ONLY_MODE = True
# When True, the run_sql_plus_plus_query tool's query parameter annotation is
# dynamically updated to guide MCP clients to use generate_or_modify_sql_plus_plus_query
# for natural-language-to-SQL++ conversion (see update_query_function_annotation).
# TODO: Once this default is changed to True, remove the dynamic annotation logic in
# update_query_function_annotation and apply the Annotated[str, Field(...)] directly
# on the run_sql_plus_plus_query function signature instead.
DEFAULT_ENABLE_QUERY_GENERATION = False
DEFAULT_TRANSPORT = "stdio"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_WORKER_BUCKET_CONCURRENCY = 3
DEFAULT_ENRICHMENT_BUCKET_CONCURRENCY = 2
DEFAULT_VERIFIER_SAMPLE_SIZE = 500

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
