# Build stage - use official uv image with Python 3.10
FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim AS builder

# Set uv configuration
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /build

# Copy dependency files for caching
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Create virtual environment and install dependencies
RUN uv venv /opt/venv && \
    uv pip install --python /opt/venv/bin/python .

# Runtime stage - use Python image with same version as builder
FROM python:3.10-slim-bookworm AS runtime

# Accept build arguments for labels
ARG GIT_COMMIT_HASH="unknown"
ARG BUILD_DATE="unknown"

# Add metadata labels
LABEL org.opencontainers.image.revision="${GIT_COMMIT_HASH}" \
    org.opencontainers.image.created="${BUILD_DATE}" \
    org.opencontainers.image.title="MCP Server Couchbase" \
    org.opencontainers.image.description="Model Context Protocol server for Couchbase" \
    org.opencontainers.image.source="https://github.com/Couchbase-Ecosystem/mcp-server-couchbase"\
    io.modelcontextprotocol.server.name="io.github.Couchbase-Ecosystem/mcp-server-couchbase"

# Create non-root user
RUN useradd --system --uid 1001 mcpuser

WORKDIR /app

# Copy virtual environment and application from builder
COPY --from=builder /opt/venv /opt/venv

# Set up Python environment
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Change ownership to non-root user
RUN chown -R mcpuser:mcpuser /app /opt/venv

# Switch to non-root user
USER 1001

# Environment variables with stdio defaults (override for network mode)
ENV CB_MCP_READ_ONLY_QUERY_MODE="true" \
    CB_MCP_TRANSPORT="stdio" \
    CB_MCP_PORT="8000"

# Expose default port for HTTP/SSE mode
EXPOSE 8000

# Use the installed console script
ENTRYPOINT ["couchbase-mcp-server"]
