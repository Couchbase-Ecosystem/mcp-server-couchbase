# Build stage - use official uv image with Python 3.10
FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim AS builder

# Set uv configuration
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /build

# Copy dependency files for caching
COPY pyproject.toml ./
COPY src/ ./src/

# Create virtual environment and install dependencies
RUN uv venv /opt/venv && \
    uv pip install --python /opt/venv/bin/python -e .

# Runtime stage - use Python image with same version as builder
FROM python:3.10-slim-bookworm AS runtime

# Create non-root user
RUN useradd --system --uid 1001 mcpuser

WORKDIR /app

# Copy virtual environment and application from builder
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /build/src ./src

# Set up Python environment
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Change ownership to non-root user
RUN chown -R mcpuser:mcpuser /app /opt/venv

# Switch to non-root user
USER mcpuser

# Environment variables with defaults
ENV READ_ONLY_QUERY_MODE="true" \
    MCP_TRANSPORT="stdio" \
    FASTMCP_PORT="8080"

# Expose default port for SSE mode
EXPOSE 8080

# Use python directly instead of uv run to avoid runtime dependency resolution
ENTRYPOINT ["python", "src/mcp_server.py"]
CMD ["--transport", "stdio"]
