FROM python:3.10-slim

WORKDIR /app

# Install build dependencies and uv
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH
ENV PATH="/root/.local/bin:${PATH}"

# Copy application files
COPY src/ /app/src/
COPY pyproject.toml README.md ./

# Install dependencies
RUN uv pip install --system -e .

# Environment variables with defaults
ENV READ_ONLY_QUERY_MODE="true"
ENV MCP_TRANSPORT="stdio"
ENV FASTMCP_PORT="8080"

# Expose default port for SSE mode (runtime port can be mapped using -p flag)
EXPOSE 8080

# Run the server using uv
ENTRYPOINT ["uv", "run", "src/mcp_server.py"]
CMD ["--transport", "stdio"] 