---
sidebar_position: 1
title: Development Setup
---

# Development Setup

Guide for setting up a development environment to contribute to the Couchbase MCP Server.

## Prerequisites

- **Python 3.10+**
- **[uv](https://docs.astral.sh/uv/)** — Fast Python package installer and dependency manager
- **Git**

## Clone and Install

```bash
# Clone the repository
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
cd mcp-server-couchbase

# Install dependencies (including development tools)
uv sync --extra dev
```

## Install Pre-Commit Hooks

```bash
# Install pre-commit hooks (runs linting on every commit)
uv run pre-commit install

# Verify installation
uv run pre-commit run --all-files
```

## Project Structure

```
mcp-server-couchbase/
├── src/
│   ├── mcp_server.py              # MCP server entry point
│   ├── certs/                     # SSL/TLS certificates
│   │   └── capella_root_ca.pem    # Capella root CA certificate
│   ├── tools/                     # MCP tool implementations
│   │   ├── __init__.py            # Tool exports and categorization
│   │   ├── server.py              # Server status and connection tools
│   │   ├── kv.py                  # Key-value operations (CRUD)
│   │   ├── query.py               # SQL++ query operations
│   │   └── index.py               # Index operations and recommendations
│   └── utils/                     # Utility modules
│       ├── constants.py           # Project constants
│       ├── config.py              # Configuration management
│       ├── connection.py          # Couchbase connection handling
│       ├── context.py             # Application context management
│       └── index_utils.py         # Index-related helper functions
├── tests/                         # Test suite
├── scripts/                       # Development scripts
├── pyproject.toml                 # Project config and dependencies
└── .pre-commit-config.yaml        # Pre-commit hook configuration
```

## Common Development Tasks

```bash
# Install new dependencies
uv add package-name

# Install new dev dependencies
uv add --dev package-name

# Update dependencies
uv sync

# Run the server for testing
uv run src/mcp_server.py \
  --connection-string "couchbase://localhost" \
  --username "Administrator" \
  --password "password"
```

## Debugging

- **Use logging**: The project uses hierarchical logging with the pattern `logger = logging.getLogger(f"{MCP_SERVER_NAME}.module.name")`
- **Check connection**: Ensure your Couchbase cluster is accessible.
- **Validate configuration**: Make sure all required environment variables are set.
