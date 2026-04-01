---
sidebar_position: 2
title: Contributing
---

# Contributing

Guide for contributing to the Couchbase MCP Server.

## Development Setup

### Prerequisites

- **Python 3.10+**
- **[uv](https://docs.astral.sh/uv/)** — Fast Python package installer and dependency manager
- **Git**

### Clone and Install

```bash
# Clone the repository
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
cd mcp-server-couchbase

# Install dependencies (including development tools)
uv sync --extra dev
```

### Install Pre-Commit Hooks

```bash
# Install pre-commit hooks (runs linting on every commit)
uv run pre-commit install

# Verify installation
uv run pre-commit run --all-files
```

### Project Structure

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

### Common Development Tasks

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

### Debugging

- **Use logging**: The project uses hierarchical logging with the pattern `logger = logging.getLogger(f"{MCP_SERVER_NAME}.module.name")`
- **Check connection**: Ensure your Couchbase cluster is accessible.
- **Validate configuration**: Make sure all required environment variables are set.

---

## Code Quality

The project uses [Ruff](https://docs.astral.sh/ruff/) for fast linting and code formatting.

### Manual Linting

```bash
# Check code quality (no changes made)
./scripts/lint.sh
# Runs: ruff check src/ --diff && ruff format src/ --diff --check

# Auto-fix issues
./scripts/lint_fix.sh
# Runs: ruff check src/ --fix && ruff format src/
```

### Automatic Linting

- **Pre-commit hooks**: Ruff runs automatically on every `git commit`.
- **VS Code**: Auto-format on save using the [Ruff extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff).

### Code Style Guidelines

- **Line length**: 88 characters (enforced by Ruff)
- **Import organization**: Use isort-style grouping (standard library, third-party, local)
- **Type hints**: Use modern Python type hints where helpful
- **Docstrings**: Add docstrings for public functions and classes
- **Error handling**: Include appropriate exception handling with logging

---

## Adding New Tools

### Steps

1. **Create the tool function** in the appropriate module under `src/tools/`:
   - `server.py` — Server and cluster management tools
   - `kv.py` — Key-value document operations
   - `query.py` — SQL++ query and performance analysis tools
   - `index.py` — Index management tools

2. **Export the tool** in `src/tools/__init__.py`:
   - Import the function
   - Add it to `READ_ONLY_TOOLS` (if it only reads data) or `KV_WRITE_TOOLS` (if it modifies data)
   - Add it to `__all__`

3. **Test the tool** with an MCP client.

### Example

Here's the pattern used by existing tools:

```python
import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.constants import MCP_SERVER_NAME
from utils.context import get_cluster_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.your_module")


def your_new_tool(
    ctx: Context, bucket_name: str, some_param: str
) -> dict[str, Any]:
    """Description of what this tool does.
    This docstring is exposed to the LLM as the tool description.
    """
    cluster = get_cluster_connection(ctx)
    # ... your implementation
    return {"result": "data"}
```

### Key Patterns

- **Context**: Always accept `ctx: Context` as the first parameter. Use `get_cluster_connection(ctx)` to get the Couchbase cluster object.
- **Lazy connection**: The cluster connection is established on the first tool call, not at server startup.
- **Logging**: Use the hierarchical logger pattern: `logging.getLogger(f"{MCP_SERVER_NAME}.tools.module_name")`
- **Error handling**: Either raise exceptions (they're returned to the LLM) or return error dictionaries — follow the pattern of similar tools.
- **Read-only awareness**: If your tool modifies data, add it to `KV_WRITE_TOOLS` so it's excluded in read-only mode.

### Checklist

- [ ] Function created in appropriate `src/tools/*.py` module
- [ ] Exported in `src/tools/__init__.py`
- [ ] Added to the correct tool list (`READ_ONLY_TOOLS` or `KV_WRITE_TOOLS`)
- [ ] Added to `__all__`
- [ ] Descriptive docstring (this is what the LLM sees)
- [ ] Tested with an MCP client

---

## Testing

The project uses [pytest](https://docs.pytest.org/) with [pytest-asyncio](https://pytest-asyncio.readthedocs.io/) for testing.

### Prerequisites

Export your Couchbase cluster credentials:

```bash
export CB_CONNECTION_STRING="couchbase://localhost"
export CB_USERNAME="Administrator"
export CB_PASSWORD="password"
export CB_MCP_TEST_BUCKET="travel-sample"  # Optional
```

### Run All Tests

```bash
uv run pytest tests/ -v
```

### Test Structure

| Test File | Description |
|-----------|-------------|
| `test_mcp_integration.py` | MCP protocol integration tests |
| `test_server_tools.py` | Server status and connection tools |
| `test_kv_tools.py` | KV CRUD operations |
| `test_query_tools.py` | SQL++ query execution |
| `test_index_tools.py` | Index listing and advisor |
| `test_performance_tools.py` | Query performance analysis |
| `test_read_only_mode.py` | Read-only mode enforcement |
| `test_disabled_tools.py` | Tool disabling mechanism |
| `test_utils.py` | Utility function tests |

### Test Configuration

- `conftest.py` provides shared fixtures including `create_mcp_session()` for full MCP client-server communication over stdio.
- Tests that require `CB_MCP_TEST_BUCKET` are skipped if the variable is not set.
- Default test timeout is 120 seconds (`CB_MCP_TEST_TIMEOUT`).

### CI/CD Testing

The GitHub Actions workflow (`test.yml`) runs integration tests across all three transport modes (stdio, HTTP, SSE) against a Couchbase Enterprise 8.0.0 container with the `travel-sample` dataset.

---

## Submitting Changes

Before submitting a pull request:

```bash
# Ensure all linting passes
./scripts/lint.sh

# Run pre-commit checks
uv run pre-commit run --all-files

# Run tests (if you have a Couchbase cluster available)
uv run pytest tests/ -v
```

Describe your changes in the PR:
- What does this change do?
- Why is this change needed?
- How have you tested it?
