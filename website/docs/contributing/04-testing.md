---
sidebar_position: 4
title: Testing
---

# Testing

The project uses [pytest](https://docs.pytest.org/) with [pytest-asyncio](https://pytest-asyncio.readthedocs.io/) for testing.

## Running Tests

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

The test suite includes:

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

## CI/CD Testing

The GitHub Actions workflow (`test.yml`) runs integration tests across all three transport modes (stdio, HTTP, SSE) against a Couchbase Enterprise 8.0.0 container with the `travel-sample` dataset.

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
