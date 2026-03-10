---
sidebar_position: 2
title: Code Quality
---

# Code Quality

The project uses [Ruff](https://docs.astral.sh/ruff/) for fast linting and code formatting.

## Manual Linting

```bash
# Check code quality (no changes made)
./scripts/lint.sh
# or: uv run ruff check src/

# Auto-fix issues
./scripts/fix_lint.sh
# or: uv run ruff check src/ --fix && uv run ruff format src/
```

## Automatic Linting

- **Pre-commit hooks**: Ruff runs automatically on every `git commit`.
- **VS Code**: Auto-format on save using the [Ruff extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff).

## Linting Rules

The Ruff configuration in `pyproject.toml` includes:

- **Code style**: PEP 8 compliance with 88-character line limit
- **Import organization**: Automatic import sorting and cleanup
- **Code quality**: Detection of unused variables, simplification opportunities
- **Modern Python**: Encourages modern Python patterns with `pyupgrade`

## Code Style Guidelines

- **Line length**: 88 characters (enforced by Ruff)
- **Import organization**: Use isort-style grouping (standard library, third-party, local)
- **Type hints**: Use modern Python type hints where helpful
- **Docstrings**: Add docstrings for public functions and classes
- **Error handling**: Include appropriate exception handling with logging
