# Contributing to Couchbase MCP Server

Thank you for your interest in contributing to the Couchbase MCP Server! This guide will help you set up your development environment and understand our development workflow.

## 🚀 Development Setup

### Prerequisites

- **Python 3.10+**: Required for the project
- **[uv](https://docs.astral.sh/uv/)**: Fast Python package installer and dependency manager
- **Git**: For version control
- **VS Code** (recommended): With Python extension for the best development experience

### Clone and Setup

```bash
# Clone the repository
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
cd mcp-server-couchbase

# Install dependencies (including development tools)
uv sync --extra dev
```

### Install Development Tools

```bash
# Install pre-commit hooks (runs linting on every commit)
uv run pre-commit install

# Verify installation
uv run pre-commit run --all-files
```

## 🧹 Code Quality & Linting

We use **[Ruff](https://docs.astral.sh/ruff/)** for fast linting and code formatting to maintain consistent code quality.

### Manual Linting

```bash
# Check code quality (no changes made)
./scripts/lint.sh
# or: uv run ruff check src/

# Auto-fix issues
./scripts/fix_lint.sh
# or: uv run ruff check src/ --fix && uv run ruff format src/
```

### Automatic Linting

- **Pre-commit hooks**: Ruff runs automatically on every `git commit`
- **VS Code**: Auto-format on save using [Ruff extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)

### Linting Rules

Our Ruff configuration includes:

- **Code style**: PEP 8 compliance with 88-character line limit
- **Import organization**: Automatic import sorting and cleanup
- **Code quality**: Detection of unused variables, simplification opportunities
- **Modern Python**: Encourages modern Python patterns with `pyupgrade`

## 🏗️ Project Structure

```
mcp-server-couchbase/
├── src/
│   ├── mcp_server.py              # MCP server entry point
│   ├── certs/                     # SSL/TLS certificates
│   │   ├── __init__.py            # Package marker
│   │   └── capella_root_ca.pem    # Capella root CA certificate (for Capella connections)
│   ├── tools/                     # MCP tool implementations
│   │   ├── __init__.py            # Tool exports and ALL_TOOLS list
│   │   ├── server.py              # Server status and connection tools
│   │   ├── kv.py                  # Key-value operations (CRUD)
│   │   ├── query.py               # SQL++ query operations
│   │   └── index.py               # Index operations and recommendations
│   └── utils/                     # Utility modules
│       ├── __init__.py            # Utility exports
│       ├── constants.py           # Project constants
│       ├── config.py              # Configuration management
│       ├── connection.py          # Couchbase connection handling
│       ├── context.py             # Application context management
│       └── index_utils.py         # Index-related helper functions
├── scripts/                       # Development scripts
│   ├── lint.sh                    # Manual linting script
│   └── lint_fix.sh                # Auto-fix linting issues
├── .pre-commit-config.yaml        # Pre-commit hook configuration
├── pyproject.toml                 # Project dependencies and Ruff config
├── CONTRIBUTING.md                # Contribution Guide
└── README.md                      # Usage
```

## 🛠️ Development Workflow

### Making Changes

1. **Create a branch** for your feature/fix:

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the existing patterns

3. **Test your changes**:

   ```bash
   # Run linting
   ./scripts/lint.sh

   # Test the MCP server
   uv run src/mcp_server.py --help
   ```

4. **Commit your changes**:

   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

   The pre-commit hooks will automatically run and fix any formatting issues.

### Adding New Tools

When adding new MCP tools:

1. **Create the tool function** in the appropriate module (in `tools` directory)
2. **Export the tool** in `tools/__init__.py`
3. **Add to ALL_TOOLS** list in `tools/__init__.py`
4. **Test the tool** with an MCP client

### Code Style Guidelines

- **Line length**: 88 characters (enforced by Ruff)
- **Import organization**: Use isort-style grouping (standard library, third-party, local)
- **Type hints**: Use modern Python type hints where helpful
- **Docstrings**: Add docstrings for public functions and classes
- **Error handling**: Include appropriate exception handling with logging

## 🧪 Testing

### Manual Testing

Currently, testing is done manually with MCP clients:

1. **Set up environment variables** for your Couchbase cluster
2. **Run the server** with an MCP client like Claude Desktop
3. **Test tool functionality** through the client interface

### Future Testing Plans

We plan to add:

- Unit tests for utility functions
- Integration tests
- Automated testing in CI/CD

## 📋 Adding New Features

### Before You Start

1. **Check existing issues** to see if someone is already working on it
2. **Open an issue** to discuss larger changes
3. **Review the codebase** to understand existing patterns

### Implementation Guidelines

1. **Follow existing patterns**: Look at similar tools for guidance
2. **Use the utility modules**: Leverage existing connection and context management
3. **Add proper logging**: Use the hierarchical logging system
4. **Handle errors gracefully**: Provide helpful error messages
5. **Update documentation**: Update README.md if adding user-facing features

## 🤝 Submitting Changes

1. **Run final checks**:

   ```bash
   # Ensure all linting passes
   ./scripts/lint.sh

   # Test with pre-commit
   uv run pre-commit run --all-files
   ```

2. **Push your branch** and create a pull request

3. **Describe your changes** in the PR description:
   - What does this change do?
   - Why is this change needed?
   - How have you tested it?

## 💡 Tips for Contributors

### Common Development Tasks

```bash
# Install new dependencies
uv add package-name

# Install new dev dependencies
uv add --dev package-name

# Update dependencies
uv sync

# Run the server for testing
uv run src/mcp_server.py --connection-string "..." --username "..." --password "..." --bucket-name "..."
```

### Debugging

- **Use logging**: The project uses hierarchical logging with the pattern `logger = logging.getLogger(f"{MCP_SERVER_NAME}.module.name")`
- **Check connection**: Ensure your Couchbase cluster is accessible
- **Validate configuration**: Make sure all required environment variables are set

## 📖 Additional Resources

- **[Model Context Protocol Documentation](https://modelcontextprotocol.io/)**
- **[Couchbase Python SDK Documentation](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html)**
- **[SQL++ Query Language](https://www.couchbase.com/sqlplusplus/)**
- **[Ruff Documentation](https://docs.astral.sh/ruff/)**

## 🆘 Getting Help

- **Open an issue** for bugs or feature requests
- **Check existing issues** for similar problems
- **Review the code** for examples and patterns

Thank you for contributing to the Couchbase MCP Server! 🚀
