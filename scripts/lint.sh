#!/bin/bash
# Development linting script

set -e

echo "🔍 Running Ruff linting..."
echo "================================"

# Check if ruff is available
if ! command -v ruff &> /dev/null; then
    echo "❌ Ruff not found. Install with: uv add --dev ruff"
    exit 1
fi

# Run linting
echo "📋 Checking code style and errors..."
ruff check src/ --diff

echo ""
echo "🎨 Checking code formatting..."
ruff format src/ --diff --check

echo ""
echo "✅ Linting complete!"
echo ""
echo "To auto-fix issues, run:"
echo "  ruff check src/ --fix"
echo "  ruff format src/"
