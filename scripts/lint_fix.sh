#!/bin/bash
# Auto-fix linting issues script

set -e

echo "🔧 Auto-fixing code issues..."
echo "================================"

# Check if ruff is available
if ! command -v ruff &> /dev/null; then
    echo "❌ Ruff not found. Install with: uv add --dev ruff"
    exit 1
fi

# Auto-fix linting issues
echo "📋 Fixing linting issues..."
ruff check src/ --fix

echo ""
echo "🎨 Formatting code..."
ruff format src/

echo ""
echo "✅ Auto-fix complete!"
echo ""
echo "Review the changes and commit if everything looks good."
