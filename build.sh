#!/bin/bash
set -euo pipefail

# Get git information
GIT_COMMIT=$(git rev-parse HEAD)
GIT_SHORT_COMMIT=$(git rev-parse --short HEAD)
BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

echo "Building Docker image with:"
echo "  Git Commit: $GIT_COMMIT"
echo "  Build Date: $BUILD_DATE"

# Build the Docker image
docker build \
  --build-arg GIT_COMMIT_HASH="$GIT_COMMIT" \
  --build-arg BUILD_DATE="$BUILD_DATE" \
  -t "mcp/couchbase:$GIT_SHORT_COMMIT" \
  -t "mcp/couchbase:latest" \
  .

echo "Build complete!"
echo "Tagged as:"
echo "  - mcp/couchbase:$GIT_SHORT_COMMIT"
echo "  - mcp/couchbase:latest"
