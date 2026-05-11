#!/bin/bash
set -euo pipefail

# Get image name parameter (default: mcp/couchbase-src)
IMAGE_NAME="${1:-mcp/couchbase-src}"

# Get git information
GIT_COMMIT=$(git rev-parse HEAD)
GIT_SHORT_COMMIT=$(git rev-parse --short HEAD)
BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

echo "Building Docker image with:"
echo "  Image Name: $IMAGE_NAME"
echo "  Git Commit: $GIT_COMMIT"
echo "  Build Date: $BUILD_DATE"

# Build the Docker image
docker build \
  --build-arg GIT_COMMIT_HASH="$GIT_COMMIT" \
  --build-arg BUILD_DATE="$BUILD_DATE" \
  -t "$IMAGE_NAME:$GIT_SHORT_COMMIT" \
  -t "$IMAGE_NAME:latest" \
  .

echo "Build complete!"
echo "Tagged as:"
echo "  - $IMAGE_NAME:$GIT_SHORT_COMMIT"
echo "  - $IMAGE_NAME:latest"
