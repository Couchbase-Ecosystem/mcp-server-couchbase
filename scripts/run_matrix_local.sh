#!/usr/bin/env bash
# =============================================================================
# Local Test Matrix Runner
# =============================================================================
# Runs integration tests across all transport × Couchbase version combinations.
#
# Prerequisites:
#   - Docker installed and running
#   - uv installed
#   - Project dependencies installed (uv sync --extra dev)
#
# Usage:
#   ./scripts/run_matrix_local.sh
#
# Optional: run a single combo for debugging:
#   CB_VERSIONS="8.0.0" TRANSPORTS="stdio" ./scripts/run_matrix_local.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (override via env if you want a subset)
# ---------------------------------------------------------------------------
CB_VERSIONS="${CB_VERSIONS:-8.0.0 7.6.11}"
TRANSPORTS="${TRANSPORTS:-stdio http}"
CB_USERNAME="Administrator"
CB_PASSWORD="password"
CB_MCP_TEST_BUCKET="travel-sample"
CONTAINER_NAME="cb-mcp-test"
HOST_PORT=8091
KV_PORT=11210
QUERY_PORT=8093
MCP_PORT=8000

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# ---------------------------------------------------------------------------
# Cleanup on exit (success, failure, or Ctrl+C)
# ---------------------------------------------------------------------------
# Tracks background MCP server PIDs (set by run_http_tests / run_sse_tests)
# so we can reap them even if a test crashes mid-run. The container is also
# removed unconditionally so an interrupted run doesn't leak a stuck
# container that blocks the next invocation's port bindings.
declare -a BACKGROUND_PIDS=()

cleanup() {
    local exit_code=$?
    # Kill any MCP server still in BACKGROUND_PIDS. Use `kill -0` first to
    # avoid noisy "no such process" lines for ones we already reaped.
    for pid in "${BACKGROUND_PIDS[@]:-}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    # Tear down the Couchbase container. `|| true` because Ctrl+C might hit
    # before the container is ever started.
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------
declare -a RESULTS=()

record_result() {
    local version="$1" transport="$2" exit_code="$3"
    if [[ "$exit_code" -eq 0 ]]; then
        RESULTS+=("PASS|$version|$transport")
    else
        RESULTS+=("FAIL|$version|$transport")
    fi
}

# ---------------------------------------------------------------------------
# Cluster lifecycle helpers
# ---------------------------------------------------------------------------
stop_container() {
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}

start_couchbase() {
    local version="$1"
    echo ""
    echo "============================================"
    echo " Starting Couchbase Server $version"
    echo "============================================"

    stop_container

    docker run -d --name "$CONTAINER_NAME" \
        -p 8091-8096:8091-8096 \
        -p 9102:9102 \
        -p 11210:11210 \
        -p 11207:11207 \
        "couchbase:enterprise-${version}"

    echo "Waiting for Couchbase REST API..."
    for i in $(seq 1 60); do
        if curl -s http://localhost:$HOST_PORT/pools > /dev/null 2>&1; then
            echo "  REST API responding (attempt $i)"
            break
        fi
        sleep 2
    done
}

initialize_cluster() {
    echo "Initializing cluster..."

    curl -s -X POST http://localhost:$HOST_PORT/nodes/self/controller/settings \
        -d 'path=/opt/couchbase/var/lib/couchbase/data' \
        -d 'index_path=/opt/couchbase/var/lib/couchbase/data'

    curl -s -X POST http://localhost:$HOST_PORT/node/controller/setupServices \
        -d 'services=kv,n1ql,index,fts'

    curl -s -X POST http://localhost:$HOST_PORT/pools/default \
        -d 'memoryQuota=512' \
        -d 'indexMemoryQuota=256' \
        -d 'ftsMemoryQuota=256'

    curl -s -X POST http://localhost:$HOST_PORT/settings/web \
        -d "password=$CB_PASSWORD" \
        -d "username=$CB_USERNAME" \
        -d 'port=SAME'

    
    curl -s -X PUT \
        -u "$CB_USERNAME:$CB_PASSWORD" \
        "http://localhost:$HOST_PORT/node/controller/setupAlternateAddresses/external" \
        -d "hostname=127.0.0.1"

    echo "  Cluster initialized."
}

load_sample_bucket() {
    echo "Loading travel-sample bucket..."
    sleep 5

    curl -s -X POST http://localhost:$HOST_PORT/sampleBuckets/install \
        -u "$CB_USERNAME:$CB_PASSWORD" \
        -H "Content-Type: application/json" \
        -d '["travel-sample"]'

    echo "  Waiting for bucket to be healthy..."
    for i in $(seq 1 60); do
        if curl -s -u "$CB_USERNAME:$CB_PASSWORD" \
            http://localhost:$HOST_PORT/pools/default/buckets/travel-sample 2>/dev/null \
            | grep -q '"status":"healthy"'; then
            echo "  Bucket healthy (attempt $i)"
            break
        fi
        sleep 3
    done

    # Wait for KV service to accept connections
    echo "  Waiting for KV port ($KV_PORT) to be ready..."
    for i in $(seq 1 30); do
        if nc -z 127.0.0.1 "$KV_PORT" 2>/dev/null; then
            echo "  KV port responding (attempt $i)"
            break
        fi
        sleep 2
    done

    # Wait for bucket data to be loaded (itemCount > 0)
    echo "  Waiting for bucket data to load..."
    for i in $(seq 1 60); do
        item_count=$(curl -s -u "$CB_USERNAME:$CB_PASSWORD" \
            http://localhost:$HOST_PORT/pools/default/buckets/travel-sample \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('basicStats',{}).get('itemCount',0))" 2>/dev/null || echo 0)
        if [[ "$item_count" -gt 0 ]]; then
            echo "  Bucket has $item_count items (attempt $i)"
            break
        fi
        sleep 3
    done
    sleep 5
}

set_indexer_mode() {
    echo "Setting indexer storage mode..."
    sleep 5
    curl -s -X POST http://localhost:$HOST_PORT/settings/indexes \
        -u "$CB_USERNAME:$CB_PASSWORD" \
        -d 'storageMode=plasma' || true
    sleep 5
}

setup_test_data() {
    echo "Running setup_test_data.py..."
    for attempt in $(seq 1 5); do
        if CB_CONNECTION_STRING="couchbase://127.0.0.1" \
           CB_USERNAME="$CB_USERNAME" \
           CB_PASSWORD="$CB_PASSWORD" \
           CB_MCP_TEST_BUCKET="$CB_MCP_TEST_BUCKET" \
           PYTHONPATH=src \
               uv run python scripts/setup_test_data.py; then
            echo "  setup_test_data.py succeeded."
            return 0
        fi
        echo "  setup_test_data.py failed (attempt $attempt/5), retrying in 15s..."
        sleep 15
    done
    echo "  ERROR: setup_test_data.py failed after 5 attempts."
    return 1
}

# ---------------------------------------------------------------------------
# Test runners per transport
# ---------------------------------------------------------------------------
run_stdio_tests() {
    echo "  [stdio] Running tests..."
    CB_CONNECTION_STRING="couchbase://127.0.0.1" \
    CB_USERNAME="$CB_USERNAME" \
    CB_PASSWORD="$CB_PASSWORD" \
    CB_MCP_TRANSPORT="stdio" \
    CB_MCP_TEST_BUCKET="$CB_MCP_TEST_BUCKET" \
    CB_MCP_TEST_SCOPE="inventory" \
    CB_MCP_TEST_COLLECTION="airline" \
    PYTHONPATH=src \
        uv run pytest tests/ -v --tb=short
}

run_http_tests() {
    echo "  [http] Starting MCP server..."
    CB_CONNECTION_STRING="couchbase://127.0.0.1" \
    CB_USERNAME="$CB_USERNAME" \
    CB_PASSWORD="$CB_PASSWORD" \
    CB_MCP_TRANSPORT="http" \
    CB_MCP_HOST="127.0.0.1" \
    CB_MCP_PORT="$MCP_PORT" \
    CB_MCP_READ_ONLY_MODE="false" \
    PYTHONPATH=src \
        uv run python -m mcp_server &
    local server_pid=$!
    BACKGROUND_PIDS+=("$server_pid")

    # Wait for server
    for i in $(seq 1 30); do
        if nc -z 127.0.0.1 "$MCP_PORT" 2>/dev/null; then
            break
        fi
        sleep 1
    done

    echo "  [http] Running tests..."
    local ret=0
    CB_CONNECTION_STRING="couchbase://127.0.0.1" \
    CB_USERNAME="$CB_USERNAME" \
    CB_PASSWORD="$CB_PASSWORD" \
    CB_MCP_TRANSPORT="http" \
    MCP_SERVER_URL="http://127.0.0.1:${MCP_PORT}/mcp" \
    CB_MCP_TEST_BUCKET="$CB_MCP_TEST_BUCKET" \
    CB_MCP_TEST_SCOPE="inventory" \
    CB_MCP_TEST_COLLECTION="airline" \
    PYTHONPATH=src \
        uv run pytest tests/ -v --tb=short || ret=$?

    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
    return $ret
}



# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
echo "======================================================"
echo " MCP Server Local Test Matrix"
echo " Versions:   $CB_VERSIONS"
echo " Transports: $TRANSPORTS"
echo "======================================================"

for version in $CB_VERSIONS; do
    start_couchbase "$version"
    initialize_cluster
    load_sample_bucket
    set_indexer_mode
    setup_test_data

    for transport in $TRANSPORTS; do
        echo ""
        echo "--------------------------------------------"
        echo " Testing: cb-$version / $transport"
        echo "--------------------------------------------"

        ret=0
        case "$transport" in
            stdio) run_stdio_tests || ret=$? ;;
            http)  run_http_tests  || ret=$? ;;
            # sse)   run_sse_tests   || ret=$? ;;
            *)     echo "Unknown transport: $transport"; ret=1 ;;
        esac

        record_result "$version" "$transport" "$ret"
    done

    stop_container
done

# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------
echo ""
echo ""
echo "======================================================"
echo " RESULTS SUMMARY"
echo "======================================================"
printf "%-8s | %-12s | %-10s\n" "Status" "CB Version" "Transport"
printf "%-8s-+-%-12s-+-%-10s\n" "--------" "------------" "----------"

any_failed=0
for entry in "${RESULTS[@]}"; do
    IFS='|' read -r result_status cb_ver transport_name <<< "$entry"
    if [[ "$result_status" == "PASS" ]]; then
        printf "  %-6s | %-12s | %-10s\n" "✅ PASS" "$cb_ver" "$transport_name"
    else
        printf "  %-6s | %-12s | %-10s\n" "❌ FAIL" "$cb_ver" "$transport_name"
        any_failed=1
    fi
done

echo "======================================================"
echo ""

if [[ "$any_failed" -eq 1 ]]; then
    echo "Some combinations FAILED."
    exit 1
else
    echo "All combinations PASSED."
    exit 0
fi
