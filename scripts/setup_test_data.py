#!/usr/bin/env python3
"""
Setup script to populate system:completed_requests for performance analysis tests.

This script:
1. Enables query logging in Couchbase (sets completed-threshold to 0)
2. Runs various queries to populate system:completed_requests
3. Ensures performance analysis tests have data to validate against

Usage:
    python scripts/setup_test_data.py

Environment variables required:
    CB_CONNECTION_STRING - Couchbase connection string (e.g., couchbase://localhost)
    CB_USERNAME - Couchbase username
    CB_PASSWORD - Couchbase password
    CB_MCP_TEST_BUCKET - Test bucket name (e.g., travel-sample)

This script should be run before pytest to ensure performance tests don't skip.
"""

import os
import sys
import urllib.request
import urllib.error
import base64
import json

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta


def get_env_or_exit(var_name: str) -> str:
    """Get environment variable or exit with error."""
    value = os.getenv(var_name)
    if not value:
        print(f"Error: {var_name} environment variable is required")
        sys.exit(1)
    return value


def enable_query_logging(connection_string: str, username: str, password: str) -> bool:
    """Enable query logging by setting completed-threshold to 0."""
    # Extract host from connection string
    host = connection_string.replace("couchbase://", "").replace("couchbases://", "")
    host = host.split(",")[0]  # Take first host if multiple
    host = host.split(":")[0]  # Remove port if present

    url = f"http://{host}:8093/admin/settings"
    data = json.dumps({"completed-threshold": 0, "completed-limit": 10000}).encode()

    # Create request with basic auth
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {credentials}",
    }

    try:
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                print("  - Query logging enabled (completed-threshold=0)")
                return True
    except urllib.error.URLError as e:
        print(f"  - Warning: Could not enable query logging: {e}")
        print("    (Performance tests may be skipped)")
    except Exception as e:
        print(f"  - Warning: Error enabling query logging: {e}")

    return False


def check_scope_exists(bucket, scope_name: str) -> bool:
    """Check if a scope exists in the bucket."""
    try:
        scopes = bucket.collections().get_all_scopes()
        return any(s.name == scope_name for s in scopes)
    except Exception:
        return False


def run_test_queries_inventory(cluster: Cluster, bucket_name: str) -> None:
    """Run queries against travel-sample inventory scope (full sample data)."""
    bucket = cluster.bucket(bucket_name)
    scope = bucket.scope("inventory")

    print("  Using inventory scope (travel-sample with sample data)...")

    # Regular SELECT queries (for longest running, most frequent, response sizes)
    print("  - Running regular SELECT queries...")
    for _ in range(3):
        result = scope.query("SELECT * FROM airline LIMIT 100")
        list(result)

    result = scope.query("SELECT * FROM route LIMIT 500")
    list(result)

    # Queries using PRIMARY index
    print("  - Running queries using primary index...")
    try:
        result = scope.query(
            "SELECT META().id, * FROM airline WHERE LOWER(name) LIKE '%air%' LIMIT 10"
        )
        list(result)
    except Exception:
        pass  # May fail depending on data

    # Queries that fetch after index scan (not using covering index)
    print("  - Running queries requiring fetch after index scan...")
    result = scope.query(
        "SELECT * FROM airline WHERE country = 'United States' LIMIT 50"
    )
    list(result)

    for _ in range(3):
        result = scope.query(
            "SELECT id, name, type, iata FROM airline WHERE country = 'United States'"
        )
        list(result)

    # Queries that are non-selective (indexScan > resultCount)
    print("  - Running non-selective queries (using secondary indexes)...")

    # Use sourceairport index on route
    try:
        result = scope.query("""
            SELECT * FROM route 
            WHERE sourceairport >= 'A' AND sourceairport < 'Z'
            AND destinationairport = 'XXXXX'
        """)
        list(result)
    except Exception:
        pass

    # Use city index on airport
    try:
        result = scope.query("""
            SELECT * FROM airport 
            WHERE city >= 'A' AND city < 'Z'
            AND faa = 'XXXX'
        """)
        list(result)
    except Exception:
        pass

    # Use city index on hotel
    try:
        result = scope.query("""
            SELECT * FROM hotel 
            WHERE city >= 'A' AND city < 'Z'
            AND name LIKE 'ZZZZZZ%'
        """)
        list(result)
    except Exception:
        pass

    # Use faa index on airport
    try:
        result = scope.query("""
            SELECT * FROM airport 
            WHERE faa >= 'A' AND faa < 'Z'
            AND country = 'XXXXXX'
        """)
        list(result)
    except Exception:
        pass

    # Additional varied queries
    print("  - Running additional varied queries...")
    result = scope.query("SELECT COUNT(*) as cnt FROM airline")
    list(result)

    result = scope.query("SELECT DISTINCT country FROM airline")
    list(result)

    result = scope.query("SELECT name, country FROM airline ORDER BY name LIMIT 20")
    list(result)


def run_test_queries_default(cluster: Cluster, bucket_name: str) -> None:
    """Run queries against _default scope (CI environment with basic bucket)."""
    bucket = cluster.bucket(bucket_name)
    scope = bucket.scope("_default")
    collection_name = "_default"

    print("  Using _default scope (CI environment)...")

    # Regular SELECT queries (for longest running, most frequent, response sizes)
    print("  - Running regular SELECT queries...")
    for _ in range(5):
        try:
            result = scope.query(f"SELECT * FROM `{collection_name}` LIMIT 100")
            list(result)
        except Exception:
            pass

    # Queries using PRIMARY index
    print("  - Running queries using primary index...")
    try:
        result = scope.query(
            f"SELECT META().id, * FROM `{collection_name}` LIMIT 10"
        )
        list(result)
    except Exception:
        pass

    # Queries with filters
    print("  - Running queries with filters...")
    try:
        result = scope.query(
            f"SELECT * FROM `{collection_name}` WHERE type = 'test' LIMIT 50"
        )
        list(result)
    except Exception:
        pass

    for _ in range(3):
        try:
            result = scope.query(
                f"SELECT * FROM `{collection_name}` WHERE id > 0"
            )
            list(result)
        except Exception:
            pass

    # Additional varied queries
    print("  - Running additional varied queries...")
    try:
        result = scope.query(f"SELECT COUNT(*) as cnt FROM `{collection_name}`")
        list(result)
    except Exception:
        pass

    try:
        result = scope.query(f"SELECT DISTINCT type FROM `{collection_name}`")
        list(result)
    except Exception:
        pass

    try:
        result = scope.query(f"SELECT * FROM `{collection_name}` ORDER BY META().id LIMIT 20")
        list(result)
    except Exception:
        pass


def run_test_queries(cluster: Cluster, bucket_name: str) -> None:
    """Run various queries to populate system:completed_requests."""
    bucket = cluster.bucket(bucket_name)

    print("\n2. Running queries to populate system:completed_requests...")

    # Check if inventory scope exists (travel-sample with full data)
    if check_scope_exists(bucket, "inventory"):
        run_test_queries_inventory(cluster, bucket_name)
    else:
        # Fall back to _default scope (CI environment)
        run_test_queries_default(cluster, bucket_name)


def verify_completed_requests(cluster: Cluster) -> int:
    """Verify that system:completed_requests has data."""
    result = cluster.query("""
        SELECT COUNT(*) as cnt
        FROM system:completed_requests
        WHERE UPPER(statement) NOT LIKE 'INFER %'
            AND UPPER(statement) NOT LIKE '% SYSTEM:%'
    """)
    rows = list(result)
    return rows[0].get("cnt", 0) if rows else 0


def main() -> int:
    """Main entry point."""
    print("Setting up test data for performance analysis tests...")

    # Get required environment variables
    connection_string = get_env_or_exit("CB_CONNECTION_STRING")
    username = get_env_or_exit("CB_USERNAME")
    password = get_env_or_exit("CB_PASSWORD")
    bucket_name = get_env_or_exit("CB_MCP_TEST_BUCKET")

    print(f"\nConnecting to {connection_string}...")
    print(f"Using bucket: {bucket_name}")

    # Enable query logging
    print("\n1. Enabling query logging...")
    enable_query_logging(connection_string, username, password)

    # Connect to cluster
    auth = PasswordAuthenticator(username, password)
    cluster = Cluster(connection_string, ClusterOptions(auth))

    try:
        cluster.wait_until_ready(timedelta(seconds=30))
        print("  - Connected to cluster")

        # Run test queries
        run_test_queries(cluster, bucket_name)

        # Verify data
        print("\n3. Verifying system:completed_requests...")
        count = verify_completed_requests(cluster)
        print(f"  - Found {count} completed requests")

        if count > 0:
            print("\n✓ Test data setup complete!")
            return 0
        else:
            print("\n⚠ Warning: No completed requests found.")
            print("  Performance analysis tests may be skipped.")
            return 0  # Don't fail, tests will skip gracefully

    except Exception as e:
        print(f"\nError: {e}")
        return 1
    finally:
        cluster.close()


if __name__ == "__main__":
    sys.exit(main())
