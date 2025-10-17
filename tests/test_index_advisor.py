#!/usr/bin/env python3
"""
Test script for Index Advisor functionality.

This script demonstrates how to use the new Index Advisor tools:
1. get_index_advisor_recommendations - Get index recommendations for a query
2. create_index_from_recommendation - Create an index from recommendations

Usage:
    uv run tests/test_index_advisor.py

Prerequisites:
    - Set environment variables: CB_CONNECTION_STRING, CB_USERNAME, CB_PASSWORD
    - Have a Couchbase cluster accessible with the travel-sample bucket (or modify the query)
    - Query should use fully qualified keyspace (bucket.scope.collection)
"""

import json
import os
import sys
import traceback
from datetime import timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

from tools.index import (
    create_index_from_recommendation,
    get_index_advisor_recommendations,
)


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80 + "\n")


def create_mock_context(cluster, read_only_mode=True):
    """Create a mock context for testing."""

    class MockLifespanContext:
        def __init__(self, cluster, read_only):
            self.cluster = cluster
            self.read_only_query_mode = read_only

    class MockRequestContext:
        def __init__(self, cluster, read_only):
            self.lifespan_context = MockLifespanContext(cluster, read_only)

    class MockContext:
        def __init__(self, cluster, read_only):
            self.request_context = MockRequestContext(cluster, read_only)
            self._cluster = cluster

    return MockContext(cluster, read_only_mode)


def main():  # noqa: PLR0915
    print_section("Couchbase Index Advisor Test Script")

    # Get connection details from environment
    connection_string = os.getenv("CB_CONNECTION_STRING")
    username = os.getenv("CB_USERNAME")
    password = os.getenv("CB_PASSWORD")

    if not all([connection_string, username, password]):
        print("❌ Error: Missing required environment variables")
        print("\nPlease set the following environment variables:")
        print("  - CB_CONNECTION_STRING")
        print("  - CB_USERNAME")
        print("  - CB_PASSWORD")
        print("\nExample:")
        print(
            "  export CB_CONNECTION_STRING='couchbases://your-cluster.cloud.couchbase.com'"
        )
        print("  export CB_USERNAME='your-username'")
        print("  export CB_PASSWORD='your-password'")
        return 1

    print(f"📡 Connecting to: {connection_string}")
    print(f"👤 Username: {username}")

    try:
        # Connect to cluster
        auth = PasswordAuthenticator(username, password)
        cluster = Cluster(connection_string, ClusterOptions(auth))

        # Wait for cluster to be ready
        cluster.wait_until_ready(timedelta(seconds=10))
        print("✅ Connected successfully!\n")

    except Exception as e:
        print(f"❌ Failed to connect to Couchbase: {e}")
        return 1

    # Test 1: Get Index Advisor Recommendations
    print_section("Test 1: Get Index Advisor Recommendations")

    # You can modify this query for your specific use case
    # Note: The query should contain fully qualified keyspace (bucket.scope.collection)
    test_query = (
        "SELECT * FROM `travel-sample`.inventory.landmark "
        "WHERE activity = 'eat' AND city = 'Paris'"
    )

    print(f"Query: {test_query}\n")

    try:
        ctx = create_mock_context(cluster, read_only_mode=True)

        print("🔍 Running Index Advisor...\n")
        recommendations = get_index_advisor_recommendations(ctx, test_query)

        print("📊 Index Advisor Results:")
        print(json.dumps(recommendations, indent=2))

        # Display summary
        summary = recommendations.get("summary", {})
        print("\n📈 Summary:")
        print(f"   Current indexes used: {summary.get('current_indexes_count', 0)}")
        print(f"   Recommended indexes: {summary.get('recommended_indexes_count', 0)}")
        print(
            f"   Recommended covering indexes: {summary.get('recommended_covering_indexes_count', 0)}"
        )
        print(
            f"   Has recommendations: {'✅ Yes' if summary.get('has_recommendations') else '❌ No'}"
        )

        # Test 2: Create Index from Recommendation (if recommendations exist)
        if recommendations.get("recommended_indexes"):
            print_section("Test 2: Create Index from Recommendation")

            first_recommendation = recommendations["recommended_indexes"][0]
            index_definition = first_recommendation["index"]

            print("📝 Index to create:")
            print(f"   {index_definition}\n")

            # Ask user if they want to create the index
            print("⚠️  Note: Creating an index requires:")
            print(
                "   - Read-only mode to be disabled (CB_MCP_READ_ONLY_QUERY_MODE=false)"
            )
            print("   - Appropriate permissions on the cluster")
            print("   - The index name must not already exist")

            response = input(
                "\n❓ Do you want to attempt to create this index? (y/N): "
            )

            if response.lower() == "y":
                # Test with read-only mode disabled
                ctx_write = create_mock_context(cluster, read_only_mode=False)

                print("\n🔨 Attempting to create index...\n")
                result = create_index_from_recommendation(ctx_write, index_definition)

                print("📊 Create Index Result:")
                print(json.dumps(result, indent=2))

                if result.get("status") == "success":
                    print("\n✅ Index created successfully!")
                else:
                    print(f"\n❌ Failed to create index: {result.get('message')}")
            else:
                print("\n⏭️  Skipped index creation")

                # Still test the function with read-only mode enabled
                print(
                    "\n🧪 Testing create_index_from_recommendation with read-only mode..."
                )
                ctx_readonly = create_mock_context(cluster, read_only_mode=True)
                result = create_index_from_recommendation(
                    ctx_readonly, index_definition
                )
                print(json.dumps(result, indent=2))
        else:
            print("\nℹ️  No index recommendations available to test creation.")  # noqa: RUF001

        print_section("Test Completed Successfully")
        cluster.close()
        return 0

    except Exception as e:
        print(f"\n❌ Error during test: {e}")
        traceback.print_exc()
        cluster.close()
        return 1


if __name__ == "__main__":
    sys.exit(main())
