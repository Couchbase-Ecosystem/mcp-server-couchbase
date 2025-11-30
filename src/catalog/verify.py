"""
Verification script for the catalog system.

This script can be used to manually test and verify the catalog system components.
"""

import logging
import time
import click
from catalog.store.store import get_catalog_store
from catalog_manager import (
    is_catalog_thread_running,
    start_catalog_thread,
    stop_catalog_thread,
)
from utils import (
    DEFAULT_READ_ONLY_MODE,
    DEFAULT_TRANSPORT,
    DEFAULT_HOST,
    DEFAULT_PORT,
    ALLOWED_TRANSPORTS,
    set_settings,
)
from utils.constants import MCP_SERVER_NAME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("catalog.verify")


def verify_store():
    """Verify the Store functionality."""
    logger.info("Testing Store...")
    
    store = get_catalog_store()
    
    # Test database info
    test_info = {
        "buckets": {
            "test_bucket": {
                "name": "test_bucket",
                "scopes": {
                    "test_scope": {
                        "name": "test_scope",
                        "collections": {
                            "test_collection": {
                                "name": "test_collection",
                                "schema": [{"field": "id", "type": "string"}]
                            }
                        }
                    }
                }
            }
        }
    }
    
    store.add_database_info(test_info)
    retrieved = store.get_database_info()
    
    assert retrieved == test_info, "Database info mismatch"
    logger.info("✓ Database info storage works")
    
    # Test prompt
    test_prompt = "This is a test prompt"
    store.add_prompt(test_prompt)
    assert store.get_prompt() == test_prompt, "Prompt mismatch"
    logger.info("✓ Prompt storage works")
    
    # Test enrichment flag
    store.set_needs_enrichment(True)
    assert store.get_needs_enrichment() is True, "Enrichment flag mismatch"
    store.clear_needs_enrichment()
    assert store.get_needs_enrichment() is False, "Clear enrichment flag failed"
    logger.info("✓ Enrichment flag works")
    
    # Test hash
    test_hash = "abc123"
    store.set_schema_hash(test_hash)
    assert store.get_schema_hash() == test_hash, "Hash mismatch"
    logger.info("✓ Schema hash storage works")
    
    logger.info("Store verification completed successfully!")


def verify_catalog_thread():
    """Verify the catalog thread functionality."""
    logger.info("Testing Catalog Thread...")
    
    # Start the thread
    start_catalog_thread()
    
    # Check if running
    assert is_catalog_thread_running(), "Thread should be running"
    logger.info("✓ Thread started successfully")
    
    # Wait a bit
    logger.info("Waiting 10 seconds to observe thread behavior...")
    time.sleep(10)
    
    # Check store for any updates
    store = get_catalog_store()
    database_info = store.get_database_info()

    
    if database_info and database_info.get("buckets"):
        logger.info(f"✓ Thread collected {len(database_info['buckets'])} bucket(s)")
        for bucket_name, bucket_data in database_info["buckets"].items():
            scopes = bucket_data.get("scopes", {})
            logger.info(f"  - Bucket '{bucket_name}': {len(scopes)} scope(s)")
    else:
        logger.info("⚠ No database info collected (may need valid connection)")

    stop_catalog_thread()    
    
    logger.info("Catalog thread verification completed!")




@click.command()
@click.option(
    "--connection-string",
    envvar="CB_CONNECTION_STRING",
    help="Couchbase connection string (required for operations)",
)
@click.option(
    "--username",
    envvar="CB_USERNAME",
    help="Couchbase database user (required for operations)",
)
@click.option(
    "--password",
    envvar="CB_PASSWORD",
    help="Couchbase database password (required for operations)",
)
@click.option(
    "--ca-cert-path",
    envvar="CB_CA_CERT_PATH",
    default=None,
    help="Path to the server trust store (CA certificate) file. The certificate at this path is used to verify the server certificate during the authentication process.",
)
@click.option(
    "--client-cert-path",
    envvar="CB_CLIENT_CERT_PATH",
    default=None,
    help="Path to the client certificate file used for mTLS authentication.",
)
@click.option(
    "--client-key-path",
    envvar="CB_CLIENT_KEY_PATH",
    default=None,
    help="Path to the client certificate key file used for mTLS authentication.",
)
@click.option(
    "--read-only-query-mode",
    envvar=[
        "CB_MCP_READ_ONLY_QUERY_MODE",
        "READ_ONLY_QUERY_MODE",  # Deprecated
    ],
    type=bool,
    default=DEFAULT_READ_ONLY_MODE,
    help="Enable read-only query mode. Set to True (default) to allow only read-only queries. Can be set to False to allow data modification queries.",
)
@click.option(
    "--transport",
    envvar=[
        "CB_MCP_TRANSPORT",
        "MCP_TRANSPORT",  # Deprecated
    ],
    type=click.Choice(ALLOWED_TRANSPORTS),
    default=DEFAULT_TRANSPORT,
    help="Transport mode for the server (stdio, http or sse). Default is stdio",
)
@click.option(
    "--host",
    envvar="CB_MCP_HOST",
    default=DEFAULT_HOST,
    help="Host to run the server on (default: 127.0.0.1)",
)
@click.option(
    "--port",
    envvar="CB_MCP_PORT",
    default=DEFAULT_PORT,
    help="Port to run the server on (default: 8000)",
)
@click.version_option(package_name="couchbase-mcp-server")
@click.pass_context
def main(
    ctx,
    connection_string,
    username,
    password,
    ca_cert_path,
    client_cert_path,
    client_key_path,
    read_only_query_mode,
    transport,
    host,
    port,
):
    """Couchbase MCP Server"""
    # Store configuration in context
    # Store configuration in context
    set_settings({
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "ca_cert_path": ca_cert_path,
        "client_cert_path": client_cert_path,
        "client_key_path": client_key_path,
        "read_only_query_mode": read_only_query_mode,
        "transport": transport,
        "host": host,
        "port": port,
    })

    logger.info("=" * 60)
    logger.info("Catalog System Verification")
    logger.info("=" * 60)
    
    try:
        # Verify store
        verify_store()
        logger.info("")
        
        # Verify catalog thread (requires connection settings)
        logger.info("Note: Thread verification requires valid Couchbase connection")
        logger.info("Set environment variables: CB_CONNECTION_STRING, CB_USERNAME, CB_PASSWORD")
        verify_catalog_thread()
        
    except Exception as e:
        logger.error(f"Verification failed: {e}", exc_info=True)
    
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

