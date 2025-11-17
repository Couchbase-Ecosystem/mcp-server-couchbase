"""
Catalog module for background schema monitoring.

This module provides:
- Worker loop for collecting schema information every 5 minutes
- Thread management utilities for starting/stopping the background worker
- Stores data in a thread-safe global Store
- Detects schema changes and flags for enrichment

For enrichment functionality (MCP thread), see the enrichment module at src level.
"""

# Import from worker module
from catalog.worker import (
    CATALOG_REFRESH_INTERVAL,
    catalog_worker_loop,
    get_catalog_store,
)

__all__ = [
    "catalog_worker_loop",
    "get_catalog_store",
    "CATALOG_REFRESH_INTERVAL",
]

