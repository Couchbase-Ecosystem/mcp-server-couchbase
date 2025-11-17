"""
Thread management for catalog background worker.

This module provides thread management functions that can be used by:
- The main MCP server (mcp_server.py)
- Standalone verification/test scripts (verify.py)
"""

import logging
import threading
from typing import Optional

from catalog.worker import catalog_worker_loop, CATALOG_REFRESH_INTERVAL
from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.catalog.thread_manager")

# Global thread management for catalog background worker
_catalog_thread: Optional[threading.Thread] = None
_catalog_stop_event: Optional[threading.Event] = None


def start_catalog_thread() -> None:
    """Start the catalog background thread."""
    global _catalog_thread, _catalog_stop_event
    
    if _catalog_thread is not None and _catalog_thread.is_alive():
        logger.warning("Catalog background thread is already running")
        return
    
    logger.info("Starting catalog background thread")
    _catalog_stop_event = threading.Event()
    _catalog_thread = threading.Thread(
        target=catalog_worker_loop,
        args=(_catalog_stop_event,),
        daemon=True,
        name="CatalogBackgroundWorker"
    )
    _catalog_thread.start()
    logger.info("Catalog background thread started")


def stop_catalog_thread() -> None:
    """Stop the catalog background thread."""
    global _catalog_thread, _catalog_stop_event
    
    if _catalog_thread is None or not _catalog_thread.is_alive():
        logger.warning("Catalog background thread is not running")
        return
    
    logger.info("Stopping catalog background thread")
    _catalog_stop_event.set()
    _catalog_thread.join(timeout=CATALOG_REFRESH_INTERVAL + 10)
    
    if _catalog_thread.is_alive():
        logger.warning("Catalog background thread did not stop gracefully")
    else:
        logger.info("Catalog background thread stopped")
    
    _catalog_thread = None
    _catalog_stop_event = None


def is_catalog_thread_running() -> bool:
    """Check if the catalog background thread is running."""
    global _catalog_thread
    return _catalog_thread is not None and _catalog_thread.is_alive()

