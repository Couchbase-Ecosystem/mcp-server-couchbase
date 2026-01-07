"""
Events module for catalog cross-thread communication.

Provides a thread-safe bridge for signaling between the background
catalog worker thread and the MCP server's async event loop.
"""

from .bridge import ThreadToAsyncBridge, get_enrichment_bridge

__all__ = ["ThreadToAsyncBridge", "get_enrichment_bridge"]
