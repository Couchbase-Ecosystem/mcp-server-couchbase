"""
Thread-to-async event bridge for cross-thread communication.

This module provides a thread-safe mechanism for signaling from the
background catalog worker thread to the MCP server's async event loop.

The challenge: asyncio.Event is NOT thread-safe between different threads.
This bridge uses threading.Event combined with loop.call_soon_threadsafe()
to safely signal across thread boundaries.
"""

import asyncio
import logging
import threading
from typing import Optional

from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.events")


class ThreadToAsyncBridge:
    """
    Bridge for signaling from a background thread to an async event loop.

    This class provides a thread-safe way to trigger actions in an async
    event loop from a separate background thread.

    Usage:
        # In main async context (MCP server startup):
        bridge = ThreadToAsyncBridge()
        bridge.set_target_loop(asyncio.get_event_loop())

        # In background thread:
        bridge.signal_from_thread()

        # In async context:
        await bridge.wait_for_signal(timeout=60.0)
    """

    def __init__(self):
        """Initialize the bridge with no target loop."""
        self._thread_event = threading.Event()
        self._async_event: Optional[asyncio.Event] = None
        self._target_loop: Optional[asyncio.AbstractEventLoop] = None
        self._lock = threading.Lock()

    def set_target_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Set the target async event loop (should be the MCP server's loop).

        This must be called from the async context that will wait for signals.

        Args:
            loop: The asyncio event loop to signal
        """
        with self._lock:
            self._target_loop = loop
            # Create a new async event for this loop
            self._async_event = asyncio.Event()
            logger.debug("ThreadToAsyncBridge: target loop set")

    def signal_from_thread(self) -> None:
        """
        Signal from a background thread that an event has occurred.

        This method is thread-safe and can be called from any thread.
        It will trigger the async event in the target loop.
        """
        with self._lock:
            # Set the thread event (for any thread-based waiters)
            self._thread_event.set()

            # Signal the async event in the target loop
            if self._target_loop and self._async_event:
                try:
                    # call_soon_threadsafe schedules the callback on the target loop
                    self._target_loop.call_soon_threadsafe(self._async_event.set)
                    logger.debug("ThreadToAsyncBridge: signal sent to async loop")
                except RuntimeError:
                    # Loop might be closed
                    logger.warning("ThreadToAsyncBridge: target loop is closed, signal not delivered")

    async def wait_for_signal(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for a signal from the background thread.

        This method should be called from the async context (MCP server).

        Args:
            timeout: Maximum time to wait in seconds, or None to wait forever

        Returns:
            True if signaled, False if timeout occurred
        """
        if self._async_event is None:
            raise RuntimeError("Target loop not set. Call set_target_loop() first.")

        try:
            if timeout is not None:
                await asyncio.wait_for(self._async_event.wait(), timeout=timeout)
            else:
                await self._async_event.wait()

            # Clear events after receiving signal
            self._async_event.clear()
            self._thread_event.clear()
            logger.debug("ThreadToAsyncBridge: signal received and cleared")
            return True

        except asyncio.TimeoutError:
            logger.debug(f"ThreadToAsyncBridge: wait timed out after {timeout}s")
            return False

    def is_signaled(self) -> bool:
        """
        Check if the bridge is currently signaled.

        This is a non-blocking, thread-safe check.

        Returns:
            True if signaled, False otherwise
        """
        return self._thread_event.is_set()

    def clear(self) -> None:
        """
        Clear the signal state.

        This is useful if you want to reset the bridge without waiting.
        """
        with self._lock:
            self._thread_event.clear()
            if self._async_event:
                # Schedule clear on the target loop if possible
                if self._target_loop and self._target_loop.is_running():
                    try:
                        self._target_loop.call_soon_threadsafe(self._async_event.clear)
                    except RuntimeError:
                        pass


# Global singleton for the enrichment bridge
_enrichment_bridge: Optional[ThreadToAsyncBridge] = None
_bridge_init_lock = threading.Lock()


def get_enrichment_bridge() -> ThreadToAsyncBridge:
    """
    Get the global enrichment bridge instance.

    This singleton is used to communicate between the catalog worker
    and the enrichment system.

    Returns:
        The global ThreadToAsyncBridge instance
    """
    global _enrichment_bridge
    if _enrichment_bridge is None:
        with _bridge_init_lock:
            if _enrichment_bridge is None:
                _enrichment_bridge = ThreadToAsyncBridge()
                logger.info("Created global enrichment bridge")
    return _enrichment_bridge
