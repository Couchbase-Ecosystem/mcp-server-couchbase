"""
Store module for catalog data with thread-safe operations.

This module provides a global store for:
- Database schema information (buckets, scopes, collections)
- Enriched prompts from LLM sampling
- Change detection flags
"""

from threading import Lock
from typing import Any, Optional


class Store:
    """Thread-safe store for catalog data and enrichment prompts."""

    def __init__(self):
        """Initialize the store with empty data structures."""
        self.database_info: dict[str, Any] = {}
        self.prompt: str = ""
        self.schema_hash: str = ""
        self.needs_enrichment: bool = False
        self._lock: Lock = Lock()

    def add_database_info(self, database_info: dict[str, Any]) -> None:
        """
        Add or update database information in the store.
        
        Args:
            database_info: Dictionary containing buckets, scopes, and collections info
        """
        with self._lock:
            self.database_info = database_info.copy()

    def get_database_info(self) -> dict[str, Any]:
        """
        Get database information from the store.
        
        Returns:
            Copy of the database information dictionary
        """
        with self._lock:
            return self.database_info.copy() if self.database_info else {}

    def add_prompt(self, prompt: str) -> None:
        """
        Add or update the enriched prompt in the store.
        
        Args:
            prompt: The enriched prompt string from LLM sampling
        """
        with self._lock:
            self.prompt = prompt

    def get_prompt(self) -> str:
        """
        Get the enriched prompt from the store.
        
        Returns:
            The enriched prompt string
        """
        with self._lock:
            return self.prompt

    def set_schema_hash(self, schema_hash: str) -> None:
        """
        Set the hash of the current schema for change detection.
        
        Args:
            schema_hash: Hash string of the schema
        """
        with self._lock:
            self.schema_hash = schema_hash

    def get_schema_hash(self) -> str:
        """
        Get the current schema hash.
        
        Returns:
            The schema hash string
        """
        with self._lock:
            return self.schema_hash

    def set_needs_enrichment(self, needs: bool) -> None:
        """
        Set whether the catalog needs LLM enrichment.
        
        Args:
            needs: True if enrichment is needed, False otherwise
        """
        with self._lock:
            self.needs_enrichment = needs

    def get_needs_enrichment(self) -> bool:
        """
        Check if the catalog needs LLM enrichment.
        
        Returns:
            True if enrichment is needed, False otherwise
        """
        with self._lock:
            return self.needs_enrichment

    def clear_needs_enrichment(self) -> None:
        """Clear the enrichment flag after processing."""
        with self._lock:
            self.needs_enrichment = False

# Global store instance
_catalog_store: Optional[Store] = None
_lock = Lock()   

def get_catalog_store() -> Store:
    """Get the global catalog store instance."""
    global _catalog_store
    global _lock
    with _lock:
        if _catalog_store is None:
            _catalog_store = Store()
        return _catalog_store            