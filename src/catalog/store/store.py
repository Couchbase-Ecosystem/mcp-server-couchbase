"""
Store module for catalog data with thread-safe operations.

This module provides a global store for:
- Database schema information (buckets, scopes, collections)
- Enriched prompts from LLM sampling
- Change detection flags
"""

import json
import logging
from pathlib import Path
from threading import Lock
from typing import Any, Optional

logger = logging.getLogger(__name__)


class Store:
    """Thread-safe store for catalog data and enrichment prompts."""
    
    # Default state file location
    STATE_FILE = Path.home() / ".couchbase_mcp" / "catalog_state.json"

    def __init__(self):
        """Initialize the store with empty data structures."""
        self.database_info: dict[str, Any] = {}
        self.prompt: str = ""
        self.schema_hash: str = ""
        self.needs_enrichment: bool = False
        self._lock: Lock = Lock()
        self._state_file: Path = self.STATE_FILE
        
        # Create state directory if it doesn't exist
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load state from file if it exists
        self._load_state()

    def _save_state(self) -> None:
        """
        Save current state to file using to_dict() for encoding.
        
        Note: This method assumes the lock is already held by the caller.
        """
        try:
            # Get state dict without acquiring lock (already held)
            state_dict = self.to_dict()
            # Encode to JSON
            json_string = json.dumps(state_dict, indent=2, ensure_ascii=False)
            # Write to file
            with open(self._state_file, "w", encoding="utf-8") as f:
                f.write(json_string)
            logger.debug(f"State saved to {self._state_file}")
        except Exception as e:
            logger.error(f"Failed to save state to {self._state_file}: {e}")

    def _load_state(self) -> None:
        """
        Load state from file if it exists, or create an empty state file.
        Uses from_dict() logic for parsing.
        
        Note: This method is called during __init__ before any threads access the store,
        so it doesn't need to acquire the lock.
        """
        try:
            if self._state_file.exists():
                # Read JSON from file
                with open(self._state_file, encoding="utf-8") as f:
                    json_string = f.read()
                
                # Parse JSON to dictionary and set state
                self.from_dict(json.loads(json_string))
                logger.info(f"State loaded from {self._state_file}")
            else:
                # Create initial empty state file
                logger.debug(f"No state file found at {self._state_file}, creating new one")
                self._create_initial_state()
        except Exception as e:
            logger.error(f"Failed to load state from {self._state_file}: {e}")
            # Continue with empty state on error

    def _create_initial_state(self) -> None:
        """
        Create an initial empty state file using to_dict() logic for encoding.
        
        Note: This method is called during initialization if no state file exists.
        """
        try:
            initial_state = self.to_dict()
            with open(self._state_file, "w", encoding="utf-8") as f:
                f.write(json.dumps(initial_state, indent=2, ensure_ascii=False))
            logger.info(f"Created initial state file at {self._state_file}")
        except Exception as e:
            logger.error(f"Failed to create initial state file at {self._state_file}: {e}")

    def add_database_info(self, database_info: dict[str, Any]) -> None:
        """
        Add or update database information in the store.
        Persists the change to disk.
        
        Args:
            database_info: Dictionary containing buckets, scopes, and collections info
        """
        with self._lock:
            self.database_info = database_info.copy()
            self._save_state()

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
        Persists the change to disk.
        
        Args:
            schema_hash: Hash string of the schema
        """
        with self._lock:
            self.schema_hash = schema_hash
            self._save_state()

    def get_schema_hash(self) -> str:
        """
        Get the current schema hash.
        
        Returns:
            The schema hash string
        """
        with self._lock:
            return self.schema_hash

    def set_needs_enrichment(self, needs_enrichment: bool) -> None:
        """
        Set the flag indicating if enrichment is needed.
        Persists the change to disk.
        
        Args:
            needs_enrichment: Boolean flag for enrichment status
        """
        with self._lock:
            self.needs_enrichment = needs_enrichment
            self._save_state()

    def get_needs_enrichment(self) -> bool:
        """
        Get the enrichment needed flag.
        
        Returns:
            Boolean indicating if enrichment is needed
        """
        with self._lock:
            return self.needs_enrichment

    def to_dict(self) -> dict[str, Any]:
        """
        Export the entire store state as a dictionary.
        
        This is the primary method for encoding store state. All file I/O operations
        use this method internally for serialization.
        
        Returns:
            Dictionary containing the store state
        """
        return {
            "database_info": self.database_info,
            "prompt": self.prompt,
            "schema_hash": self.schema_hash,
            "needs_enrichment": self.needs_enrichment,
        }

    def from_dict(self, state_dict: dict[str, Any]) -> None:
        """
        Import store state from a dictionary.
        
        This is the primary method for decoding store state. All file I/O operations
        use this method internally for deserialization.
        
        Note: Called during initialization before threads access the store.
        
        Args:
            state_dict: Dictionary containing store state with required fields:
                       - database_info: Database schema information
                       - prompt: Enriched prompt string
                       - schema_hash: Hash of current schema
                       - needs_enrichment: Boolean flag for enrichment status
            
        Raises:
            ValueError: If the dictionary is missing required fields
        """
        try:
            required_fields = {"database_info", "prompt", "schema_hash", "needs_enrichment"}
            missing_fields = required_fields - set(state_dict.keys())
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")
            
            self.database_info = state_dict["database_info"]
            self.prompt = state_dict["prompt"]
            self.schema_hash = state_dict["schema_hash"]
            self.needs_enrichment = state_dict["needs_enrichment"]
        except Exception as e:
            logger.error(f"Failed to import state from dictionary: {e}")
            raise

# Global store instance (singleton pattern)
_catalog_store: Optional[Store] = None
_store_init_lock = Lock()


def get_catalog_store() -> Store:
    """Get the global catalog store instance (thread-safe singleton)."""
    global _catalog_store
    if _catalog_store is None:
        with _store_init_lock:
            if _catalog_store is None:
                _catalog_store = Store()
    return _catalog_store