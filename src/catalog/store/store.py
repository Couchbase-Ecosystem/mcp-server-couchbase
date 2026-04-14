"""
Store module for catalog data with thread-safe operations.

This module provides a global store for:
- Database schema information (buckets, scopes, collections)
- Enriched prompts from LLM sampling
- Change detection flags
"""

import hashlib
import json
import logging
import shutil
from pathlib import Path
from threading import Lock
from typing import Any

from catalog.store.cluster_state import (
    LEGACY_CATALOG_STATE_FILE,
    build_state_file_path,
    derive_cluster_key,
)
from utils.config import get_settings

logger = logging.getLogger(__name__)
_LEGACY_STORE_KEY = "__legacy__"


class Store:
    """Thread-safe store for catalog data and enrichment prompts."""

    # Default state file location
    STATE_FILE = LEGACY_CATALOG_STATE_FILE

    def __init__(self, state_file: Path | None = None):
        """Initialize the store with empty data structures."""
        self.database_info: dict[str, Any] = {}
        self.prompt: str = ""
        self.schema_hash: str = ""
        self._lock: Lock = Lock()
        self._state_file: Path = state_file or self.STATE_FILE

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
        Deprecated no-op: enrichment necessity is derived from schema hash comparison.

        Args:
            needs_enrichment: ignored
        """
        _ = needs_enrichment

    def get_needs_enrichment(self) -> bool:
        """
        Return True when current database_info hash differs from stored schema_hash.

        Returns:
            Boolean indicating if enrichment is needed
        """
        with self._lock:
            if not self.database_info:
                return False
            current_hash = hashlib.sha256(
                json.dumps(self.database_info, sort_keys=True).encode()
            ).hexdigest()
            return current_hash != self.schema_hash

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
                       - schema_hash: Hash of database_info that produced current prompt

        Raises:
            ValueError: If the dictionary is missing required fields
        """
        try:
            required_fields = {"database_info", "prompt", "schema_hash"}
            missing_fields = required_fields - set(state_dict.keys())
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")

            self.database_info = state_dict["database_info"]
            self.prompt = state_dict["prompt"]
            self.schema_hash = state_dict["schema_hash"]
        except Exception as e:
            logger.error(f"Failed to import state from dictionary: {e}")
            raise

def _resolve_catalog_state_file(cluster_key: str | None = None) -> Path:
    """Resolve the state file path for the active cluster."""
    resolved_cluster_key = cluster_key
    if resolved_cluster_key is None:
        settings = get_settings()
        connection_string = settings.get("connection_string")
        resolved_cluster_key = (
            derive_cluster_key(connection_string)
            if connection_string
            else _LEGACY_STORE_KEY
        )

    if resolved_cluster_key == _LEGACY_STORE_KEY:
        return LEGACY_CATALOG_STATE_FILE

    state_file = build_state_file_path(resolved_cluster_key)
    state_file.parent.mkdir(parents=True, exist_ok=True)

    if (
        resolved_cluster_key != "default"
        and not state_file.exists()
        and LEGACY_CATALOG_STATE_FILE.exists()
    ):
        try:
            shutil.copy2(LEGACY_CATALOG_STATE_FILE, state_file)
            logger.info(
                "Copied legacy catalog state to cluster-scoped file: %s",
                state_file,
            )
        except OSError as exc:
            logger.warning(
                "Failed to copy legacy catalog state file to %s: %s",
                state_file,
                exc,
            )

    return state_file


# Global store instances (singleton per cluster key)
_catalog_stores: dict[str, Store] = {}
_store_init_lock = Lock()


def get_catalog_store(cluster_key: str | None = None) -> Store:
    """Get the cluster-scoped catalog store instance (thread-safe singleton)."""
    resolved_cluster_key = cluster_key
    if resolved_cluster_key is None:
        settings = get_settings()
        connection_string = settings.get("connection_string")
        resolved_cluster_key = (
            derive_cluster_key(connection_string)
            if connection_string
            else _LEGACY_STORE_KEY
        )

    if resolved_cluster_key not in _catalog_stores:
        with _store_init_lock:
            if resolved_cluster_key not in _catalog_stores:
                state_file = _resolve_catalog_state_file(resolved_cluster_key)
                _catalog_stores[resolved_cluster_key] = Store(state_file=state_file)
    return _catalog_stores[resolved_cluster_key]
