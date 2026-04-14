"""
Store module for catalog data with thread-safe operations.

This module provides per-bucket stores for:
- Bucket-scoped schema information
- Bucket-scoped enriched prompts from LLM sampling
- Bucket-scoped schema hash tracking
"""

import hashlib
import json
import logging
import shutil
from pathlib import Path
from threading import Lock
from typing import Any

from catalog.store.cluster_state import (
    CATALOG_STATE_DIR,
    build_cluster_state_dir,
    build_state_file_path,
    derive_bucket_key,
    derive_cluster_key,
    extract_bucket_key_from_state_file,
)
from utils.config import get_settings

logger = logging.getLogger(__name__)
_DEFAULT_CLUSTER_KEY = "default"
_DEFAULT_BUCKET_KEY = "default_bucket"


class Store:
    """Thread-safe bucket-scoped store for catalog data and enrichment prompts."""

    def __init__(self, state_file: Path, bucket_name: str | None = None):
        """Initialize the store with empty data structures."""
        self.database_info: dict[str, Any] = {}
        self.prompt: str = ""
        self.schema_hash: str = ""
        self.bucket_name: str = bucket_name or ""
        self.bucket_summary_line: str = ""
        self._lock: Lock = Lock()
        self._state_file: Path = state_file

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

    def set_bucket_name(self, bucket_name: str) -> None:
        """Set and persist the bucket name represented by this store."""
        with self._lock:
            self.bucket_name = bucket_name
            self._save_state()

    def get_bucket_name(self) -> str:
        """Get the bucket name represented by this store."""
        with self._lock:
            return self.bucket_name

    def set_bucket_summary_line(self, summary_line: str) -> None:
        """Set and persist deterministic one-line summary for this bucket."""
        with self._lock:
            self.bucket_summary_line = summary_line
            self._save_state()

    def get_bucket_summary_line(self) -> str:
        """Get deterministic one-line summary for this bucket."""
        with self._lock:
            return self.bucket_summary_line

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
            "bucket_name": self.bucket_name,
            "bucket_summary_line": self.bucket_summary_line,
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
            self.bucket_name = str(state_dict.get("bucket_name", self.bucket_name))
            self.bucket_summary_line = str(
                state_dict.get("bucket_summary_line", self.bucket_summary_line)
            )
        except Exception as e:
            logger.error(f"Failed to import state from dictionary: {e}")
            raise

def _resolve_cluster_key(cluster_key: str | None = None) -> str:
    """Resolve the active cluster key from explicit input or runtime settings."""
    if cluster_key:
        return cluster_key
    settings = get_settings()
    connection_string = settings.get("connection_string")
    if not connection_string:
        return _DEFAULT_CLUSTER_KEY
    return derive_cluster_key(connection_string)


def _resolve_catalog_state_file(cluster_key: str, bucket_key: str) -> Path:
    """Resolve the state file path for the active cluster + bucket key."""
    state_file = build_state_file_path(cluster_key, bucket_key)
    legacy_flat_file = CATALOG_STATE_DIR / f"catalog_state_{cluster_key}_{bucket_key}.json"

    state_file.parent.mkdir(parents=True, exist_ok=True)

    # Migrate from old flat-file layout (~/.couchbase_mcp/catalog_state_<cluster>_<bucket>.json)
    # to new folder layout (~/.couchbase_mcp/<cluster>/catalog_state_<bucket>.json).
    if not state_file.exists() and legacy_flat_file.exists():
        try:
            shutil.move(str(legacy_flat_file), str(state_file))
            logger.info(
                "Migrated catalog state file from %s to %s",
                legacy_flat_file,
                state_file,
            )
        except OSError as exc:
            logger.warning(
                "Failed to migrate legacy catalog state file %s to %s: %s",
                legacy_flat_file,
                state_file,
                exc,
            )

    return state_file


def _resolve_bucket_key(bucket_name: str | None = None) -> str:
    """Resolve bucket key from bucket name."""
    if not bucket_name:
        return _DEFAULT_BUCKET_KEY
    return derive_bucket_key(bucket_name)


def _extract_bucket_name_from_database_info(database_info: dict[str, Any]) -> str:
    """Best-effort extraction of bucket name from bucket-scoped database_info."""
    buckets = database_info.get("buckets", {})
    if not isinstance(buckets, dict) or len(buckets) != 1:
        return ""
    return next(iter(buckets))


# Global store instances (singleton per cluster + bucket key)
_catalog_stores: dict[tuple[str, str], Store] = {}
_store_init_lock = Lock()


def get_catalog_store(
    bucket_name: str | None = None, cluster_key: str | None = None
) -> Store:
    """Get a bucket-scoped catalog store instance (thread-safe singleton)."""
    resolved_cluster_key = _resolve_cluster_key(cluster_key)
    resolved_bucket_key = _resolve_bucket_key(bucket_name)
    cache_key = (resolved_cluster_key, resolved_bucket_key)

    if cache_key not in _catalog_stores:
        with _store_init_lock:
            if cache_key not in _catalog_stores:
                state_file = _resolve_catalog_state_file(
                    resolved_cluster_key, resolved_bucket_key
                )
                _catalog_stores[cache_key] = Store(
                    state_file=state_file, bucket_name=bucket_name
                )

    store = _catalog_stores[cache_key]
    if bucket_name and store.get_bucket_name() != bucket_name:
        store.set_bucket_name(bucket_name)
    return store


def get_all_catalog_stores(cluster_key: str | None = None) -> dict[str, Store]:
    """Return all bucket stores discovered for the active cluster."""
    resolved_cluster_key = _resolve_cluster_key(cluster_key)
    cluster_dir = build_cluster_state_dir(resolved_cluster_key)
    cluster_dir.mkdir(parents=True, exist_ok=True)
    pattern = "catalog_state_*.json"

    discovered_files = sorted(cluster_dir.glob(pattern))
    for state_file in discovered_files:
        bucket_key = extract_bucket_key_from_state_file(state_file, resolved_cluster_key)
        if not bucket_key:
            continue
        cache_key = (resolved_cluster_key, bucket_key)
        if cache_key not in _catalog_stores:
            with _store_init_lock:
                if cache_key not in _catalog_stores:
                    _catalog_stores[cache_key] = Store(state_file=state_file)

    stores_by_bucket: dict[str, Store] = {}
    for (cluster, bucket_key), store in _catalog_stores.items():
        if cluster != resolved_cluster_key:
            continue

        bucket_name = store.get_bucket_name()
        if not bucket_name:
            bucket_name = _extract_bucket_name_from_database_info(store.get_database_info())
        if not bucket_name:
            bucket_name = bucket_key

        stores_by_bucket[bucket_name] = store

    return stores_by_bucket


def get_all_bucket_database_info(cluster_key: str | None = None) -> dict[str, Any]:
    """Return combined database_info payload reconstructed from bucket stores."""
    merged: dict[str, Any] = {"buckets": {}}
    for bucket_name, store in get_all_catalog_stores(cluster_key).items():
        info = store.get_database_info()
        buckets = info.get("buckets", {})
        if isinstance(buckets, dict) and bucket_name in buckets:
            merged["buckets"][bucket_name] = buckets[bucket_name]
            continue
        if isinstance(buckets, dict) and len(buckets) == 1:
            source_bucket_name = next(iter(buckets))
            merged["buckets"][source_bucket_name] = buckets[source_bucket_name]
    return merged
