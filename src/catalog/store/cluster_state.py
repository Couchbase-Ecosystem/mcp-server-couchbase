"""Helpers for deriving per-cluster catalog state file paths."""

import hashlib
import re
from pathlib import Path

CATALOG_STATE_DIR = Path.home() / ".couchbase_mcp"
LEGACY_CATALOG_STATE_FILE = CATALOG_STATE_DIR / "catalog_state.json"
_MAX_CLUSTER_KEY_LENGTH = 80


def _extract_connection_hosts(connection_string: str) -> list[str]:
    """Extract host entries from the main connection-string segment."""
    target = connection_string.strip()
    if not target:
        return []

    if "://" in target:
        target = target.split("://", 1)[1]

    target = target.split("?", 1)[0]
    target = target.split("/", 1)[0]

    hosts = [host.strip().lower() for host in target.split(",") if host.strip()]
    return sorted(hosts)


def derive_cluster_key(connection_string: str | None) -> str:
    """Build a stable, filename-safe cluster key from the connection string."""
    if not connection_string:
        return "default"

    hosts = _extract_connection_hosts(connection_string)
    normalized = ",".join(hosts) if hosts else connection_string.strip().lower()
    normalized = normalized or "default"

    safe = re.sub(r"[^a-z0-9._-]+", "_", normalized).strip("._-")
    if not safe:
        safe = "cluster"

    if len(safe) > _MAX_CLUSTER_KEY_LENGTH:
        digest = hashlib.sha256(normalized.encode()).hexdigest()[:16]
        safe = f"{safe[:40]}_{digest}"

    return safe


def build_state_file_path(cluster_key: str) -> Path:
    """Return the state file path for the given cluster key."""
    return CATALOG_STATE_DIR / f"catalog_state_{cluster_key}.json"
