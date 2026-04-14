"""Helpers for deriving per-cluster catalog state file paths."""

import hashlib
import re
from pathlib import Path

CATALOG_STATE_DIR = Path.home() / ".couchbase_mcp"
_MAX_CLUSTER_KEY_LENGTH = 80
_MAX_BUCKET_KEY_LENGTH = 80


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


def _sanitize_key(value: str, *, max_length: int, fallback: str) -> str:
    """Sanitize a string into a filename-safe key."""
    normalized = value.strip().lower()
    safe = re.sub(r"[^a-z0-9._-]+", "_", normalized).strip("._-")
    if not safe:
        safe = fallback

    if len(safe) > max_length:
        digest = hashlib.sha256(normalized.encode()).hexdigest()[:16]
        safe = f"{safe[:40]}_{digest}"
    return safe


def derive_bucket_key(bucket_name: str | None) -> str:
    """Build a stable, filename-safe bucket key."""
    if not bucket_name:
        return "default_bucket"
    return _sanitize_key(
        bucket_name,
        max_length=_MAX_BUCKET_KEY_LENGTH,
        fallback="bucket",
    )


def build_cluster_state_dir(cluster_key: str) -> Path:
    """Return cluster-scoped state directory path."""
    return CATALOG_STATE_DIR / cluster_key


def build_state_file_path(cluster_key: str, bucket_key: str) -> Path:
    """Return bucket-scoped state file path inside the cluster directory."""
    return build_cluster_state_dir(cluster_key) / f"catalog_state_{bucket_key}.json"


def extract_bucket_key_from_state_file(path: Path, cluster_key: str) -> str | None:
    """Extract bucket key from file name within the cluster directory."""
    cluster_dir = build_cluster_state_dir(cluster_key)
    if path.parent != cluster_dir:
        return None

    prefix = "catalog_state_"
    name = path.stem
    if not name.startswith(prefix):
        return None
    bucket_key = name[len(prefix) :]
    return bucket_key or None
