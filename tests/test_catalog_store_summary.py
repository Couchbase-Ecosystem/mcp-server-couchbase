"""Unit tests for persisted bucket summary line in catalog store."""

from __future__ import annotations

from catalog.store import cluster_state
from catalog.store.store import Store


def test_bucket_summary_line_is_persisted(tmp_path) -> None:
    """Bucket summary line should survive save/load cycles."""
    state_file = tmp_path / "catalog_state_test.json"
    summary_line = "orders: scopes=2 (_default, sales), collections=3 (orders, items, users)"

    store = Store(state_file=state_file, bucket_name="orders")
    store.set_bucket_summary_line(summary_line)

    reloaded = Store(state_file=state_file, bucket_name="orders")
    assert reloaded.get_bucket_summary_line() == summary_line


def test_build_state_file_path_uses_cluster_folder(tmp_path, monkeypatch) -> None:
    """State file path should use ~/.couchbase_mcp/<cluster>/catalog_state_<bucket>.json."""
    monkeypatch.setattr(cluster_state, "CATALOG_STATE_DIR", tmp_path)
    state_file = cluster_state.build_state_file_path("localhost", "orders")

    assert state_file == tmp_path / "localhost" / "catalog_state_orders.json"
