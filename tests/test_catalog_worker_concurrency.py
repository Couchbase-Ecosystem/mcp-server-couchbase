"""Unit tests for catalog worker bucket concurrency and failure isolation."""

from __future__ import annotations

import asyncio

import pytest

from catalog import worker


class _FakeBucketInfo:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeBucketManager:
    def __init__(self, bucket_names: list[str]) -> None:
        self._bucket_names = bucket_names

    async def get_all_buckets(self) -> list[_FakeBucketInfo]:
        return [_FakeBucketInfo(name) for name in self._bucket_names]


class _FakeCluster:
    def __init__(self, bucket_names: list[str]) -> None:
        self._bucket_manager = _FakeBucketManager(bucket_names)

    def buckets(self) -> _FakeBucketManager:
        return self._bucket_manager


class _FakeCollection:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeScope:
    def __init__(self, name: str, collections: list[_FakeCollection]) -> None:
        self.name = name
        self.collections = collections


class _FakeCollectionManager:
    def __init__(
        self,
        scope_name: str,
        collection_names: list[str],
        counters: dict[str, int],
    ) -> None:
        self._scope_name = scope_name
        self._collection_names = collection_names
        self._counters = counters

    async def get_all_scopes(self) -> list[_FakeScope]:
        self._counters["current"] += 1
        self._counters["max"] = max(self._counters["max"], self._counters["current"])
        await asyncio.sleep(0.01)
        self._counters["current"] -= 1
        collections = [_FakeCollection(name) for name in self._collection_names]
        return [_FakeScope(self._scope_name, collections)]


class _FakeBucket:
    def __init__(
        self,
        scope_name: str,
        collection_names: list[str],
        counters: dict[str, int],
    ) -> None:
        self._scope_name = scope_name
        self._collection_names = collection_names
        self._counters = counters

    def collections(self) -> _FakeCollectionManager:
        return _FakeCollectionManager(
            self._scope_name, self._collection_names, self._counters
        )


class _FakeSchemaCollection:
    def to_dict(self) -> list[dict]:
        return []

    def merge(self, _: object) -> None:
        return None

    def __len__(self) -> int:
        return 0


class _AsyncRows:
    def __init__(self, rows: list[dict] | list[int]) -> None:
        self._rows = rows
        self._index = 0

    def __aiter__(self) -> _AsyncRows:
        return self

    async def __anext__(self) -> dict | int:
        if self._index >= len(self._rows):
            raise StopAsyncIteration
        row = self._rows[self._index]
        self._index += 1
        return row


@pytest.mark.asyncio
async def test_collect_buckets_respects_worker_concurrency_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker should honor the configured per-bucket concurrency bound."""
    counters = {"current": 0, "max": 0}

    def _fake_connect_to_bucket(_: object, __: str) -> _FakeBucket:
        return _FakeBucket("_default", ["c1"], counters)

    monkeypatch.setattr(worker, "connect_to_bucket_async", _fake_connect_to_bucket)
    monkeypatch.setattr(
        worker,
        "parse_infer_output",
        lambda _: _FakeSchemaCollection(),
    )
    async def _fake_get_indexes(*_: object, **__: object) -> list[dict]:
        return []

    async def _fake_infer(*_: object, **__: object) -> list[dict]:
        return []

    monkeypatch.setattr(worker, "_get_index_definitions", _fake_get_indexes)
    monkeypatch.setattr(worker, "_infer_collection_schema", _fake_infer)
    monkeypatch.setattr(worker, "get_settings", lambda: {"worker_bucket_concurrency": 2})

    database_info = await worker._collect_buckets_scopes_collections(
        _FakeCluster(["b1", "b2", "b3", "b4"]),
        {},
    )

    assert len(database_info["buckets"]) == 4
    assert counters["max"] <= 2


@pytest.mark.asyncio
async def test_collect_buckets_isolates_bucket_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker should continue collecting healthy buckets if one bucket fails."""
    counters = {"current": 0, "max": 0}

    def _fake_connect_to_bucket(_: object, bucket_name: str) -> _FakeBucket:
        if bucket_name == "bad":
            raise RuntimeError("bucket failure")
        return _FakeBucket("_default", ["c1"], counters)

    monkeypatch.setattr(worker, "connect_to_bucket_async", _fake_connect_to_bucket)
    monkeypatch.setattr(
        worker,
        "parse_infer_output",
        lambda _: _FakeSchemaCollection(),
    )
    async def _fake_get_indexes(*_: object, **__: object) -> list[dict]:
        return []

    async def _fake_infer(*_: object, **__: object) -> list[dict]:
        return []

    monkeypatch.setattr(worker, "_get_index_definitions", _fake_get_indexes)
    monkeypatch.setattr(worker, "_infer_collection_schema", _fake_infer)
    monkeypatch.setattr(worker, "get_settings", lambda: {"worker_bucket_concurrency": 3})

    database_info = await worker._collect_buckets_scopes_collections(
        _FakeCluster(["good", "bad"]),
        {},
    )

    assert "good" in database_info["buckets"]
    assert "bad" not in database_info["buckets"]


@pytest.mark.asyncio
async def test_infer_collection_schema_retries_scope_not_found_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Schema inference should retry transient 12021 errors."""
    query_calls = {"count": 0}

    async def _fake_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(worker.asyncio, "sleep", _fake_sleep)

    class _FakeScopeForRetry:
        def query(self, query_text: str) -> _AsyncRows:
            query_calls["count"] += 1
            if query_calls["count"] < 3:
                raise Exception(
                    "ScopeNotFoundException code=12021 Scope not found in CB datastore"
                )
            if query_text.startswith("SELECT RAW 1"):
                return _AsyncRows([1])
            return _AsyncRows([[{"field": {"type": "string"}}]])

    class _FakeBucketForRetry:
        def scope(self, name: str) -> _FakeScopeForRetry:
            _ = name
            return _FakeScopeForRetry()

    result = await worker._infer_collection_schema(
        _FakeBucketForRetry(),  # type: ignore[arg-type]
        scope_name="s1",
        collection_name="c1",
    )

    assert result == [{"field": {"type": "string"}}]
    assert query_calls["count"] == 4


@pytest.mark.asyncio
async def test_infer_collection_schema_returns_empty_after_retry_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Schema inference should stop retrying and return empty on repeated 5000 errors."""
    query_calls = {"count": 0}

    async def _fake_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(worker.asyncio, "sleep", _fake_sleep)

    class _AlwaysFailScope:
        def query(self, _: str) -> _AsyncRows:
            query_calls["count"] += 1
            raise Exception("InternalServerFailureException code=5000 Index not ready")

    class _AlwaysFailBucket:
        def scope(self, name: str) -> _AlwaysFailScope:
            _ = name
            return _AlwaysFailScope()

    result = await worker._infer_collection_schema(
        _AlwaysFailBucket(),  # type: ignore[arg-type]
        scope_name="s1",
        collection_name="c1",
    )

    assert result == []
    assert query_calls["count"] == 3
