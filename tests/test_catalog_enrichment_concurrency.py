"""Unit tests for catalog enrichment bucket concurrency and failure isolation."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from catalog.enrichment import catalog_enrichment


class _FakeStore:
    def __init__(self, database_info: dict[str, Any], schema_hash: str = "old") -> None:
        self._database_info = database_info
        self._schema_hash = schema_hash
        self.prompt = ""

    def get_database_info(self) -> dict[str, Any]:
        return self._database_info

    def get_schema_hash(self) -> str:
        return self._schema_hash

    def add_prompt(self, prompt: str) -> None:
        self.prompt = prompt

    def set_schema_hash(self, schema_hash: str) -> None:
        self._schema_hash = schema_hash


@pytest.mark.asyncio
async def test_enrichment_respects_bucket_concurrency_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enrichment should honor configured per-bucket concurrency."""
    stores = {
        "b1": _FakeStore({"buckets": {"b1": {}}}),
        "b2": _FakeStore({"buckets": {"b2": {}}}),
        "b3": _FakeStore({"buckets": {"b3": {}}}),
        "b4": _FakeStore({"buckets": {"b4": {}}}),
    }
    counters = {"current": 0, "max": 0}

    monkeypatch.setattr(catalog_enrichment, "get_all_catalog_stores", lambda: stores)
    monkeypatch.setattr(catalog_enrichment, "compute_catalog_schema_hash", lambda _: "new")
    monkeypatch.setattr(
        catalog_enrichment,
        "get_settings",
        lambda: {"enrichment_bucket_concurrency": 2},
    )

    async def _fake_request(_: object, __: dict[str, Any]) -> str:
        counters["current"] += 1
        counters["max"] = max(counters["max"], counters["current"])
        await asyncio.sleep(0.01)
        counters["current"] -= 1
        return "enriched"

    async def _fake_append_verified_relationships(**kwargs: Any) -> str:
        return kwargs["enriched_prompt"]

    monkeypatch.setattr(catalog_enrichment, "_request_llm_enrichment", _fake_request)
    monkeypatch.setattr(
        catalog_enrichment,
        "append_verified_relationships_to_prompt",
        _fake_append_verified_relationships,
    )

    await catalog_enrichment._check_and_enrich_catalog(session=object())  # type: ignore[arg-type]

    assert counters["max"] <= 2
    assert all(store.prompt == "enriched" for store in stores.values())


@pytest.mark.asyncio
async def test_enrichment_isolates_bucket_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failure in one bucket should not block enrichment in another."""
    stores = {
        "good": _FakeStore({"buckets": {"good": {}}}),
        "bad": _FakeStore({"buckets": {"bad": {}}}),
    }

    monkeypatch.setattr(catalog_enrichment, "get_all_catalog_stores", lambda: stores)
    monkeypatch.setattr(catalog_enrichment, "compute_catalog_schema_hash", lambda _: "new")
    monkeypatch.setattr(
        catalog_enrichment,
        "get_settings",
        lambda: {"enrichment_bucket_concurrency": 2},
    )

    async def _fake_request(_: object, database_info: dict[str, Any]) -> str:
        if "bad" in database_info.get("buckets", {}):
            raise RuntimeError("sampling failed")
        return "good-prompt"

    async def _fake_append_verified_relationships(**kwargs: Any) -> str:
        return kwargs["enriched_prompt"]

    monkeypatch.setattr(catalog_enrichment, "_request_llm_enrichment", _fake_request)
    monkeypatch.setattr(
        catalog_enrichment,
        "append_verified_relationships_to_prompt",
        _fake_append_verified_relationships,
    )

    await catalog_enrichment._check_and_enrich_catalog(session=object())  # type: ignore[arg-type]

    assert stores["good"].prompt == "good-prompt"
    assert stores["good"].get_schema_hash() == "new"
    assert stores["bad"].prompt == ""
    assert stores["bad"].get_schema_hash() == "old"
