"""Sampling-only behavior tests for relationship verifier tasks."""

from __future__ import annotations

from typing import Any

from catalog.enrichment.relationship_verifier.relationship_verifier import (
    RelationshipVerifier,
)
from catalog.enrichment.relationship_verifier.tasks.column_exists_task import (
    ColumnExistsTask,
)
from catalog.enrichment.relationship_verifier.tasks.column_not_null_task import (
    ColumnNotNullTask,
)
from catalog.enrichment.relationship_verifier.tasks.column_not_object_task import (
    ColumnNotObjectTask,
)
from catalog.enrichment.relationship_verifier.tasks.column_type_compatibility_task import (
    ColumnTypeCompatibilityTask,
)
from catalog.enrichment.relationship_verifier.tasks.column_uniqueness_task import (
    ColumnUniquenessTask,
)
from catalog.enrichment.relationship_verifier.tasks.meta_id_reference_exists_task import (
    MetaIdReferenceExistsTask,
)
from catalog.enrichment.relationship_verifier.tasks.value_set_inclusion_task import (
    ValueSetInclusionTask,
)
from utils.config import set_settings


class FakeCB:
    """Minimal fake Couchbase helper for sampling-only task tests."""

    def __init__(
        self,
        sample_data: dict[tuple[str, str, str], list[tuple[str, Any]]],
        existing_docs: set[tuple[str, str, str, str]] | None = None,
    ) -> None:
        self.sample_data = sample_data
        self.existing_docs = existing_docs or set()
        self.document_exists_calls: list[tuple[str, str, str, str]] = []

    def sample_collection_documents(
        self,
        *,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        limit: int,
        seed: int | None = None,
    ) -> list[tuple[str, Any]]:
        _ = seed
        rows = self.sample_data.get((bucket_name, scope_name, collection_name), [])
        return rows[:limit]

    def scan_collection_documents(self, **_: Any) -> list[tuple[str, Any]]:
        raise AssertionError("sampling-only tasks must not call scan_collection_documents")

    def run_query(self, _: str, timeout_seconds: int = 30) -> list[Any]:
        _ = timeout_seconds
        raise AssertionError("sampling-only tasks must not call run_query")

    def document_exists(
        self,
        *,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        document_id: str,
        timeout_seconds: int = 30,
    ) -> bool:
        _ = timeout_seconds
        key = (bucket_name, scope_name, collection_name, document_id)
        self.document_exists_calls.append(key)
        return key in self.existing_docs


def _run_task(task: Any, cb: FakeCB, keyspace_map: dict[str, str]) -> tuple[Any, str]:
    output, mode, error, _ = task.run(
        cb=cb,
        bucket_name="b1",
        keyspace_map=keyspace_map,
        index_map={},
        max_unindexed_scan_rows=1000,
        value_set_timeout_sample_size=5,
        value_set_timeout_sample_seed=42,
        meta_id_timeout_sample_size=5,
        meta_id_timeout_sample_seed=99,
        sdk_operation_logs=[],
    )
    assert error is None
    return output, mode


def test_sampling_only_column_tasks_use_sampled_rows() -> None:
    cb = FakeCB(
        sample_data={
            ("b1", "s1", "users"): [
                ("u1", {"email": None, "meta": {"nested": 1}, "id": "A"}),
                ("u2", {"email": "a@x.io", "meta": "flat", "id": "A"}),
            ],
        }
    )
    keyspace_map = {"users": "s1.users"}

    output, mode = _run_task(ColumnExistsTask("users", "email"), cb, keyspace_map)
    assert mode == "sampling"
    assert output == {"exists_count": 1}

    output, mode = _run_task(ColumnNotNullTask("users", "email"), cb, keyspace_map)
    assert mode == "sampling"
    assert output == {"null_count": 1}

    output, mode = _run_task(ColumnNotObjectTask("users", "meta"), cb, keyspace_map)
    assert mode == "sampling"
    assert output == {"nested_count": 1}

    output, mode = _run_task(ColumnUniquenessTask("users", ("id",)), cb, keyspace_map)
    assert mode == "sampling"
    assert output == {"duplicate_groups": 1}


def test_sampling_only_type_compatibility_reports_observed_mismatch() -> None:
    cb = FakeCB(
        sample_data={
            ("b1", "s1", "child"): [("c1", {"fk": "abc"})],
            ("b1", "s1", "parent"): [("p1", {"pk": True})],
        }
    )
    keyspace_map = {"child": "s1.child", "parent": "s1.parent"}

    output, mode = _run_task(
        ColumnTypeCompatibilityTask("child", "fk", "parent", "pk"),
        cb,
        keyspace_map,
    )
    assert mode == "sampling"
    assert output == {"type_mismatch_count": 1}


def test_sampling_only_value_set_inclusion_and_meta_id_reference() -> None:
    cb = FakeCB(
        sample_data={
            ("b1", "s1", "orders"): [("o1", {"customer_id": "c1"}), ("o2", {"customer_id": "c2"})],
            ("b1", "s1", "customers"): [("c1", {"id": "c1"})],
        },
        existing_docs={("b1", "s1", "customers", "c1")},
    )
    keyspace_map = {"orders": "s1.orders", "customers": "s1.customers"}

    output, mode = _run_task(
        ValueSetInclusionTask(
            child_collection="orders",
            child_columns=("customer_id",),
            parent_collection="customers",
            parent_columns=("id",),
        ),
        cb,
        keyspace_map,
    )
    assert mode == "sampling"
    assert output == {"missing_count": 1}

    output, mode = _run_task(
        MetaIdReferenceExistsTask(
            child_collection="orders",
            child_column="customer_id",
            parent_collection="customers",
        ),
        cb,
        keyspace_map,
    )
    assert mode == "sampling"
    assert output == {"has_missing_reference": 1}
    assert ("b1", "s1", "customers", "c1") in cb.document_exists_calls
    assert ("b1", "s1", "customers", "c2") in cb.document_exists_calls


def test_relationship_verifier_sample_size_default_is_configurable() -> None:
    set_settings({"verifier_sample_size": 777})
    verifier = RelationshipVerifier(cb=FakeCB({}), bucket_name="b1")
    assert verifier._value_set_timeout_sample_size == 777
    assert verifier._meta_id_timeout_sample_size == 777
