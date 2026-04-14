"""Unit tests for canonical catalog schema hashing."""

from __future__ import annotations

from copy import deepcopy

from catalog.store.store import compute_catalog_schema_hash


def _base_database_info() -> dict:
    return {
        "buckets": {
            "b1": {
                "name": "b1",
                "scopes": {
                    "s1": {
                        "name": "s1",
                        "collections": {
                            "c1": {
                                "name": "c1",
                                "schema": [
                                    {
                                        "variant_id": "variant_0",
                                        "doc_count": 10,
                                        "fields": {
                                            "id": {"string": ["a", "b"]},
                                            "age": {"number": [1, 2]},
                                        },
                                    }
                                ],
                                "indexes": [
                                    {
                                        "id": "idxid",
                                        "name": "idx_c1_age",
                                        "index_key": ["`age`"],
                                        "definition": "CREATE INDEX idx_c1_age ON `b1`.`s1`.`c1`(`age`)",
                                    }
                                ],
                            }
                        },
                    }
                },
            }
        }
    }


def test_hash_ignores_doc_count_samples_variant_and_index_id() -> None:
    """Hash should stay stable when only volatile fields change."""
    base = _base_database_info()
    changed = deepcopy(base)

    variant = changed["buckets"]["b1"]["scopes"]["s1"]["collections"]["c1"]["schema"][0]
    variant["doc_count"] = 9999
    variant["variant_id"] = "different_variant"
    variant["fields"]["id"]["string"] = ["z", "y", "x"]
    changed["buckets"]["b1"]["scopes"]["s1"]["collections"]["c1"]["indexes"][0]["id"] = (
        "new-id"
    )

    assert compute_catalog_schema_hash(base) == compute_catalog_schema_hash(changed)


def test_hash_changes_on_structural_field_type_change() -> None:
    """Hash must change when schema structure/type changes."""
    base = _base_database_info()
    changed = deepcopy(base)
    changed["buckets"]["b1"]["scopes"]["s1"]["collections"]["c1"]["schema"][0]["fields"][
        "age"
    ] = {"string": ["young"]}

    assert compute_catalog_schema_hash(base) != compute_catalog_schema_hash(changed)


def test_hash_changes_on_index_key_change() -> None:
    """Hash must change when index planning shape changes."""
    base = _base_database_info()
    changed = deepcopy(base)
    changed["buckets"]["b1"]["scopes"]["s1"]["collections"]["c1"]["indexes"][0][
        "index_key"
    ] = ["`id`"]

    assert compute_catalog_schema_hash(base) != compute_catalog_schema_hash(changed)
