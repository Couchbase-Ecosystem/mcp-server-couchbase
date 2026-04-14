"""Guardrail tests for relationship parsing and verifier task planning."""

from __future__ import annotations

from catalog.enrichment.relationship_verifier.common.relationship_text_parser import (
    parse_relationship_text_to_dicts,
)
from catalog.enrichment.relationship_verifier.common.relationships import (
    ForeignKeyRelationship,
)
from catalog.enrichment.relationship_verifier.relationships.foreign_key_relationship import (
    ForeignKeyRelationshipRule,
)


def test_parser_normalizes_meta_id_variants() -> None:
    """`$meta().id` variants should normalize to `$meta_id`."""
    entries = parse_relationship_text_to_dicts("PK(ss.dd,$meta().id)")

    assert len(entries) == 1
    assert entries[0]["kind"] == "PK"
    assert entries[0]["columns"] == ["$meta_id"]


def test_parser_skips_inferred_relationship_without_fk_pairs() -> None:
    """OO/OM without supporting FK pairs should be skipped."""
    entries = parse_relationship_text_to_dicts("OM(ss.dd,ss.dd)")

    assert entries == []


def test_fk_rule_skips_invalid_empty_mapping() -> None:
    """Invalid FK with empty columns should not build tasks and should fail cleanly."""
    rule = ForeignKeyRelationshipRule(
        ForeignKeyRelationship(
            child_table="ss.dd",
            child_columns=(),
            parent_table="ss.dd",
            parent_columns=(),
        )
    )

    assert rule.build_tasks() == []
    is_valid, reason = rule.verify([], {})
    assert is_valid is False
    assert reason and "invalid_column_mapping" in reason
