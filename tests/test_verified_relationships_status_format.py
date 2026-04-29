from __future__ import annotations

from typing import Any

import pytest

from catalog.enrichment.relationship_verifier.common.relationships import (
    ForeignKeyRelationship,
    InferredRelationship,
    PrimaryKeyRelationship,
)
from catalog.enrichment.relationship_verifier.integration_utils import (
    verified_relationships,
)
from catalog.enrichment.relationship_verifier.relationship_verifier import (
    VerificationResult,
)


@pytest.mark.asyncio
async def test_verification_status_uses_implicit_verified_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verification section should omit explicit verified lines."""
    pk_rel = PrimaryKeyRelationship(table="s.parent", columns=("$meta_id",))
    fk_rel = ForeignKeyRelationship(
        child_table="s.child",
        child_columns=("parent_id",),
        parent_table="s.parent",
        parent_columns=("id",),
    )
    skipped_rel = InferredRelationship(
        kind="OM",
        table1="s.parent",
        table2="s.child",
        foreign_key_table="s.child",
        from_columns=("parent_id",),
        to_columns=("id",),
    )

    monkeypatch.setattr(
        verified_relationships,
        "parse_relationship_text_to_relationships",
        lambda _: [pk_rel, fk_rel],
    )

    async def _fake_to_thread(*_: Any, **__: Any) -> Any:
        return (
            [
                VerificationResult(relationship=pk_rel, is_valid=True),
                VerificationResult(
                    relationship=fk_rel,
                    is_valid=False,
                    failure_reason="fk_referential_inclusion_failed",
                ),
            ],
            [(skipped_rel, "cross_bucket_relationship")],
        )

    monkeypatch.setattr(verified_relationships.asyncio, "to_thread", _fake_to_thread)

    output = await verified_relationships.append_verified_relationships_to_prompt(
        enriched_prompt="## RELATIONSHIPS\nPK(s.parent,$meta_id)\nFK(s.child,parent_id;s.parent,id)\n",
        database_info={"buckets": {"b1": {}}},
    )

    verification_section = output.split("## Relationship Verification Status", maxsplit=1)[1]
    assert "Key: F=failed, U=unable, S=skipped; not listed => verified" in verification_section
    assert "Summary: V=1 F=1 U=0 S=1" in verification_section
    assert "- F: FK(s.child,parent_id;s.parent,id) | fk_referential_inclusion_failed" in verification_section
    assert "- S: OM(s.parent,s.child) | cross_bucket_relationship" in verification_section
    assert "VERIFIED" not in verification_section


@pytest.mark.asyncio
async def test_verification_status_has_only_summary_when_all_verified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When all are verified, section should rely on summary without per-row V lines."""
    pk_rel = PrimaryKeyRelationship(table="s.t", columns=("$meta_id",))

    monkeypatch.setattr(
        verified_relationships,
        "parse_relationship_text_to_relationships",
        lambda _: [pk_rel],
    )

    async def _fake_to_thread(*_: Any, **__: Any) -> Any:
        return ([VerificationResult(relationship=pk_rel, is_valid=True)], [])

    monkeypatch.setattr(verified_relationships.asyncio, "to_thread", _fake_to_thread)

    output = await verified_relationships.append_verified_relationships_to_prompt(
        enriched_prompt="## RELATIONSHIPS\nPK(s.t,$meta_id)\n",
        database_info={"buckets": {"b1": {}}},
    )

    verification_section = output.split("## Relationship Verification Status", maxsplit=1)[1]
    assert "Summary: V=1 F=0 U=0 S=0" in verification_section
    assert "- F:" not in verification_section
    assert "- U:" not in verification_section
    assert "- S:" not in verification_section
    assert "VERIFIED" not in verification_section
