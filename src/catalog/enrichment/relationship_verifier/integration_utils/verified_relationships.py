"""Helpers to parse, verify, and append data-backed relationships to enrichment text."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from catalog.enrichment.relationship_verifier.common.relationship_text_parser import (
    parse_relationship_text_to_relationships,
)
from catalog.enrichment.relationship_verifier.common.relationships import (
    AnyRelationship,
    ForeignKeyRelationship,
    InferredRelationship,
    PrimaryKeyRelationship,
)
from catalog.enrichment.relationship_verifier.couchbase_utils.cb_utils import CB
from catalog.enrichment.relationship_verifier.relationship_verifier import (
    RelationshipVerifier,
)
from utils.config import get_settings
from utils.connection import connect_to_couchbase_cluster
from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.enrichment")


def _normalize_name(name: str) -> str:
    return name.strip().lower()


def _extract_relationship_tables(relationship: AnyRelationship) -> tuple[str, ...]:
    if isinstance(relationship, PrimaryKeyRelationship):
        return (relationship.table,)

    if isinstance(relationship, ForeignKeyRelationship):
        return (relationship.child_table, relationship.parent_table)

    if isinstance(relationship, InferredRelationship):
        return (
            relationship.table1,
            relationship.table2,
            relationship.foreign_key_table,
        )

    return ()


def _relationship_to_expression(relationship: AnyRelationship) -> str:
    if isinstance(relationship, PrimaryKeyRelationship):
        columns = ",".join(relationship.columns)
        if not columns:
            return f"PK({relationship.table})"
        return f"PK({relationship.table},{columns})"

    if isinstance(relationship, ForeignKeyRelationship):
        child_columns = ",".join(relationship.child_columns)
        parent_columns = ",".join(relationship.parent_columns)
        if not child_columns or not parent_columns:
            return f"FK({relationship.child_table};{relationship.parent_table})"
        return (
            f"FK({relationship.child_table},{child_columns};"
            f"{relationship.parent_table},{parent_columns})"
        )

    if isinstance(relationship, InferredRelationship):
        return f"{relationship.kind}({relationship.table1},{relationship.table2})"

    return str(relationship)


def _extract_index_columns(index_def: dict[str, Any]) -> list[str]:
    index_key = index_def.get("index_key")

    if isinstance(index_key, list):
        return [str(component) for component in index_key if component is not None]

    if isinstance(index_key, str) and index_key.strip():
        return [index_key.strip()]

    return []


def _iter_collection_entries(
    database_info: dict[str, Any],
) -> list[tuple[str, str, str, dict[str, Any]]]:
    entries: list[tuple[str, str, str, dict[str, Any]]] = []

    buckets = database_info.get("buckets")
    if not isinstance(buckets, dict):
        return entries

    for bucket_name, bucket_data in buckets.items():
        if not isinstance(bucket_data, dict):
            continue

        scopes = bucket_data.get("scopes")
        if not isinstance(scopes, dict):
            continue

        for scope_name, scope_data in scopes.items():
            if not isinstance(scope_data, dict):
                continue

            collections = scope_data.get("collections")
            if not isinstance(collections, dict):
                continue

            for collection_name, collection_data in collections.items():
                if not isinstance(collection_data, dict):
                    continue
                entries.append(
                    (
                        str(bucket_name),
                        str(scope_name),
                        str(collection_name),
                        collection_data,
                    )
                )

    return entries


def _build_verifier_maps(
    database_info: dict[str, Any],
) -> tuple[
    dict[str, str], dict[str, dict[str, str]], dict[str, dict[str, list[list[str]]]]
]:
    keyspace_to_bucket: dict[str, str] = {}
    bucket_keyspace_map: dict[str, dict[str, str]] = {}
    bucket_index_map: dict[str, dict[str, list[list[str]]]] = {}
    unqualified_occurrences: dict[str, set[str]] = {}

    for (
        bucket_name,
        scope_name,
        collection_name,
        collection_data,
    ) in _iter_collection_entries(database_info):
        normalized_bucket = bucket_name
        bucket_keyspace_map.setdefault(normalized_bucket, {})
        bucket_index_map.setdefault(normalized_bucket, {})

        qualified = f"{scope_name}.{collection_name}"
        normalized_qualified = _normalize_name(qualified)
        normalized_collection = _normalize_name(collection_name)

        keyspace_to_bucket[normalized_qualified] = normalized_bucket
        unqualified_occurrences.setdefault(normalized_collection, set()).add(
            normalized_bucket
        )

        bucket_keyspace_map[normalized_bucket].setdefault(
            normalized_collection,
            qualified,
        )

        parsed_index_keys: list[list[str]] = []
        for index_def in collection_data.get("indexes", []):
            if not isinstance(index_def, dict):
                continue
            index_columns = _extract_index_columns(index_def)
            if index_columns:
                parsed_index_keys.append(index_columns)

        bucket_index_map[normalized_bucket][normalized_collection] = parsed_index_keys
        bucket_index_map[normalized_bucket][normalized_qualified] = parsed_index_keys

    for collection_name, bucket_names in unqualified_occurrences.items():
        if len(bucket_names) == 1:
            keyspace_to_bucket[collection_name] = next(iter(bucket_names))

    return keyspace_to_bucket, bucket_keyspace_map, bucket_index_map


def _group_relationships_by_bucket(
    relationships: list[AnyRelationship],
    keyspace_to_bucket: dict[str, str],
) -> tuple[dict[str, list[AnyRelationship]], int]:
    relationships_by_bucket: dict[str, list[AnyRelationship]] = {}
    skipped = 0

    for relationship in relationships:
        tables = _extract_relationship_tables(relationship)
        bucket_for_relationship: str | None = None
        is_cross_bucket = False
        has_missing_table = False

        for table in tables:
            bucket = keyspace_to_bucket.get(_normalize_name(table))
            if bucket is None:
                has_missing_table = True
                break

            if bucket_for_relationship is None:
                bucket_for_relationship = bucket
            elif bucket_for_relationship != bucket:
                is_cross_bucket = True
                break

        if has_missing_table or bucket_for_relationship is None or is_cross_bucket:
            skipped += 1
            continue

        relationships_by_bucket.setdefault(bucket_for_relationship, []).append(
            relationship
        )

    return relationships_by_bucket, skipped


def _verify_relationships_blocking(
    *,
    relationships: list[AnyRelationship],
    database_info: dict[str, Any],
) -> tuple[list[AnyRelationship], int]:
    keyspace_to_bucket, bucket_keyspace_map, bucket_index_map = _build_verifier_maps(
        database_info
    )
    relationships_by_bucket, skipped_count = _group_relationships_by_bucket(
        relationships,
        keyspace_to_bucket,
    )

    try:
        settings = get_settings()
    except Exception:
        settings = {}

    connection_string = settings.get("connection_string")
    if not connection_string:
        raise ValueError("Missing connection settings for relationship verification")

    cluster = connect_to_couchbase_cluster(
        connection_string=str(connection_string),
        username=str(settings.get("username") or ""),
        password=str(settings.get("password") or ""),
        ca_cert_path=settings.get("ca_cert_path"),
        client_cert_path=settings.get("client_cert_path"),
        client_key_path=settings.get("client_key_path"),
    )
    cb = CB.from_cluster(cluster)
    verified_relationships: list[AnyRelationship] = []

    try:
        for bucket_name, bucket_relationships in relationships_by_bucket.items():
            verifier = RelationshipVerifier(
                cb=cb,
                bucket_name=bucket_name,
                keyspace_map=bucket_keyspace_map.get(bucket_name, {}),
                index_map=bucket_index_map.get(bucket_name, {}),
            )
            results = verifier.verify(bucket_relationships)
            verified_relationships.extend(
                result.relationship for result in results if result.is_valid
            )
    finally:
        try:
            cluster.close()
        except Exception:
            logger.debug("Failed to close verifier Couchbase cluster cleanly")

    return verified_relationships, skipped_count


async def append_verified_relationships_to_prompt(
    *,
    enriched_prompt: str,
    database_info: dict[str, Any],
) -> str:
    """Parse and verify relationships from LLM text, then append valid ones.

    Returns the original text unchanged when parsing or verification is unavailable.
    """
    try:
        candidate_relationships = parse_relationship_text_to_relationships(
            enriched_prompt
        )
    except Exception as error:
        logger.warning(
            "Could not parse relationship candidates from LLM response: %s",
            error,
        )
        return enriched_prompt

    if not candidate_relationships:
        logger.info("No relationship candidates found in LLM response")
        return enriched_prompt

    try:
        verified_relationships, skipped_count = await asyncio.to_thread(
            _verify_relationships_blocking,
            relationships=candidate_relationships,
            database_info=database_info,
        )
    except Exception as error:
        logger.warning(
            "Relationship verification unavailable, storing original prompt: %s",
            error,
        )
        return enriched_prompt

    if not verified_relationships:
        logger.info(
            "Relationship verification completed: 0 valid relationships (%d skipped)",
            skipped_count,
        )
        return enriched_prompt

    verified_lines = [
        f"- {_relationship_to_expression(relationship)}"
        for relationship in verified_relationships
    ]
    appended_section = "\n".join(
        [
            "## Verified Relationships (Data-backed)",
            *verified_lines,
        ]
    )
    logger.info(
        "Relationship verification completed: %d valid relationships (%d skipped)",
        len(verified_relationships),
        skipped_count,
    )
    return f"{enriched_prompt.rstrip()}\n\n{appended_section}\n"
