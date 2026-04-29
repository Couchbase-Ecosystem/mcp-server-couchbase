"""
Catalog enrichment using LLM sampling.

This module runs in the MCP server's async event loop and:
1. Periodically checks for schema changes (every 2 minutes)
2. Uses MCP sampling to request LLM to generate descriptions
3. Stores enriched prompts back in the catalog store

This is part of the MCP server infrastructure that enriches catalog data.
"""

import asyncio
import json
import logging
import re
import time
from typing import Any

from mcp.server.session import ServerSession
from mcp.types import SamplingMessage, TextContent

from catalog.enrichment.catalog_info_formatter import (
    build_coverage_checklist,
    build_stub_section,
    find_missing_ids,
    format_database_info_compact,
    parse_described_ids,
)
from catalog.enrichment.relationship_verifier.integration_utils import (
    append_verified_relationships_to_prompt,
)
from catalog.store.store import compute_catalog_schema_hash, get_all_catalog_stores
from catalog.worker import has_catalog_first_refresh_completed
from utils.config import get_settings
from utils.constants import DEFAULT_ENRICHMENT_BUCKET_CONCURRENCY, MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.enrichment")

# Enrichment check interval (2 minutes)
ENRICHMENT_CHECK_INTERVAL = 120  # seconds
# Sampling timeout so a stalled client does not block the enrichment cron loop forever.
LLM_SAMPLING_TIMEOUT_SECONDS = 160
LLM_SAMPLING_MAX_ATTEMPTS = 3
LLM_SAMPLING_INITIAL_BACKOFF_SECONDS = 1.0
# Maximum coverage checklist items to send back in a follow-up "fill missing" prompt.
COMPLETENESS_FOLLOWUP_MAX_IDS = 200


def _compute_sampling_backoff_seconds(attempt: int) -> float:
    """Return exponential backoff delay for the given attempt number."""
    return LLM_SAMPLING_INITIAL_BACKOFF_SECONDS * (2 ** max(0, attempt - 1))


def _get_enrichment_bucket_concurrency() -> int:
    """Read enrichment bucket concurrency from settings with safe fallback."""
    raw_value = get_settings().get(
        "enrichment_bucket_concurrency", DEFAULT_ENRICHMENT_BUCKET_CONCURRENCY
    )
    try:
        parsed_value = int(raw_value)
    except (TypeError, ValueError):
        return DEFAULT_ENRICHMENT_BUCKET_CONCURRENCY
    return max(1, parsed_value)


def _format_checklist_block(checklist_ids: list[str]) -> str:
    """Render the coverage checklist as numbered lines for the LLM."""
    if not checklist_ids:
        return "(no entries)"
    return "\n".join(f"{idx + 1}. {entry}" for idx, entry in enumerate(checklist_ids))


def _build_non_relationship_enrichment_prompt(database_info: dict[str, Any]) -> str:
    """
    Build a prompt for the LLM to generate schema descriptions excluding relationships.

    The schema is rendered in a compact markdown layout (vs raw JSON) to reduce
    token usage and reduce the chance of truncated responses. A coverage
    checklist with stable IDs is embedded so the response can be programmatically
    verified and missing entries can be backfilled deterministically.

    Args:
        database_info: Database schema information

    Returns:
        Formatted prompt string for the LLM
    """
    compact_schema = format_database_info_compact(database_info)
    checklist_ids = build_coverage_checklist(database_info)
    checklist_block = _format_checklist_block(checklist_ids)

    prompt_parts = [
        "# Database Schema Analysis Request",
        "",
        "Please analyze the following Couchbase database schema and provide:",
        "1. A brief description for each bucket, scope, and collection",
        "2. Field descriptions for each key path in each collection",
        "3. Any patterns or conventions you observe",
        "",
        "Format your response as a structured prompt that can be used by an AI assistant to help users write better SQL++ queries.",
        "",
        "## Database Schema (compact view)",
        "",
        "Each table row lists path, observed types, and up to three sample values per type.",
        "Indexes are listed under each collection with their key columns.",
        "",
        compact_schema or "(no schema available)",
        "",
        "## Coverage Checklist (you MUST address every entry below)",
        "",
        "Each entry has a stable ID. In your response, describe each entry on its own",
        "line and include the entry's ID verbatim (so coverage can be machine-verified).",
        "Use these formats exactly:",
        "- COLLECTION: <scope>.<collection> -- <description>",
        "- FIELD: <scope>.<collection> :: <path> -- <description>",
        "- INDEX: <scope>.<collection> :: <name> -- <description>",
        "",
        checklist_block,
        "",
        "## Important Instructions:",
        "- Analyze the indexes section to understand which fields are optimized for filtering and sorting.",
        "- Analyze the sample values to understand actual data formats, date formats, and status codes.",
        "- Suggest optimal query patterns based on the available indexes.",
        "- Handle polymorphic collections (multiple document shapes in one collection) as a union schema.",
        "- Represent each collection with one merged schema (do not output separate schemas per variant).",
        "- If variant discriminator fields exist (for example: type='car' vs type='bike'), describe each variant and its fields.",
        "- Treat fields missing in a variant as NULL for that variant when reasoning about a table-like view.",
        "- Avoid assuming every field is present in every document; call out variant-specific columns explicitly.",
        "- For nested object fields, use dot paths (for example: a.b).",
        "- For fields inside arrays of objects, use [] in the path (for example: a.[].c.d).",
        "- Include key descriptions for all nested and array-object paths, not only top-level keys.",
        "- Use exact scope.collection names from the schema.",
        "- Do NOT include a RELATIONSHIPS section in this response.",
        "- Do NOT skip any checklist entry; every COLLECTION, FIELD, and INDEX ID above must appear in your response.",
    ]

    return "\n".join(prompt_parts)


def _build_completeness_followup_prompt(
    database_info: dict[str, Any],
    missing_ids: list[str],
) -> str:
    """Build a focused follow-up prompt asking only for the missing checklist IDs."""
    truncated = missing_ids[:COMPLETENESS_FOLLOWUP_MAX_IDS]
    compact_schema = format_database_info_compact(database_info)
    checklist_block = _format_checklist_block(truncated)
    note = ""
    if len(missing_ids) > len(truncated):
        note = (
            f"\n(Only the first {len(truncated)} of {len(missing_ids)} missing entries "
            "are listed; remaining entries will be auto-filled from the schema.)\n"
        )
    return "\n".join(
        [
            "# Schema Enrichment Follow-up",
            "",
            "Your previous response was missing descriptions for the entries listed below.",
            "Please return ONLY descriptions for these missing entries, one per line, using",
            "the same exact ID formats:",
            "- COLLECTION: <scope>.<collection> -- <description>",
            "- FIELD: <scope>.<collection> :: <path> -- <description>",
            "- INDEX: <scope>.<collection> :: <name> -- <description>",
            "",
            "Do not repeat entries that you already described. Do not include a",
            "RELATIONSHIPS section. Keep each description concise (one line).",
            note,
            "## Schema reference (compact view)",
            "",
            compact_schema or "(no schema available)",
            "",
            "## Missing entries to describe",
            "",
            checklist_block,
        ]
    )


def _build_relationship_enrichment_prompt(database_info: dict[str, Any]) -> str:
    """
    Build a prompt for the LLM to generate relationships only.

    Args:
        database_info: Database schema information

    Returns:
        Formatted relationships-only prompt for the LLM
    """
    prompt_parts = [
        "# Database Schema Relationship Analysis Request",
        "",
        "Please analyze the following Couchbase database schema and output only the RELATIONSHIPS section.",
        "",
        "## Database Schema:",
        "",
        json.dumps(database_info, indent=2),
        "",
        "## Important Instructions:",
        "- Include a section titled RELATIONSHIPS.",
        "- In the RELATIONSHIPS section, first add a short legend that explains each shorthand and its structure:",
        "  - PK: Primary key for a collection. Format: PK(scope.collection,$meta_id)",
        "  - PKA: Primary key alternative (logical key). Format: PKA(scope.collection,col1,col2)",
        "  - FK: Foreign key mapping from child columns to parent columns. Format: FK(scope.collection,child_col1,child_col2;scope.collection,parent_col1,parent_col2)",
        "  - OO: One-to-one relationship. Format: OO(scope.collection,scope.collection)",
        "  - OM: One-to-many relationship. Format: OM(scope.collection,scope.collection)",
        "- Under RELATIONSHIPS, output one relationship per line using only these formats:",
        "  - PK(scope.collection,$meta_id)",
        "  - PKA(scope.collection,col1,col2)",
        "  - FK(scope.collection,child_col1,child_col2;scope.collection,parent_col1,parent_col2)",
        "  - OO(scope.collection,scope.collection)",
        "  - OM(scope.collection,scope.collection)",
        "- Identify potential join keys even if they are not explicitly defined as foreign keys.",
        "- Emit PKA only when a stable logical key candidate exists.",
        "- For nested object fields, use dot paths (for example: a.b).",
        "- For fields inside arrays of objects, use [] in the path (for example: a.[].c.d).",
        "- Use exact scope.collection names from the schema.",
        "- Do NOT include collection descriptions, field descriptions, or query guidance.",
    ]
    return "\n".join(prompt_parts)


def _normalize_relationship_section(relationships_prompt: str) -> str:
    """Return relationships text with a guaranteed RELATIONSHIPS header."""
    normalized = relationships_prompt.strip()
    if not normalized:
        return ""

    if re.search(r"^##+\s*relationships\b", normalized, flags=re.IGNORECASE | re.MULTILINE):
        return normalized

    return f"## RELATIONSHIPS\n\n{normalized}"


def _merge_enrichment_sections(
    *,
    non_relationship_prompt: str,
    relationships_prompt: str | None,
) -> str:
    """Merge enrichment sections while preserving current order."""
    merged_base = non_relationship_prompt.rstrip()
    if not relationships_prompt:
        return f"{merged_base}\n"

    normalized_relationships = _normalize_relationship_section(relationships_prompt)
    if not normalized_relationships:
        return f"{merged_base}\n"
    return f"{merged_base}\n\n{normalized_relationships.rstrip()}\n"


async def _request_llm_enrichment_with_prompt(
    *,
    session: ServerSession,
    prompt: str,
    job_label: str,
) -> str | None:
    """Request LLM enrichment for a pre-built prompt with retries."""

    for attempt in range(1, LLM_SAMPLING_MAX_ATTEMPTS + 1):
        try:
            logger.info(
                "Requesting LLM enrichment via sampling for %s (attempt %s/%s)",
                job_label,
                attempt,
                LLM_SAMPLING_MAX_ATTEMPTS,
            )

            # Create sampling request with timeout so cron can recover on stalled clients.
            result = await asyncio.wait_for(
                session.create_message(
                    messages=[
                        SamplingMessage(
                            role="user", content=TextContent(type="text", text=prompt)
                        )
                    ],
                    max_tokens=4096,
                    temperature=0.2,  # Lower temperature for more consistent descriptions
                ),
                timeout=LLM_SAMPLING_TIMEOUT_SECONDS,
            )
            logger.debug(f"Sampling response received: {type(result).__name__}")

            # Extract the response text
            if (
                result
                and hasattr(result, "content")
                and (
                    isinstance(result.content, TextContent)
                    or hasattr(result.content, "text")
                )
            ):
                enriched_prompt = result.content.text or ""
                logger.info(
                    "Received %s enrichment prompt (%s chars)",
                    job_label,
                    len(enriched_prompt),
                )
                if enriched_prompt.strip():
                    return enriched_prompt
                logger.warning(
                    "Sampling response was empty for %s on attempt %s/%s",
                    job_label,
                    attempt,
                    LLM_SAMPLING_MAX_ATTEMPTS,
                )
            else:
                logger.warning(
                    "No text content in sampling response for %s on attempt %s/%s",
                    job_label,
                    attempt,
                    LLM_SAMPLING_MAX_ATTEMPTS,
                )
        except asyncio.TimeoutError:
            logger.warning(
                "LLM enrichment sampling timed out for %s after %ss on attempt %s/%s",
                job_label,
                LLM_SAMPLING_TIMEOUT_SECONDS,
                attempt,
                LLM_SAMPLING_MAX_ATTEMPTS,
            )
        except Exception as e:
            logger.warning(
                "Error requesting LLM enrichment for %s on attempt %s/%s: %s",
                job_label,
                attempt,
                LLM_SAMPLING_MAX_ATTEMPTS,
                e,
                exc_info=True,
            )

        if attempt < LLM_SAMPLING_MAX_ATTEMPTS:
            backoff_seconds = _compute_sampling_backoff_seconds(attempt)
            logger.info(
                "Retrying LLM enrichment sampling for %s in %.1fs (next attempt %s/%s)",
                job_label,
                backoff_seconds,
                attempt + 1,
                LLM_SAMPLING_MAX_ATTEMPTS,
            )
            await asyncio.sleep(backoff_seconds)

    logger.error(
        "LLM enrichment sampling failed for %s after %s attempts",
        job_label,
        LLM_SAMPLING_MAX_ATTEMPTS,
    )
    return None


def _ensure_complete_non_relationship_prompt(
    *,
    database_info: dict[str, Any],
    enriched_text: str,
    followup_text: str | None,
) -> str:
    """Combine LLM output with deterministic stubs so coverage is 100% guaranteed.

    Returns the merged prompt that contains all checklist IDs (whether described
    by the LLM or auto-filled from the schema).
    """
    expected_ids = build_coverage_checklist(database_info)
    described = parse_described_ids(enriched_text)
    if followup_text:
        described |= parse_described_ids(followup_text)

    missing = find_missing_ids(expected_ids, described)

    parts: list[str] = [enriched_text.rstrip()]
    if followup_text and followup_text.strip():
        parts.append("")
        parts.append("## Follow-up Coverage Additions")
        parts.append("")
        parts.append(followup_text.strip())

    if missing:
        logger.warning(
            "Auto-filling %d missing checklist entries after LLM enrichment "
            "(total expected=%d, described=%d)",
            len(missing),
            len(expected_ids),
            len(expected_ids) - len(missing),
        )
        stub_section = build_stub_section(database_info, missing)
        if stub_section:
            parts.append("")
            parts.append(stub_section)

    return "\n".join(parts).rstrip()


async def _request_llm_non_relationship_enrichment(
    session: ServerSession,
    database_info: dict[str, Any],
) -> str | None:
    """Request non-relationship enrichment content from the LLM with coverage safety net.

    Steps:
      1. Send the compact + checklist prompt.
      2. Parse described IDs from the response.
      3. If any IDs are missing, send ONE focused follow-up sampling request.
      4. Backfill any still-missing IDs with deterministic stubs so the stored
         prompt never loses schema information.
    """
    prompt = _build_non_relationship_enrichment_prompt(database_info)
    enriched_text = await _request_llm_enrichment_with_prompt(
        session=session,
        prompt=prompt,
        job_label="non_relationships",
    )
    if enriched_text is None:
        return None

    expected_ids = build_coverage_checklist(database_info)
    described = parse_described_ids(enriched_text)
    missing = find_missing_ids(expected_ids, described)

    followup_text: str | None = None
    if missing:
        logger.info(
            "LLM enrichment response missing %d/%d checklist entries; "
            "issuing follow-up sampling for missing items",
            len(missing),
            len(expected_ids),
        )
        followup_prompt = _build_completeness_followup_prompt(database_info, missing)
        followup_text = await _request_llm_enrichment_with_prompt(
            session=session,
            prompt=followup_prompt,
            job_label="non_relationships_followup",
        )

    return _ensure_complete_non_relationship_prompt(
        database_info=database_info,
        enriched_text=enriched_text,
        followup_text=followup_text,
    )


async def _request_llm_relationship_enrichment(
    session: ServerSession,
    database_info: dict[str, Any],
) -> str | None:
    """Request relationships-only enrichment content from the LLM."""
    prompt = _build_relationship_enrichment_prompt(database_info)
    return await _request_llm_enrichment_with_prompt(
        session=session,
        prompt=prompt,
        job_label="relationships",
    )


def _resolve_enrichment_result(
    *,
    bucket_name: str,
    job_label: str,
    result: str | Exception | None,
) -> str | None:
    """Normalize gather results for enrichment jobs."""
    if isinstance(result, Exception):
        logger.warning(
            "%s enrichment job failed for bucket=%s: %s",
            job_label,
            bucket_name,
            result,
        )
        return None
    return result


async def _process_bucket_enrichment(
    *,
    bucket_name: str,
    store: Any,
    session: ServerSession,
    semaphore: asyncio.Semaphore,
) -> None:
    """Process enrichment for one bucket using split async jobs."""
    async with semaphore:
        bucket_start = time.perf_counter()
        try:
            database_info = store.get_database_info()
            if not database_info or not database_info.get("buckets"):
                logger.debug(
                    "No database info available for enrichment (bucket=%s)",
                    bucket_name,
                )
                return

            current_schema_hash = compute_catalog_schema_hash(database_info)
            if current_schema_hash == store.get_schema_hash():
                logger.debug("No enrichment needed for bucket=%s", bucket_name)
                return

            logger.info(
                "Catalog needs enrichment for bucket=%s, starting process",
                bucket_name,
            )

            non_relationship_result, relationships_result = await asyncio.gather(
                _request_llm_non_relationship_enrichment(session, database_info),
                _request_llm_relationship_enrichment(session, database_info),
                return_exceptions=True,
            )

            non_relationship_prompt = _resolve_enrichment_result(
                bucket_name=bucket_name,
                job_label="Non-relationship",
                result=non_relationship_result,
            )
            relationships_prompt = _resolve_enrichment_result(
                bucket_name=bucket_name,
                job_label="Relationships",
                result=relationships_result,
            )

            if not non_relationship_prompt:
                logger.warning(
                    "Failed to get non-relationship prompt from LLM for bucket=%s",
                    bucket_name,
                )
                return

            merged_prompt = _merge_enrichment_sections(
                non_relationship_prompt=non_relationship_prompt,
                relationships_prompt=relationships_prompt,
            )

            store_mode = "partial_non_relationship_only"
            prompt_to_store = merged_prompt
            if relationships_prompt:
                prompt_to_store = await append_verified_relationships_to_prompt(
                    enriched_prompt=merged_prompt,
                    database_info=database_info,
                )
                store_mode = "full_with_relationships_and_verification"
            else:
                logger.warning(
                    "Relationships job unavailable for bucket=%s; storing non-relationship prompt only",
                    bucket_name,
                )

            store.add_prompt(prompt_to_store)
            store.set_schema_hash(current_schema_hash)
            logger.info(
                "Enriched prompt stored successfully for bucket=%s (mode=%s, non_rel_chars=%d, relationships_chars=%d)",
                bucket_name,
                store_mode,
                len(non_relationship_prompt),
                len(relationships_prompt or ""),
            )
        except Exception as bucket_error:
            logger.error("Error enriching bucket=%s: %s", bucket_name, bucket_error)
        finally:
            bucket_duration = time.perf_counter() - bucket_start
            logger.debug(
                "Enrichment check finished for bucket=%s in %.2fs",
                bucket_name,
                bucket_duration,
            )


async def _check_and_enrich_catalog(session: ServerSession | None) -> None:
    """
    Check if catalog needs enrichment and perform it if necessary.

    Args:
        session: Optional MCP ServerSession for sampling
    """
    try:
        if not has_catalog_first_refresh_completed():
            logger.info(
                "Skipping enrichment: waiting for first successful catalog refresh cycle"
            )
            return

        stores_by_bucket = get_all_catalog_stores()
        if not stores_by_bucket:
            logger.debug("No bucket store available for enrichment")
            return

        if session is None:
            logger.warning("No session available for sampling, skipping enrichment")
            return

        cycle_start = time.perf_counter()
        semaphore = asyncio.Semaphore(_get_enrichment_bucket_concurrency())

        await asyncio.gather(
            *(
                _process_bucket_enrichment(
                    bucket_name=bucket_name,
                    store=store,
                    session=session,
                    semaphore=semaphore,
                )
                for bucket_name, store in stores_by_bucket.items()
            )
        )
        cycle_duration = time.perf_counter() - cycle_start
        logger.debug("Enrichment cycle finished in %.2fs", cycle_duration)

    except Exception as e:
        logger.error(f"Error in catalog enrichment: {e}", exc_info=True)


async def run_enrichment_cron(session: ServerSession | None) -> None:
    """
    Run the enrichment cron job that waits for schema changes.

    This function runs in the MCP server's event loop and waits for a signal
    from the catalog worker that enrichment is needed.

    Args:
        session: Optional MCP ServerSession for sampling
    """
    logger.info("Catalog enrichment task started (cron job)")
    while True:
        try:
            # Perform enrichment
            await _check_and_enrich_catalog(session)

            # Wait for a bit before retrying
            await asyncio.sleep(ENRICHMENT_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"Error in enrichment cycle: {e}", exc_info=True)
            # Wait a bit before retrying to avoid tight loops on error
            await asyncio.sleep(60)


_ENRICHMENT_STATE: dict[str, asyncio.Task | None] = {"task": None}


def start_enrichment_cron(session: ServerSession | None) -> None:
    """
    Start the enrichment cron as an async task in the MCP server's event loop.

    Args:
        session: Optional MCP ServerSession for sampling
    """
    current_task = _ENRICHMENT_STATE["task"]
    if current_task is not None and not current_task.done():
        logger.warning("Enrichment cron is already running")
        return

    logger.info("Starting catalog enrichment cron task")
    _ENRICHMENT_STATE["task"] = asyncio.create_task(run_enrichment_cron(session))


async def stop_enrichment_cron() -> None:
    """Stop the enrichment cron task."""
    current_task = _ENRICHMENT_STATE["task"]
    if current_task is None or current_task.done():
        logger.warning("Enrichment cron is not running")
        return

    logger.info("Stopping catalog enrichment cron task")
    current_task.cancel()

    try:
        await current_task
    except asyncio.CancelledError:
        logger.info("Catalog enrichment cron task cancelled")

    _ENRICHMENT_STATE["task"] = None


def is_enrichment_cron_running() -> bool:
    """Check if the enrichment cron is running."""
    current_task = _ENRICHMENT_STATE["task"]
    return current_task is not None and not current_task.done()
