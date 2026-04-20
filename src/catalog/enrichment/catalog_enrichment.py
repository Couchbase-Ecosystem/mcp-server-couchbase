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
import time
from typing import Any

from mcp.server.session import ServerSession
from mcp.types import SamplingMessage, TextContent

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


def _build_enrichment_prompt(database_info: dict[str, Any]) -> str:
    """
    Build a prompt for the LLM to generate schema descriptions and relationships.

    Args:
        database_info: Database schema information

    Returns:
        Formatted prompt string for the LLM
    """
    prompt_parts = [
        "# Database Schema Analysis Request",
        "",
        "Please analyze the following Couchbase database schema and provide:",
        "1. A brief description for each bucket, scope, and collection",
        "2. Field descriptions for important fields in each collection",
        "3. Relationships between collections (foreign keys, references, etc.)",
        "4. Any patterns or conventions you observe",
        "",
        "Format your response as a structured prompt that can be used by an AI assistant to help users write better SQL++ queries.",
        "",
        "## Database Schema:",
        "",
        json.dumps(database_info, indent=2),
        "",
        "Please provide your analysis in a clear, concise format that includes:",
        "- Bucket descriptions",
        "- Scope descriptions",
        "- Collection descriptions with their purpose",
        "- Key field descriptions",
        "- Relationships between collections",
        "",
        "## Important Instructions:",
        "- Analyze the 'indexes' field to understand which fields are optimized for filtering and sorting.",
        "- Analyze the 'samples' field to understand the actual data values, date formats, and status codes.",
        "- Identify potential join keys even if they are not explicitly defined as foreign keys.",
        "- Suggest optimal query patterns based on the available indexes.",
        "- Handle polymorphic collections (multiple document shapes in one collection) as a union schema.",
        "- Represent each collection with one merged schema (do not output separate schemas per variant).",
        "- If variant discriminator fields exist (for example: type='car' vs type='bike'), describe each variant and its fields.",
        "- Treat fields missing in a variant as NULL for that variant when reasoning about a table-like view.",
        "- Avoid assuming every field is present in every document; call out variant-specific columns explicitly.",
        "- Include a final section titled RELATIONSHIPS.",
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
        "- Emit PKA only when a stable logical key candidate exists.",
        "- For nested object fields, use dot paths (for example: a.b).",
        "- For fields inside arrays of objects, use [] in the path (for example: a.[].c.d).",
        "- Use exact scope.collection names from the schema.",
    ]

    return "\n".join(prompt_parts)


async def _request_llm_enrichment(
    session: ServerSession, database_info: dict[str, Any]
) -> str | None:
    """
    Request LLM to enrich the schema with descriptions using sampling.

    Args:
        session: MCP ServerSession for sampling
        database_info: Database schema information

    Returns:
        Enriched prompt string or None if sampling fails
    """
    # Build the prompt once and retry sampling failures with exponential backoff.
    prompt = _build_enrichment_prompt(database_info)

    for attempt in range(1, LLM_SAMPLING_MAX_ATTEMPTS + 1):
        try:
            logger.info(
                "Requesting LLM enrichment via sampling (attempt %s/%s)",
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
                logger.info("Received enriched prompt (%s chars)", len(enriched_prompt))
                if enriched_prompt.strip():
                    return enriched_prompt
                logger.warning(
                    "Sampling response was empty on attempt %s/%s",
                    attempt,
                    LLM_SAMPLING_MAX_ATTEMPTS,
                )
            else:
                logger.warning(
                    "No text content in sampling response on attempt %s/%s",
                    attempt,
                    LLM_SAMPLING_MAX_ATTEMPTS,
                )
        except asyncio.TimeoutError:
            logger.warning(
                "LLM enrichment sampling timed out after %ss on attempt %s/%s",
                LLM_SAMPLING_TIMEOUT_SECONDS,
                attempt,
                LLM_SAMPLING_MAX_ATTEMPTS,
            )
        except Exception as e:
            logger.warning(
                "Error requesting LLM enrichment on attempt %s/%s: %s",
                attempt,
                LLM_SAMPLING_MAX_ATTEMPTS,
                e,
                exc_info=True,
            )

        if attempt < LLM_SAMPLING_MAX_ATTEMPTS:
            backoff_seconds = _compute_sampling_backoff_seconds(attempt)
            logger.info(
                "Retrying LLM enrichment sampling in %.1fs (next attempt %s/%s)",
                backoff_seconds,
                attempt + 1,
                LLM_SAMPLING_MAX_ATTEMPTS,
            )
            await asyncio.sleep(backoff_seconds)

    logger.error(
        "LLM enrichment sampling failed after %s attempts",
        LLM_SAMPLING_MAX_ATTEMPTS,
    )
    return None


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

        async def _process_bucket(bucket_name: str, store: Any) -> None:
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
                    enriched_prompt = await _request_llm_enrichment(session, database_info)
                    if not enriched_prompt:
                        logger.warning(
                            "Failed to get enriched prompt from LLM for bucket=%s",
                            bucket_name,
                        )
                        return

                    prompt_to_store = await append_verified_relationships_to_prompt(
                        enriched_prompt=enriched_prompt,
                        database_info=database_info,
                    )
                    store.add_prompt(prompt_to_store)
                    store.set_schema_hash(current_schema_hash)
                    logger.info(
                        "Enriched prompt stored successfully for bucket=%s", bucket_name
                    )
                except Exception as bucket_error:
                    logger.error(
                        "Error enriching bucket=%s: %s", bucket_name, bucket_error
                    )
                finally:
                    bucket_duration = time.perf_counter() - bucket_start
                    logger.debug(
                        "Enrichment check finished for bucket=%s in %.2fs",
                        bucket_name,
                        bucket_duration,
                    )

        await asyncio.gather(
            *(
                _process_bucket(bucket_name, store)
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
