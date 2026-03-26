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
from typing import Any

from mcp.server.session import ServerSession
from mcp.types import SamplingMessage, TextContent

from catalog.enrichment.relationship_verifier.integration_utils import (
    append_verified_relationships_to_prompt,
)
from catalog.store.store import get_catalog_store
from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.enrichment")

# Enrichment check interval (2 minutes)
ENRICHMENT_CHECK_INTERVAL = 120  # seconds


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
        "- Under RELATIONSHIPS, output one relationship per line using only these formats:",
        "  - PK(scope.collection,column1,column2,...) ",
        "  - FK(scope.collection,child_col1,child_col2;scope.collection,parent_col1,parent_col2)",
        "  - OO(scope.collection,scope.collection)",
        "  - OM(scope.collection,scope.collection)",
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
    try:
        # Build the prompt
        prompt = _build_enrichment_prompt(database_info)

        logger.info("Requesting LLM enrichment via sampling")

        # Create sampling request
        result = await session.create_message(
            messages=[
                SamplingMessage(
                    role="user", content=TextContent(type="text", text=prompt)
                )
            ],
            max_tokens=4096,
            temperature=0.2,  # Lower temperature for more consistent descriptions
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
            enriched_prompt = result.content.text
            logger.info(f"Received enriched prompt ({len(enriched_prompt)} chars)")
            return enriched_prompt

        logger.warning("No text content in sampling response")
        return None

    except Exception as e:
        logger.error(f"Error requesting LLM enrichment: {e}", exc_info=True)
        return None


async def _check_and_enrich_catalog(session: ServerSession | None) -> None:
    """
    Check if catalog needs enrichment and perform it if necessary.

    Args:
        session: Optional MCP ServerSession for sampling
    """
    try:
        store = get_catalog_store()

        # Check if enrichment is needed
        if not store.get_needs_enrichment():
            logger.debug("No enrichment needed")
            return

        # Check if we have a session for sampling
        if session is None:
            logger.warning("No session available for sampling, skipping enrichment")
            return

        logger.info("Catalog needs enrichment, starting process")

        # Get database info
        database_info = store.get_database_info()

        if not database_info or not database_info.get("buckets"):
            logger.warning("No database info available for enrichment")
            store.set_needs_enrichment(False)
            return

        # Request LLM enrichment
        enriched_prompt = await _request_llm_enrichment(session, database_info)

        if enriched_prompt:
            prompt_to_store = await append_verified_relationships_to_prompt(
                enriched_prompt=enriched_prompt,
                database_info=database_info,
            )
            # Store the enriched prompt
            store.add_prompt(prompt_to_store)
            logger.info("Enriched prompt stored successfully")
            # Clear the enrichment flag
            store.set_needs_enrichment(False)
        else:
            logger.warning("Failed to get enriched prompt from LLM")

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
