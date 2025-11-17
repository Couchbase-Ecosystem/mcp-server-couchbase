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
from typing import Any, Optional

from mcp.server.session import ServerSession
from mcp.types import SamplingMessage, TextContent

from store import get_catalog_store
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
        "- Example query patterns that would be useful",
    ]
    
    return "\n".join(prompt_parts)


async def _request_llm_enrichment(
    session: ServerSession, database_info: dict[str, Any]
) -> Optional[str]:
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
                    role="user",
                    content=TextContent(
                        type="text",
                        text=prompt
                    )
                )
            ],
            max_tokens=4096,
            temperature=0.2,  # Lower temperature for more consistent descriptions
        )
        logger.info(result)
        
        # Extract the response text
        if result and hasattr(result, 'content'):
            if isinstance(result.content, TextContent):
                enriched_prompt = result.content.text
                logger.info(f"Received enriched prompt ({len(enriched_prompt)} chars)")
                return enriched_prompt
            elif hasattr(result.content, 'text'):
                enriched_prompt = result.content.text
                logger.info(f"Received enriched prompt ({len(enriched_prompt)} chars)")
                return enriched_prompt
        
        logger.warning("No text content in sampling response")
        return None
        
    except Exception as e:
        logger.error(f"Error requesting LLM enrichment: {e}", exc_info=True)
        return None


async def _check_and_enrich_catalog(session: Optional[ServerSession]) -> None:
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
            store.clear_needs_enrichment()
            return
        
        # Request LLM enrichment
        enriched_prompt = await _request_llm_enrichment(session, database_info)
        
        if enriched_prompt:
            # Store the enriched prompt
            store.add_prompt(enriched_prompt)
            logger.info("Enriched prompt stored successfully")
        else:
            logger.warning("Failed to get enriched prompt from LLM")
        
        # Clear the enrichment flag
        store.clear_needs_enrichment()
        
    except Exception as e:
        logger.error(f"Error in catalog enrichment: {e}", exc_info=True)


async def run_enrichment_cron(session: Optional[ServerSession]) -> None:
    """
    Run the enrichment cron job that periodically checks for changes.
    
    This function runs in the MCP server's event loop and checks every 2 minutes
    if the catalog needs enrichment.
    
    Args:
        session: Optional MCP ServerSession for sampling
    """
    logger.info("Catalog enrichment cron started")
    
    while True:
        try:
            await _check_and_enrich_catalog(session)
        except Exception as e:
            logger.error(f"Error in enrichment cron cycle: {e}", exc_info=True)
        
        # Wait for next cycle
        await asyncio.sleep(ENRICHMENT_CHECK_INTERVAL)


_enrichment_task: Optional[asyncio.Task] = None


def start_enrichment_cron(session: Optional[ServerSession]) -> None:
    """
    Start the enrichment cron as an async task in the MCP server's event loop.
    
    Args:
        session: Optional MCP ServerSession for sampling
    """
    global _enrichment_task
    
    if _enrichment_task is not None and not _enrichment_task.done():
        logger.warning("Enrichment cron is already running")
        return
    
    logger.info("Starting catalog enrichment cron task")
    _enrichment_task = asyncio.create_task(run_enrichment_cron(session))


async def stop_enrichment_cron() -> None:
    """Stop the enrichment cron task."""
    global _enrichment_task
    
    if _enrichment_task is None or _enrichment_task.done():
        logger.warning("Enrichment cron is not running")
        return
    
    logger.info("Stopping catalog enrichment cron task")
    _enrichment_task.cancel()
    
    try:
        await _enrichment_task
    except asyncio.CancelledError:
        logger.info("Catalog enrichment cron task cancelled")
    
    _enrichment_task = None


def is_enrichment_cron_running() -> bool:
    """Check if the enrichment cron is running."""
    global _enrichment_task
    return _enrichment_task is not None and not _enrichment_task.done()

