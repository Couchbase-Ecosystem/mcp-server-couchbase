"""
Enrichment module for MCP server.

This module runs in the MCP server's async event loop and enriches
catalog data using LLM sampling. It periodically checks for schema changes
and requests AI-generated descriptions and relationships.

This is MCP server infrastructure that consumes catalog data.
"""

from enrichment.catalog_enrichment import (
    ENRICHMENT_CHECK_INTERVAL,
    is_enrichment_cron_running,
    start_enrichment_cron,
    stop_enrichment_cron,
)

__all__ = [
    "start_enrichment_cron",
    "stop_enrichment_cron",
    "is_enrichment_cron_running",
    "ENRICHMENT_CHECK_INTERVAL",
]

