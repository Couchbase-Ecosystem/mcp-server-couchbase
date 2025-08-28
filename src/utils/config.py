import logging

import click

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.config")


def get_settings() -> dict:
    """Get settings from Click context."""
    ctx = click.get_current_context()
    return ctx.obj or {}


def get_bucket_name_from_settings() -> str | None:
    """Get bucket name from Click context."""
    ctx = click.get_current_context()
    return ctx.obj.get("bucket_name")


def resolve_bucket_name(passed_bucket_name: str | None = None) -> str:
    """Resolve the bucket name from passed argument or passed settings and validate it.

    Returns the resolved bucket name, or raises a ValueError if missing.
    """
    # If passed bucket name is provided, use it.
    # If not, check if bucket name is provided in the settings.
    # If not, raise an error.
    bucket_name = passed_bucket_name or get_bucket_name_from_settings()
    if not bucket_name:
        raise ValueError("Bucket name is required")
    return bucket_name
