import logging

import click

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.config")


def validate_required_param(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> str:
    """Validate that a required parameter is not empty."""
    if not value or value.strip() == "":
        raise click.BadParameter(f"{param.name} cannot be empty")
    return value


def get_settings() -> dict:
    """Get settings from Click context."""
    ctx = click.get_current_context()
    return ctx.obj or {}


def validate_connection_config() -> None:
    """Validate that all required parameters for the MCP server are available when needed."""
    settings = get_settings()
    required_params = ["connection_string", "username", "password", "bucket_name"]
    missing_params = []

    for param in required_params:
        if not settings.get(param):
            missing_params.append(param)

    if missing_params:
        error_msg = f"Missing required parameters for the MCP server: {', '.join(missing_params)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
