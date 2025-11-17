import logging

import click

from .constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.utils.config")


config = {}

def set_settings(settings: dict) -> None:
    """Set settings in global variable."""
    global config
    config = settings

def get_settings() -> dict:
    """Get settings from global variable."""
    return config
