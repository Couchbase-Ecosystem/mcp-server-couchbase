import logging

import click
import os
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
    required_params = ["connection_string", "bucket_name"]
    missing_params = []

    for param in required_params:
        if not settings.get(param):
            missing_params.append(param)

    if missing_params:
        error_msg = f"Missing required parameters for the MCP server: {', '.join(missing_params)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        validate_authentication_method(settings)
    except Exception as e:
        logger.error(f"Error validating authentication method: {e}")
        raise
    

def validate_authentication_method(params : dict ) -> bool:
    """Util function to verify either user/password combination OR client certificates have been included"""
    username = params.get("username")
    password = params.get("password")
    client_cert_path = params.get("client_cert_path")
    ca_cert_path = params.get("ca_cert_path")

    # Strip values to check for empty strings
    if username is not None:
        username = username.strip()
    if password is not None:
        password = password.strip()

    if client_cert_path:
        client_cert = os.path.join(client_cert_path, "client.pem")
        client_key = os.path.join(client_cert_path, "client.key")

        if not os.path.isfile(client_cert) or not os.path.isfile(client_key):
            raise click.BadParameter(
                f"Client certificate files not found in {client_cert_path}. Required: client.pem and client.key."
            )

        if username or password or username == "" or password =="":
            raise click.BadParameter(
                "You must use either a client certificate or username/password, not both."
            )

    elif username or password:
        if not username or not password:
            raise click.BadParameter(
                "Both username and password must be provided and non-empty if using basic authentication."
            )
    else:
        raise click.BadParameter(
            "You must provide either a client certificate path or username/password combination, neither received."
        )

    if not ca_cert_path:
        logger.warning(f"A trusted CA certificate has not been provided, using local trust store for TLS connections")