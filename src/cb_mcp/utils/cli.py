"""Click validators for CLI parameters.

These thin wrappers exist so multiple ``@click.command`` entrypoints can share identical
validation and fallback behaviour for common flags without re-implementing
the glue. The framework-agnostic parsing helpers live in
:mod:`cb_mcp.utils.logging`; this module keeps the ``click`` import isolated
to the layer that actually needs it.

Usage::

    @click.option("--log-level", callback=validate_log_level, ...)
    @click.option("--log-sinks", callback=validate_log_sinks, ...)
"""

import click

from .logging import parse_log_level, parse_log_sinks


def validate_log_level(
    ctx: click.Context, param: click.Parameter, value: str
) -> tuple[str, str | None]:
    """Click callback for ``--log-level``.

    Delegates to :func:`parse_log_level`, which falls back to the default
    level on invalid input and returns the original token so
    ``configure_logging`` can surface an error record once handlers are wired.
    """
    # del ctx, param
    return parse_log_level(value)


def validate_log_sinks(
    ctx: click.Context, param: click.Parameter, value: str
) -> tuple[set[str], list[str]]:
    """Click callback for ``--log-sinks``.

    Delegates to :func:`parse_log_sinks`, which keeps valid tokens, collects
    invalid ones for later reporting, and falls back to the default sink set
    when nothing valid survives.
    """
    # del ctx, param
    return parse_log_sinks(value)
