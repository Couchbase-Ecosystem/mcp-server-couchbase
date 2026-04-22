"""
Request-scoped authentication context.

Hosts that perform per-request authentication (e.g. the managed Capella
runtime validating an API key) populate an ``AuthContext`` via their
middleware. Hosts without per-request auth (e.g. the standalone CLI,
where identity is implicit in the process) can ignore this entirely;
``AuthContext.anonymous()`` is the fallback.

The key name is standardized here so that both hosts agree on where
identity lives, but reading it from the MCP ``Context`` is host-specific:
FastMCP's ``ctx.get_state`` is async, so sync hosts must use an
alternative state slot (e.g. ``ctx.request_context.request.state``) if
they ever need to read identity from a sync tool body.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

AUTH_STATE_KEY = "couchbase_mcp.auth"


@dataclass(frozen=True)
class AuthContext:
    """Identity established for a single MCP request.

    Attributes:
        principal_id: Stable identifier for the caller
            (e.g. API key ID, username, or ``"anonymous"``).
        auth_scheme: Free-form label for how the caller was authenticated
            (e.g. ``"none"``, ``"static"``, ``"api-key"``, ``"oauth"``).
        metadata: Host-specific extras — for example, the name of the
            secret that holds the cluster credentials for this principal.
    """

    principal_id: str
    auth_scheme: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def anonymous(cls) -> "AuthContext":
        return cls(principal_id="anonymous", auth_scheme="none")
