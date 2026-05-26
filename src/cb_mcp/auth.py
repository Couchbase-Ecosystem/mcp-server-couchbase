"""
Authentication provider for the Couchbase MCP Server.

Implements the OAuth 2.0 protected-resource pattern via FastMCP's
``RemoteAuthProvider``: the MCP server validates bearer JWTs locally against
a JWKS, advertises the upstream authorization server through RFC 9728
protected-resource metadata, and delegates everything else (Dynamic Client
Registration, token issuance, refresh, revocation) to that upstream server.

Provider-agnostic: any OAuth 2.1 / OIDC identity provider that publishes a
JWKS and supports DCR (Stytch, Auth0, WorkOS, Clerk, Keycloak-with-DCR, etc.)
plugs in via CLI flags or env vars — no provider-specific code lives here.

Optional metadata-proxy mode: when the upstream provider's OIDC discovery
doc omits ``registration_endpoint`` (e.g. Stytch supports DCR but doesn't
advertise it), pass ``upstream_metadata_url`` and ``registration_endpoint``
to ``build_remote_auth`` and mount ``metadata_proxy_handler`` on the FastMCP
app. The MCP server then serves an augmented AS metadata doc at
``/.well-known/oauth-authorization-server``, and clients discover DCR
through that route instead of the upstream's.
"""

import logging
from collections.abc import Callable, Coroutine
from typing import Any

import httpx
from fastmcp.server.auth import RemoteAuthProvider
from fastmcp.server.auth.providers.jwt import JWTVerifier
from pydantic import AnyHttpUrl
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from .utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.auth")

# Fields some providers (Stytch) add to their JSON responses that aren't
# part of the OIDC/OAuth metadata spec. Strip them when proxying.
_NON_METADATA_FIELDS = ("request_id", "status_code")


def build_remote_auth(
    *,
    base_url: str | None,
    jwks_uri: str | None,
    issuer: str | None,
    authorization_server: str | None,
    audience: str | None = None,
    required_scopes: list[str] | None = None,
    upstream_metadata_url: str | None = None,
) -> RemoteAuthProvider | None:
    """Build a ``RemoteAuthProvider`` from generic OAuth 2.0 metadata.

    Returns ``None`` when no auth config is supplied (all required fields
    empty), so the server runs unauthenticated — useful for stdio transport
    and local development.

    Required (must all be supplied together to enable auth):
        base_url: Public base URL of this MCP server. Advertised in
            protected-resource metadata.
        jwks_uri: JWKS endpoint of the upstream identity provider, used to
            verify token signatures.
        issuer: Expected ``iss`` claim on incoming JWTs.
        authorization_server: AS URL advertised in protected-resource metadata.
            In direct mode, point this at the upstream provider's AS URL. In
            metadata-proxy mode (``upstream_metadata_url`` set), this is
            automatically overridden to ``base_url`` so clients fetch our
            augmented metadata.

    Optional:
        audience: Expected ``aud`` claim. Leave ``None`` to skip audience
            checks (e.g. when the provider doesn't bind tokens to a resource).
        required_scopes: Scopes a token must carry to be accepted.
        upstream_metadata_url: Enables metadata-proxy mode. When provided,
            the PRM advertises ``base_url`` as the AS, and the caller is
            expected to mount ``metadata_proxy_handler`` at
            ``/.well-known/oauth-authorization-server`` (and the OIDC alias).

    Raises ``ValueError`` when some — but not all — required fields are set,
    to fail fast on misconfiguration instead of silently disabling auth.
    """
    required = {
        "base_url": base_url,
        "jwks_uri": jwks_uri,
        "issuer": issuer,
        "authorization_server": authorization_server,
    }
    supplied = {k: v for k, v in required.items() if v}

    if not supplied:
        return None
    if len(supplied) != len(required):
        missing = sorted(set(required) - set(supplied))
        raise ValueError(
            f"Incomplete auth configuration; missing: {missing}. "
            "Provide all of base_url, jwks_uri, issuer, authorization_server "
            "to enable auth, or none of them to run unauthenticated."
        )

    # In metadata-proxy mode, advertise ourselves as the AS in PRM so clients
    # fetch metadata from our augmented route, not the incomplete upstream one.
    advertised_as = base_url if upstream_metadata_url else authorization_server
    if upstream_metadata_url and authorization_server != base_url:
        logger.info(
            "Metadata-proxy mode enabled: PRM authorization_servers will "
            "advertise base_url (%s) instead of authorization_server (%s).",
            base_url,
            authorization_server,
        )

    token_verifier = JWTVerifier(
        jwks_uri=jwks_uri,
        issuer=issuer,
        audience=audience,
        required_scopes=required_scopes,
    )

    auth = RemoteAuthProvider(
        token_verifier=token_verifier,
        authorization_servers=[AnyHttpUrl(advertised_as)],
        base_url=base_url,
    )

    logger.info(
        "RemoteAuthProvider configured "
        "(issuer=%s, jwks_uri=%s, audience=%s, advertised_as=%s, base_url=%s, "
        "scopes=%s, metadata_proxy=%s)",
        issuer,
        jwks_uri,
        audience,
        advertised_as,
        base_url,
        required_scopes,
        bool(upstream_metadata_url),
    )
    return auth


def make_metadata_proxy_handler(
    *,
    upstream_metadata_url: str,
    extra_fields: dict[str, Any],
    timeout_seconds: float = 5.0,
) -> Callable[[Request], Coroutine[Any, Any, Response]]:
    """Build a Starlette handler that serves augmented AS metadata.

    The handler fetches the upstream OIDC/OAuth metadata document on each
    request, strips provider-specific noise, merges ``extra_fields`` over
    the top, and returns the result as JSON. Use this to inject fields the
    upstream provider supports but doesn't advertise (e.g. Stytch's
    ``registration_endpoint``).

    Per-request fetch keeps the served doc fresh if upstream config changes;
    the upstream call is cheap (small JSON, HTTPS, usually CDN-cached) and
    metadata is requested infrequently by clients.
    """

    async def handler(_request: Request) -> Response:
        try:
            async with httpx.AsyncClient(timeout=timeout_seconds) as client:
                resp = await client.get(upstream_metadata_url)
                resp.raise_for_status()
                doc = resp.json()
        except Exception as e:
            logger.error(
                "Failed to fetch upstream metadata from %s: %s",
                upstream_metadata_url,
                e,
            )
            return JSONResponse(
                {
                    "error": "upstream_metadata_unavailable",
                    "error_description": str(e),
                },
                status_code=502,
            )

        if not isinstance(doc, dict):
            return JSONResponse(
                {
                    "error": "upstream_metadata_invalid",
                    "error_description": "Upstream did not return a JSON object.",
                },
                status_code=502,
            )

        for field in _NON_METADATA_FIELDS:
            doc.pop(field, None)
        doc.update(extra_fields)
        return JSONResponse(doc)

    return handler
