"""
Stytch Connected Apps — Authorization Endpoint (login UI).

A standalone web app that fills the "Authorization Endpoint URL" slot in
Stytch's Connected Apps configuration. Stytch redirects users here after a
client (e.g. VSCode's MCP plugin) starts an OAuth authorize request; we
authenticate the user with email OTP, then call Stytch's IDP OAuth API
server-side to issue the authorization code, and redirect the user back to
the original client.

Runs entirely independently of the Couchbase MCP server — no shared imports.

Configure once in the Stytch dashboard:
    Connected Apps → Authorization Endpoint URL = http://localhost:5050/authorize
"""

import html
import logging
import os
from urllib.parse import parse_qs, urlparse

import uvicorn
from dotenv import load_dotenv
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response
from starlette.routing import Route

import stytch

load_dotenv()

PROJECT_ID = os.environ["STYTCH_PROJECT_ID"]
SECRET = os.environ["STYTCH_SECRET"]
DOMAIN = os.environ.get("STYTCH_DOMAIN") or (
    "https://api.stytch.com"
    if PROJECT_ID.startswith("project-live-")
    else "https://test.stytch.com"
)
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "5050"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("stytch-login")

client = stytch.Client(project_id=PROJECT_ID, secret=SECRET, custom_base_url=DOMAIN)

# OAuth params Stytch forwards to our authorization endpoint and that we
# replay back into idp.oauth.authorize_async. We carry these through the OTP
# flow via hidden form fields rather than server-side session state, so the
# app stays stateless.
OAUTH_FIELDS = (
    "client_id",
    "redirect_uri",
    "response_type",
    "scope",
    "state",
    "nonce",
    "code_challenge",
    "code_challenge_method",
    "prompt",
)


def _page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<style>
  body {{ font-family: -apple-system, system-ui, sans-serif; max-width: 460px;
         margin: 60px auto; padding: 0 20px; color: #1a1a1a; }}
  h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
  p.sub {{ color: #666; margin-top: 0; }}
  .card {{ border: 1px solid #e0e0e0; border-radius: 8px; padding: 24px;
           margin: 20px 0; background: #fafafa; }}
  input[type=email], input[type=text] {{ width: 100%; padding: 10px 12px;
           border: 1px solid #ccc; border-radius: 4px; font-size: 1rem;
           box-sizing: border-box; margin-top: 8px; }}
  button {{ background: #ea2328; color: #fff; border: 0; padding: 12px 24px;
            border-radius: 6px; font-size: 1rem; cursor: pointer; width: 100%;
            margin-top: 16px; }}
  button:hover {{ background: #c91e22; }}
  .err {{ color: #ea2328; margin: 12px 0; }}
  code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 3px;
          font-size: 0.9em; }}
</style>
</head>
<body>{body}</body>
</html>"""


def _hidden_fields(values: dict) -> str:
    """Render hidden inputs for the OAuth params we need to carry forward."""
    return "".join(
        f'<input type="hidden" name="{name}" value="{html.escape(str(values[name]))}">'
        for name in OAUTH_FIELDS
        if values.get(name)
    )


def _err(message: str, status: int = 400) -> HTMLResponse:
    return HTMLResponse(
        _page("Error", f'<h1>Error</h1><p class="err">{html.escape(message)}</p>'),
        status_code=status,
    )


async def authorize_get(request: Request) -> Response:
    """GET /authorize — Stytch lands here. Show the email entry form."""
    qp = dict(request.query_params)
    client_id = qp.get("client_id")
    redirect_uri = qp.get("redirect_uri")

    if not client_id or not redirect_uri:
        return _err("Missing client_id or redirect_uri.")

    # Surface what the client is asking for so the user knows what they're
    # consenting to. Best-effort: also show the host of the redirect URI.
    redirect_host = urlparse(redirect_uri).netloc or redirect_uri
    scope_display = qp.get("scope") or "(none)"

    body = f"""
    <h1>Sign in</h1>
    <p class="sub">Authorize <code>{html.escape(redirect_host)}</code> to access your account.</p>
    <form method="POST" action="/send-otp">
      {_hidden_fields(qp)}
      <div class="card">
        <label>Email address
          <input type="email" name="email" placeholder="you@example.com"
                 required autofocus>
        </label>
        <p class="sub" style="margin-top:12px;">Scopes requested: <code>{html.escape(scope_display)}</code></p>
      </div>
      <button type="submit">Send verification code</button>
    </form>
    """
    return HTMLResponse(_page("Sign in — Couchbase MCP", body))


async def send_otp(request: Request) -> Response:
    """POST /send-otp — Send a Stytch email OTP, show the code-entry form."""
    form = await request.form()
    email = str(form.get("email", "")).strip()
    if not email:
        return _err("Email required.")

    try:
        resp = await client.otps.email.login_or_create_async(email=email)
    except Exception as e:
        log.error("OTP send failed: %s", e)
        return _err(f"Failed to send code: {e}", status=502)

    method_id = resp.email_id
    log.info("OTP sent | email=%s method_id=%s", email, method_id)

    carry = {**{f: form.get(f, "") for f in OAUTH_FIELDS}, "method_id": method_id}
    body = f"""
    <h1>Check your email</h1>
    <p class="sub">We sent a 6-digit code to <strong>{html.escape(email)}</strong>.</p>
    <form method="POST" action="/verify-otp">
      {_hidden_fields(carry)}
      <input type="hidden" name="method_id" value="{html.escape(method_id)}">
      <div class="card">
        <input type="text" name="otp_code" inputmode="numeric"
               pattern="[0-9]{{6}}" maxlength="6" placeholder="123456"
               required autofocus>
      </div>
      <button type="submit">Verify &amp; authorize</button>
    </form>
    """
    return HTMLResponse(_page("Verify code — Couchbase MCP", body))


async def verify_otp(request: Request) -> Response:
    """POST /verify-otp — Verify the OTP, call Stytch to mint an auth code, redirect."""
    form = await request.form()
    otp_code = str(form.get("otp_code", "")).strip()
    method_id = str(form.get("method_id", ""))
    if not otp_code or not method_id:
        return _err("Missing verification code.")

    # Verify OTP — short session, just enough to call idp.oauth.authorize.
    try:
        otp = await client.otps.authenticate_async(
            method_id=method_id,
            code=otp_code,
            session_duration_minutes=5,
        )
    except Exception as e:
        log.warning("OTP verification failed: %s", e)
        return _err("Invalid or expired verification code.", status=401)

    log.info("OTP verified | user_id=%s", otp.user_id)

    # Replay the OAuth params Stytch sent us originally into idp.oauth.authorize.
    # Only pass kwargs the SDK actually accepts and only when non-empty.
    scope_str = str(form.get("scope", "")).strip()
    scopes = scope_str.split() if scope_str else ["tools:read_only"]

    kwargs = {
        "consent_granted": True,
        "client_id": str(form.get("client_id", "")),
        "redirect_uri": str(form.get("redirect_uri", "")),
        "response_type": str(form.get("response_type", "code")) or "code",
        "scopes": scopes,
        "session_token": otp.session_token,
    }
    # `prompt` is forwarded so Stytch can honor `prompt=consent` from the client.
    # `code_challenge_method` is not accepted by the SDK (Stytch assumes S256),
    # so we deliberately drop it here even if the client sent it.
    for opt in ("state", "nonce", "code_challenge", "prompt"):
        val = str(form.get(opt, "")).strip()
        if val:
            kwargs[opt] = val

    try:
        auth_resp = await client.idp.oauth.authorize_async(**kwargs)
    except Exception as e:
        log.error("Stytch idp.oauth.authorize failed: %s", e)
        return _err(f"Authorization failed: {e}", status=502)

    # Stytch returns `redirect_uri` containing either `?code=…` on success or
    # `?error=…` on failure (per RFC 6749). Inspect it so misconfiguration
    # surfaces as a clear server-side log + visible error page, instead of
    # silently bouncing the user back to a client that just says "no code".
    final_uri = auth_resp.redirect_uri
    parsed = urlparse(final_uri)
    params = parse_qs(parsed.query)
    if "error" in params:
        err = params.get("error", [""])[0]
        desc = params.get("error_description", [""])[0]
        log.error(
            "Stytch returned OAuth error in redirect | client_id=%s error=%s "
            "description=%s redirect_uri=%s",
            kwargs["client_id"],
            err,
            desc,
            final_uri,
        )
        return _err(f"Authorization failed: {err} — {desc}", status=400)

    if "code" not in params:
        log.error(
            "Stytch redirect_uri has neither 'code' nor 'error' | uri=%s",
            final_uri,
        )
        return _err(
            "Authorization failed: missing code in Stytch response.", status=502
        )

    log.info(
        "Authorization code issued | client_id=%s redirect_uri=%s",
        kwargs["client_id"],
        kwargs["redirect_uri"],
    )
    return RedirectResponse(final_uri, status_code=302)


async def healthz(request: Request) -> Response:
    return HTMLResponse("ok")


async def index(request: Request) -> Response:
    body = """
    <h1>Stytch Login App</h1>
    <p class="sub">This service is the Stytch Connected Apps
       <strong>Authorization Endpoint</strong> for the Couchbase MCP Server.</p>
    <div class="card">
      <p>You're seeing this page because you opened the app directly.
         Real users arrive at <code>/authorize</code> via Stytch's OAuth flow,
         not at <code>/</code>.</p>
      <p>Health check: <a href="/healthz">/healthz</a></p>
    </div>
    """
    return HTMLResponse(_page("Stytch Login App", body))


app = Starlette(
    routes=[
        Route("/", index, methods=["GET"]),
        Route("/authorize", authorize_get, methods=["GET"]),
        Route("/send-otp", send_otp, methods=["POST"]),
        Route("/verify-otp", verify_otp, methods=["POST"]),
        Route("/healthz", healthz, methods=["GET"]),
    ],
)


if __name__ == "__main__":
    log.info(
        "Starting Stytch login app | project=%s domain=%s addr=http://%s:%s",
        PROJECT_ID,
        DOMAIN,
        HOST,
        PORT,
    )
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
