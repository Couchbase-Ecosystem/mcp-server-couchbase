# Stytch Login App

Standalone "Authorization Endpoint URL" for Stytch Connected Apps. Lives in
its own folder, has its own dependencies, and does not import anything from
the Couchbase MCP server — so the two services can be deployed and scaled
independently.

## What it does

When the MCP server uses `RemoteAuthProvider` with Stytch as the upstream
authorization server, Stytch needs a URL to redirect users to for actual
authentication. This app fills that slot: it shows an email + OTP form, then
calls Stytch's `idp.oauth.authorize` server-side to mint the OAuth
authorization code, and bounces the user back to the original client.

```
VSCode ─► Stytch /oauth2/authorize
            │
            ▼ redirects user (browser)
        [this app] /authorize ─► email OTP ─► /verify-otp
                                                │
                                                ▼ server-side call
                                          Stytch idp.oauth.authorize
                                                │
                                                ▼ redirects user
                                          VSCode redirect_uri?code=…
```

## Setup

```bash
cd stytch
uv venv
uv pip install -e .
cp .env.example .env
# edit .env with your Stytch credentials
uv run app.py
```

The app listens on `http://127.0.0.1:5050` by default. `/healthz` returns
`ok` for quick liveness checks.

## Stytch dashboard configuration (one-time)

In <https://stytch.com/dashboard> for your project:

1. Open **Connected Apps**.
2. Set **Authorization Endpoint URL** to `http://localhost:5050/authorize`
   (for local testing; use a real HTTPS URL in production).
3. Enable Dynamic Client Registration if not already enabled.
4. Verify
   `https://test.stytch.com/v1/public/<project_id>/.well-known/openid-configuration`
   now returns `200` with `authorization_endpoint`, `token_endpoint`,
   `registration_endpoint`, and `jwks_uri` populated. If it still returns
   `400 authorization_endpoint_not_configured_for_project`, the dashboard
   configuration isn't saved yet.

## Local end-to-end test

Two terminals, both pointed at the same Stytch project:

```bash
# Terminal 1 — login app
cd stytch && uv run app.py

# Terminal 2 — MCP server
cd .. && uv run src/mcp_server.py \
  --transport http --host 127.0.0.1 --port 8000 \
  --mcp-base-url http://127.0.0.1:8000 \
  --auth-jwks-uri "https://test.stytch.com/v1/sessions/jwks/<project_id>" \
  --auth-issuer "stytch.com/<project_id>" \
  --auth-audience "<project_id>" \
  --auth-authorization-server "https://test.stytch.com/v1/public/<project_id>"
```

Then point an MCP client (VSCode, Claude Desktop, MCP Inspector) at
`http://127.0.0.1:8000/mcp` and walk through the OAuth flow. You should see
the login form served by this app, receive a Stytch OTP by email, and end
up with the client holding a JWT it can replay to the MCP server.

## Notes

- The OAuth params Stytch forwards (`client_id`, `redirect_uri`, `state`,
  `code_challenge`, `scope`, etc.) are carried through the OTP form as
  hidden inputs — the app keeps no server-side session state.
- `idp.oauth.authorize` is called with `consent_granted=True`. If you want
  an explicit consent screen, add a "Do you authorize…?" step between OTP
  verification and the call.
- `STYTCH_SECRET` lives only in `.env` (gitignored). Never commit it.
