"""Fyers API v3 — OAuth login and lightweight helpers.

Fyers no longer allows programmatic OTP/TOTP login for third-party apps
(the old vagator endpoints reject every request with -1025). The supported
path is the OAuth2 redirect flow:

  1. send the user to  /api/v3/generate-authcode?client_id&redirect_uri&...
  2. they authenticate on Fyers' hosted page (ID + PIN + TOTP)
  3. Fyers redirects back to our redirect_uri with ?auth_code=...
  4. /api/v3/validate-authcode  (appIdHash + auth_code)  -> access_token
  5. /api/v3/profile  confirms the token
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from urllib.parse import urlencode

import httpx

API = "https://api-t1.fyers.in/api/v3"
TIMEOUT = httpx.Timeout(30.0)


class FyersAuthError(Exception):
    """Any failure in the Fyers login flow."""


@dataclass(frozen=True)
class AppConfig:
    client_id: str        # "6349V3VIP6-200"
    secret_key: str
    redirect_uri: str

    @property
    def app_id_hash(self) -> str:
        return hashlib.sha256(
            f"{self.client_id}:{self.secret_key}".encode()
        ).hexdigest()

    def require(self) -> None:
        missing = [
            n
            for n, v in (
                ("FYERS_CLIENT_ID", self.client_id),
                ("FYERS_SECRET_KEY", self.secret_key),
                ("FYERS_REDIRECT_URI", self.redirect_uri),
            )
            if not v
        ]
        if missing:
            raise FyersAuthError("Missing app credentials: " + ", ".join(missing))


def authorize_url(app: AppConfig, state: str) -> str:
    """The Fyers hosted-login URL to send the user to."""
    app.require()
    params = {
        "client_id": app.client_id,
        "redirect_uri": app.redirect_uri,
        "response_type": "code",
        "state": state,
    }
    return f"{API}/generate-authcode?{urlencode(params)}"


async def exchange_code(app: AppConfig, auth_code: str) -> dict:
    """Swap the auth code from the redirect for an access token."""
    app.require()
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        try:
            resp = await client.post(
                f"{API}/validate-authcode",
                json={
                    "grant_type": "authorization_code",
                    "appIdHash": app.app_id_hash,
                    "code": auth_code,
                },
            )
            data = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise FyersAuthError(f"token exchange failed: {exc}") from exc

    access_token = data.get("access_token")
    if data.get("s") != "ok" or not access_token:
        raise FyersAuthError(data.get("message") or "validate-authcode rejected the code")

    profile = await fetch_profile(app.client_id, access_token)
    funds = await fetch_funds(app.client_id, access_token)
    return {
        "client_id": app.client_id,
        "fy_id": profile.get("fy_id") or "",
        "access_token": access_token,
        "refresh_token": data.get("refresh_token"),
        "auth_header": f"{app.client_id}:{access_token}",
        "profile": profile,
        "funds": funds,
    }


async def _authed_get(path: str, client_id: str, access_token: str) -> dict:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        try:
            resp = await client.get(
                f"{API}{path}",
                headers={"Authorization": f"{client_id}:{access_token}"},
            )
            return resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise FyersAuthError(f"{path} request failed: {exc}") from exc


async def fetch_profile(client_id: str, access_token: str) -> dict:
    """Hit /profile to confirm the token authenticates."""
    data = await _authed_get("/profile", client_id, access_token)
    if data.get("s") != "ok":
        raise FyersAuthError(data.get("message") or "token rejected by /profile")
    return data.get("data") or {}


async def fetch_funds(client_id: str, access_token: str) -> dict:
    """Return a small summary of the account funds.

    Fyers /funds returns a `fund_limit` list of titled rows; we surface the
    total and available balance.
    """
    data = await _authed_get("/funds", client_id, access_token)
    rows = data.get("fund_limit") or []
    by_title = {r.get("title", "").lower(): r for r in rows}

    def amount(title: str) -> float | None:
        row = by_title.get(title.lower())
        if not row:
            return None
        return row.get("equityAmount", row.get("equity_amount"))

    return {
        "total_balance": amount("Total Balance"),
        "available_balance": amount("Available Balance"),
        "rows": rows,
    }


async def token_is_valid(client_id: str, access_token: str) -> bool:
    try:
        await fetch_profile(client_id, access_token)
        return True
    except FyersAuthError:
        return False
