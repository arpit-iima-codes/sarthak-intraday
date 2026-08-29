"""sarthak-intraday — Fyers login (OAuth) + broker connection.

Screener and order terminal come later; this establishes the FastAPI app and
the Fyers connection they will build on.

  GET  /                 login page (redirects to /dashboard when connected)
  GET  /login            start the Fyers OAuth flow (redirect to Fyers)
  GET  /fyers/callback   OAuth redirect target — exchanges the code for a token
  POST /logout           drop the stored session
  GET  /dashboard        connected landing page (placeholder)
  GET  /health           json status
  GET  /api/session      json view of the current broker session (no token)
"""

from __future__ import annotations

import secrets
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from . import session as broker_session
from .config import SESSION_SECRET, get_settings
from .fyers import (
    AppConfig,
    FyersAuthError,
    authorize_url,
    exchange_code,
    fetch_funds,
    fetch_profile,
    token_is_valid,
)

BASE = Path(__file__).resolve().parent

app = FastAPI(title="sarthak-intraday", docs_url=None, redoc_url=None)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax")
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")

templates = Jinja2Templates(directory=BASE / "templates")


def app_config() -> AppConfig:
    s = get_settings()
    return AppConfig(
        client_id=s.fyers_client_id,
        secret_key=s.fyers_secret_key,
        redirect_uri=s.redirect_uri,
    )


async def active_session() -> dict | None:
    """Stored session, but only if its token still authenticates."""
    data = broker_session.load()
    if not data:
        return None
    if not await token_is_valid(data["client_id"], data["access_token"]):
        return None
    return data


def _flash(request: Request, message: str, level: str = "error") -> None:
    request.session.setdefault("_flash", []).append({"msg": message, "level": level})


def _take_flashes(request: Request) -> list[dict]:
    return request.session.pop("_flash", [])


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if request.session.get("authed") and await active_session():
        return RedirectResponse("/dashboard", status_code=303)

    s = get_settings()
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "creds_ready": s.creds_ready,
            "fy_id": s.fyers_fy_id,
            "redirect_uri": s.redirect_uri,
            "flashes": _take_flashes(request),
        },
    )


@app.get("/login")
async def start_login(request: Request):
    if not get_settings().creds_ready:
        _flash(request, "Fyers app credentials are not configured.")
        return RedirectResponse("/", status_code=303)

    state = secrets.token_urlsafe(16)
    request.session["oauth_state"] = state
    try:
        url = authorize_url(app_config(), state)
    except FyersAuthError as exc:
        _flash(request, str(exc))
        return RedirectResponse("/", status_code=303)
    return RedirectResponse(url, status_code=303)


@app.get("/fyers/callback")
async def fyers_callback(request: Request):
    params = request.query_params
    auth_code = params.get("auth_code") or params.get("code")
    returned_state = params.get("state")
    expected_state = request.session.pop("oauth_state", None)

    if params.get("s") == "error" or not auth_code:
        _flash(request, params.get("message") or "Fyers login was cancelled or failed.")
        return RedirectResponse("/", status_code=303)
    if not expected_state or returned_state != expected_state:
        _flash(request, "Login state mismatch — please try again.")
        return RedirectResponse("/", status_code=303)

    try:
        result = await exchange_code(app_config(), auth_code)
    except FyersAuthError as exc:
        _flash(request, str(exc))
        return RedirectResponse("/", status_code=303)

    record = broker_session.save(result)
    request.session["authed"] = True
    name = (record.get("profile") or {}).get("name") or record.get("fy_id") or "your account"
    _flash(request, f"Connected to Fyers as {name}.", "success")
    return RedirectResponse("/dashboard", status_code=303)


@app.post("/logout")
async def do_logout(request: Request):
    broker_session.clear()
    request.session.clear()
    _flash(request, "Disconnected.", "success")
    return RedirectResponse("/", status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    data = await active_session()
    if not data:
        request.session.clear()
        _flash(request, "Not connected. Please sign in.")
        return RedirectResponse("/", status_code=303)

    # refresh funds + profile live for display
    try:
        funds = await fetch_funds(data["client_id"], data["access_token"])
        profile = await fetch_profile(data["client_id"], data["access_token"])
    except FyersAuthError:
        funds = data.get("funds") or {}
        profile = data.get("profile") or {}

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "data": data,
            "profile": profile,
            "funds": funds,
            "flashes": _take_flashes(request),
        },
    )


@app.get("/health")
async def health():
    s = get_settings()
    data = broker_session.load()
    return {
        "app": "sarthak-intraday",
        "creds_configured": s.creds_ready,
        "broker_connected": bool(data),
        "connected_at": (data or {}).get("connected_at"),
    }


@app.get("/api/session")
async def api_session():
    data = broker_session.load()
    if not data:
        return JSONResponse({"connected": False}, status_code=404)
    return {
        "connected": True,
        "client_id": data.get("client_id"),
        "fy_id": data.get("fy_id"),
        "connected_at": data.get("connected_at"),
        "profile": data.get("profile") or {},
    }
