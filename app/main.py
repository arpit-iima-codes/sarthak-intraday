"""sarthak-intraday — Fyers connection, live scanner, controls.

  GET  /                    login page (redirects to /controls when connected)
  GET  /login               start the Fyers OAuth flow
  GET  /fyers/callback      OAuth redirect target
  POST /logout              drop the stored session
  GET  /scanner             daily live scanner
  GET  /controls            broker details + scanner controls (landing page)
  POST /controls/universe   validate + save the universe
  POST /controls/freeze-time  set the static-section freeze time (IST)
  GET  /api/scanner/state   one-shot scanner snapshot (JSON)
  GET  /api/scanner/stream  scanner snapshot stream (SSE, ~1s)
  GET  /health              json status
  GET  /api/session         json view of the current broker session
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from . import controls
from . import session as broker_session
from .config import IST, MARKET_CLOSE, MARKET_OPEN, SESSION_SECRET, get_settings
from .fyers import (
    AppConfig,
    FyersAuthError,
    authorize_url,
    exchange_code,
    fetch_funds,
    fetch_profile,
    token_is_valid,
)
from .marketdata import MarketDataError
from .scanner import scanner

BASE = Path(__file__).resolve().parent


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI):
    scanner.start()
    try:
        yield
    finally:
        scanner.stop()


app = FastAPI(title="sarthak-intraday", docs_url=None, redoc_url=None, lifespan=lifespan)
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


def broker_auth() -> str | None:
    data = broker_session.load()
    if not data or not data.get("access_token"):
        return None
    return f"{data['client_id']}:{data['access_token']}"


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


def _ctx(request: Request, **extra) -> dict:
    return {"flashes": _take_flashes(request), "nav": request.url.path, **extra}


# --------------------------------------------------------------------------- #
# auth
# --------------------------------------------------------------------------- #
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if request.session.get("authed") and await active_session():
        return RedirectResponse("/controls", status_code=303)

    s = get_settings()
    return templates.TemplateResponse(
        request,
        "login.html",
        _ctx(request, creds_ready=s.creds_ready, fy_id=s.fyers_fy_id,
             redirect_uri=s.redirect_uri),
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
    scanner.reload_controls()
    name = (record.get("profile") or {}).get("name") or record.get("fy_id") or "your account"
    _flash(request, f"Connected to Fyers as {name}.", "success")
    return RedirectResponse("/controls", status_code=303)


@app.post("/logout")
async def do_logout(request: Request):
    broker_session.clear()
    request.session.clear()
    _flash(request, "Disconnected.", "success")
    return RedirectResponse("/", status_code=303)


@app.get("/dashboard")
async def dashboard_moved():
    return RedirectResponse("/controls", status_code=301)


# --------------------------------------------------------------------------- #
# scanner
# --------------------------------------------------------------------------- #
@app.get("/scanner", response_class=HTMLResponse)
async def scanner_page(request: Request):
    data = await active_session()
    if not data:
        request.session.clear()
        _flash(request, "Not connected. Please sign in.")
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(
        request, "scanner.html",
        _ctx(request, universe_count=len(controls.load().get("universe", []))),
    )


@app.get("/api/scanner/state")
async def scanner_state():
    return scanner.snapshot()


@app.get("/api/scanner/stream")
async def scanner_stream(request: Request):
    async def gen():
        try:
            while True:
                if await request.is_disconnected():
                    break
                payload = json.dumps(scanner.snapshot(), default=str)
                yield f"data: {payload}\n\n"
                await asyncio.sleep(1)
        except asyncio.CancelledError:  # client went away
            raise

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --------------------------------------------------------------------------- #
# controls
# --------------------------------------------------------------------------- #
@app.get("/controls", response_class=HTMLResponse)
async def controls_page(request: Request):
    data = await active_session()
    if not data:
        request.session.clear()
        _flash(request, "Not connected. Please sign in.")
        return RedirectResponse("/", status_code=303)

    try:
        funds = await fetch_funds(data["client_id"], data["access_token"])
        profile = await fetch_profile(data["client_id"], data["access_token"])
    except FyersAuthError:
        funds = data.get("funds") or {}
        profile = data.get("profile") or {}

    connected_ist = None
    if data.get("connected_at"):
        try:
            connected_ist = (
                datetime.fromisoformat(data["connected_at"])
                .astimezone(IST)
                .strftime("%d %b %Y, %I:%M %p IST")
            )
        except ValueError:
            connected_ist = data["connected_at"]

    cfg = controls.load()
    raw = cfg.get("universe_raw") or ", ".join(cfg.get("universe", []))
    return templates.TemplateResponse(
        request, "controls.html",
        _ctx(request, data=data, profile=profile, funds=funds,
             connected_ist=connected_ist,
             universe_raw=raw, universe=cfg.get("universe", []),
             freeze_time=cfg.get("freeze_time"),
             market_open=MARKET_OPEN.strftime("%H:%M"),
             market_close=MARKET_CLOSE.strftime("%H:%M"),
             results=request.session.pop("_validation", None)),
    )


@app.post("/controls/universe")
async def save_universe(request: Request, universe: str = Form("")):
    if not await active_session():
        _flash(request, "Not connected. Please sign in.")
        return RedirectResponse("/", status_code=303)

    auth = broker_auth()
    symbols = controls.parse_universe(universe)
    if not symbols:
        controls.save({"universe": [], "universe_raw": universe})
        scanner.reload_controls()
        _flash(request, "Universe cleared.", "success")
        return RedirectResponse("/controls", status_code=303)

    try:
        results = controls.validate(auth, symbols)
    except MarketDataError as exc:
        _flash(request, f"Validation failed: {exc}")
        return RedirectResponse("/controls", status_code=303)

    valid = [r["symbol"] for r in results if r["ok"]]
    controls.save({"universe": valid, "universe_raw": universe})
    scanner.reload_controls()

    request.session["_validation"] = results
    bad = len(results) - len(valid)
    if bad:
        _flash(request, f"Saved {len(valid)} symbol(s); {bad} rejected — see below.",
               "error" if not valid else "success")
    else:
        _flash(request, f"All {len(valid)} symbol(s) valid and saved.", "success")
    return RedirectResponse("/controls", status_code=303)


@app.post("/controls/freeze-time")
async def save_freeze_time(request: Request, freeze_time: str = Form("")):
    if not await active_session():
        _flash(request, "Not connected. Please sign in.")
        return RedirectResponse("/", status_code=303)
    try:
        value = controls.parse_freeze_time(freeze_time)
    except ValueError as exc:
        _flash(request, f"Freeze time — {exc}.")
        return RedirectResponse("/controls", status_code=303)

    controls.save({"freeze_time": value})
    scanner.reload_controls()
    _flash(request, f"Freeze time set to {value} IST.", "success")
    return RedirectResponse("/controls", status_code=303)


# --------------------------------------------------------------------------- #
# misc
# --------------------------------------------------------------------------- #
@app.get("/health")
async def health():
    s = get_settings()
    data = broker_session.load()
    snap = scanner.snapshot()
    return {
        "app": "sarthak-intraday",
        "creds_configured": s.creds_ready,
        "broker_connected": bool(data),
        "connected_at": (data or {}).get("connected_at"),
        "scanner": {
            "universe": snap["universe_count"],
            "socket": snap["socket"]["status"],
            "frozen": snap["frozen"],
        },
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
