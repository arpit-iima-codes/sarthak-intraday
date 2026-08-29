"""Day positions + funds for the scanner side-panel.

Fyers /positions gives qty / entry / live P&L per symbol; /funds gives the
available balance. Target and SL are not broker data — a strategy layer will
fill them in later; for now they come from posmeta (empty by default).

Both upstream calls are cached briefly (module-level) so several browser tabs
polling the panel don't multiply into Fyers rate-limit trouble.
"""

from __future__ import annotations

import time
from datetime import datetime

import httpx

from . import posmeta
from .config import IST
from .fyers import API, TIMEOUT, FyersAuthError

_POS_TTL = 2.0        # seconds
_FUNDS_TTL = 20.0
_cache: dict = {"pos_at": 0.0, "pos": None, "funds_at": 0.0, "funds": None}


async def _get(path: str, auth: str) -> dict:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(f"{API}{path}", headers={"Authorization": auth})
        return resp.json()


async def _positions(auth: str) -> dict:
    now = time.time()
    if _cache["pos"] is not None and now - _cache["pos_at"] < _POS_TTL:
        return _cache["pos"]
    data = await _get("/positions", auth)
    if data.get("s") != "ok":
        raise FyersAuthError(data.get("message") or "positions rejected")
    _cache["pos"], _cache["pos_at"] = data, now
    return data


async def _available_balance(auth: str) -> float | None:
    now = time.time()
    if _cache["funds"] is not None and now - _cache["funds_at"] < _FUNDS_TTL:
        return _cache["funds"]
    data = await _get("/funds", auth)
    rows = data.get("fund_limit") or []
    by_title = {r.get("title", "").lower(): r for r in rows}
    row = by_title.get("available balance")
    avail = None
    if row:
        avail = row.get("equityAmount", row.get("equity_amount"))
    _cache["funds"], _cache["funds_at"] = avail, now
    return avail


def _entry(p: dict) -> float | None:
    for key in ("netAvg", "avgPrice", "netAvgPrice"):
        if p.get(key):
            return p[key]
    return p.get("sellAvg") if p.get("side") == -1 else p.get("buyAvg")


def _empty(connected: bool, error: str | None = None) -> dict:
    return {
        "connected": connected,
        "error": error,
        "server_time": datetime.now(IST).isoformat(),
        "positions": [],
        "pnl": None,
        "available_balance": None,
    }


async def snapshot(client_id: str, access_token: str) -> dict:
    auth = f"{client_id}:{access_token}"
    meta = posmeta.load()
    try:
        pos = await _positions(auth)
        avail = await _available_balance(auth)
    except FyersAuthError as exc:
        return _empty(True, str(exc))
    except (httpx.HTTPError, ValueError) as exc:
        return _empty(True, f"request failed: {exc}")

    rows = []
    for p in pos.get("netPositions", []) or []:
        sym = p.get("symbol")
        m = meta.get(sym, {})
        rows.append(
            {
                "symbol": sym,
                "qty": p.get("netQty", p.get("qty")),
                "entry": _entry(p),
                "ltp": p.get("ltp"),
                "pnl": p.get("pl"),
                "target": m.get("target"),
                "sl": m.get("sl"),
                "product": p.get("productType") or p.get("product"),
            }
        )
    # open (non-zero qty) first, then by |pnl|
    rows.sort(key=lambda r: (r["qty"] in (0, None), -abs(r["pnl"] or 0)))

    overall = pos.get("overall") or {}
    pnl = overall.get("pl_total")
    if pnl is None:
        pnl = sum((r["pnl"] or 0) for r in rows)

    out = _empty(True)
    out.update({"positions": rows, "pnl": pnl, "available_balance": avail})
    return out
