"""Fyers order placement — used by the buy engine in *live* mode only.

Raw httpx (kept independent, like marketdata.py). INTRADAY (MIS) market orders
only; the engine manages target / stop / square-off itself rather than leaning
on broker-side bracket orders.

`client_id` / `access_token` come from data/broker_session.json.
"""

from __future__ import annotations

import time

import httpx

API = "https://api-t1.fyers.in/api/v3"
TIMEOUT = httpx.Timeout(15.0)

SIDE = {"BUY": 1, "SELL": -1}
_SIDE_NAME = {1: "BUY", -1: "SELL"}

# Fyers order status codes
_STATUS_CANCELLED = 1
_STATUS_FILLED = 2
_STATUS_TRANSIT = 4
_STATUS_REJECTED = 5
_STATUS_PENDING = 6
_STATUS_EXPIRED = 7
_OPEN_STATUSES = {_STATUS_TRANSIT, _STATUS_PENDING}
_STATUS_LABEL = {
    _STATUS_CANCELLED: "cancelled", _STATUS_FILLED: "traded",
    _STATUS_TRANSIT: "in transit", _STATUS_REJECTED: "rejected",
    _STATUS_PENDING: "pending", _STATUS_EXPIRED: "expired",
}

_bal: dict = {"at": 0.0, "value": None}
_BAL_TTL = 15.0

_ob: dict = {"at": 0.0, "value": []}
_OB_TTL = 3.0


class OrderResult:
    __slots__ = ("ok", "id", "fill_price", "fill_confirmed", "message")

    def __init__(
        self,
        ok: bool,
        *,
        id: str | None = None,
        fill_price: float | None = None,
        fill_confirmed: bool = False,
        message: str = "",
    ) -> None:
        self.ok = ok
        self.id = id
        self.fill_price = fill_price
        self.fill_confirmed = fill_confirmed
        self.message = message


def _header(client_id: str, token: str) -> dict:
    return {"Authorization": f"{client_id}:{token}"}


def available_balance(client_id: str, token: str) -> float | None:
    """Live 'Available Balance' from /funds, cached ~15s. None if unreachable."""
    now = time.time()
    if _bal["value"] is not None and now - _bal["at"] < _BAL_TTL:
        return _bal["value"]
    try:
        with httpx.Client(timeout=TIMEOUT, headers=_header(client_id, token)) as c:
            data = c.get(f"{API}/funds").json()
    except (httpx.HTTPError, ValueError):
        return _bal["value"]
    rows = data.get("fund_limit") or []
    row = next(
        (r for r in rows if str(r.get("title", "")).strip().lower() == "available balance"),
        None,
    )
    val = None
    if row:
        val = row.get("equityAmount", row.get("equity_amount"))
    if val is not None:
        _bal.update(at=now, value=val)
    return val if val is not None else _bal["value"]


def open_orders(client_id: str, token: str) -> list[dict]:
    """Orders still open at the broker (transit / pending) — not yet filled,
    rejected, cancelled or expired. Cached ~3s so a busy UI poll doesn't hammer
    Fyers; returns the last good list (never raises) if a fetch fails."""
    now = time.time()
    if now - _ob["at"] < _OB_TTL:
        return _ob["value"]
    try:
        with httpx.Client(timeout=TIMEOUT, headers=_header(client_id, token)) as c:
            data = c.get(f"{API}/orders").json()
    except (httpx.HTTPError, ValueError):
        return _ob["value"]
    if data.get("s") != "ok":
        return _ob["value"]

    rows = []
    for o in data.get("orderBook") or []:
        status = o.get("status")
        if status not in _OPEN_STATUSES:
            continue
        rows.append({
            "id": str(o.get("id") or ""),
            "symbol": o.get("symbol") or "",
            "side": _SIDE_NAME.get(o.get("side"), "?"),
            "qty": o.get("qty"),
            "price": o.get("limitPrice") or o.get("tradedPrice") or None,
            "status": _STATUS_LABEL.get(status, str(status)),
            "at": o.get("orderDateTime"),
        })
    _ob.update(at=now, value=rows)
    return rows


def place_market(
    client_id: str, token: str, symbol: str, qty: int, side: str
) -> OrderResult:
    """Place an INTRADAY market order. `side` is "BUY" or "SELL"."""
    body = {
        "symbol": symbol,
        "qty": int(qty),
        "type": 2,               # market
        "side": SIDE[side],
        "productType": "INTRADAY",
        "limitPrice": 0,
        "stopPrice": 0,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
    }
    try:
        with httpx.Client(timeout=TIMEOUT, headers=_header(client_id, token)) as c:
            data = c.post(f"{API}/orders/sync", json=body).json()
    except (httpx.HTTPError, ValueError) as exc:
        return OrderResult(False, message=f"order request failed: {exc}")

    if data.get("s") != "ok" or not data.get("id"):
        return OrderResult(False, message=data.get("message") or "order rejected")

    oid = str(data["id"])
    price, confirmed = _await_fill(client_id, token, oid)
    return OrderResult(
        True, id=oid, fill_price=price, fill_confirmed=confirmed,
        message=data.get("message", ""),
    )


def _await_fill(client_id: str, token: str, order_id: str) -> tuple[float | None, bool]:
    """Poll the order book briefly for the traded price. (price, confirmed)."""
    for _ in range(6):
        time.sleep(0.6)
        try:
            with httpx.Client(timeout=TIMEOUT, headers=_header(client_id, token)) as c:
                data = c.get(f"{API}/orders", params={"id": order_id}).json()
        except (httpx.HTTPError, ValueError):
            continue
        for o in data.get("orderBook") or []:
            if str(o.get("id")) != order_id:
                continue
            status = o.get("status")
            if status == _STATUS_FILLED:
                tp = o.get("tradedPrice")
                return (round(float(tp), 2) if tp else None), True
            if status == _STATUS_REJECTED:
                return None, False
    return None, False
