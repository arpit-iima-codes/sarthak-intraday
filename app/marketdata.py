"""Fyers market-data REST helpers (independent, httpx).

Used for symbol validation and for the 10:00 static snapshot (1-minute
history). The live feed is a websocket, handled in scanner.py.

`auth` is the header value `"<client_id>:<access_token>"`.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta

import httpx

BASE = "https://api-t1.fyers.in"
TIMEOUT = httpx.Timeout(20.0)

QUOTES_BATCH = 50
QUOTES_RATE = 5.0        # requests/sec — Fyers rejects a faster burst
_QUOTES_GAP = 1.0 / QUOTES_RATE


class MarketDataError(Exception):
    pass


class RateLimited(MarketDataError):
    """The broker asked us to slow down — back off rather than retry."""


def _is_rate_limit(message: str) -> bool:
    m = (message or "").lower()
    return "limit" in m and ("request" in m or "rate" in m)


def quotes(auth: str, symbols: list[str]) -> dict[str, dict]:
    """Return {symbol: value-dict} for each symbol.

    A symbol that the broker rejects maps to {"error": "<reason>"}.

    Batches are spaced to stay under the broker's request rate. A chunk that
    fails ends the sweep but keeps whatever was already fetched - losing a
    whole pass because the last chunk was throttled just means fetching it all
    again next time, which is what provoked the throttle. Raises only if
    nothing at all came back.
    """
    out: dict[str, dict] = {}
    failure: Exception | None = None
    with httpx.Client(timeout=TIMEOUT, headers={"Authorization": auth}) as client:
        for i in range(0, len(symbols), QUOTES_BATCH):
            chunk = symbols[i : i + QUOTES_BATCH]
            if i:
                time.sleep(_QUOTES_GAP)
            try:
                resp = client.get(
                    f"{BASE}/data/quotes", params={"symbols": ",".join(chunk)}
                )
                body = resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                failure = MarketDataError(f"quotes request failed: {exc}")
                break

            if body.get("s") != "ok":
                msg = body.get("message") or "quotes rejected"
                failure = RateLimited(msg) if _is_rate_limit(msg) else MarketDataError(msg)
                break

            for row in body.get("d", []):
                name = row.get("n")
                value = row.get("v", {}) or {}
                if not name:
                    continue
                if value.get("s") == "error" or row.get("s") == "error":
                    out[name] = {"error": value.get("errmsg") or "invalid symbol"}
                else:
                    out[name] = value
    if failure is not None and not out:
        raise failure
    # symbols the API silently dropped, plus any chunk cut short by `failure` -
    # the caller retries them next pass, since their yclose is still missing
    for s in symbols:
        out.setdefault(s, {"error": "no data returned"})
    return out


def last_trading_date(auth: str, symbol: str, tz) -> date | None:
    """Date of the most recent daily candle — i.e. the last session with data."""
    today = datetime.now(tz).date()
    with httpx.Client(timeout=TIMEOUT, headers={"Authorization": auth}) as client:
        try:
            resp = client.get(
                f"{BASE}/data/history",
                params={
                    "symbol": symbol,
                    "resolution": "D",
                    "date_format": "1",
                    "range_from": (today - timedelta(days=12)).isoformat(),
                    "range_to": today.isoformat(),
                    "cont_flag": "1",
                },
            )
            body = resp.json()
        except (httpx.HTTPError, ValueError):
            return None
    candles = body.get("candles") or []
    if not candles:
        return None
    return datetime.fromtimestamp(candles[-1][0], tz).date()


def history_1m(auth: str, symbol: str, day: date) -> list[list]:
    """Return today's 1-minute candles: [[epoch, o, h, l, c, v], ...]."""
    ds = day.isoformat()
    with httpx.Client(timeout=TIMEOUT, headers={"Authorization": auth}) as client:
        try:
            resp = client.get(
                f"{BASE}/data/history",
                params={
                    "symbol": symbol,
                    "resolution": "1",
                    "date_format": "1",
                    "range_from": ds,
                    "range_to": ds,
                    "cont_flag": "1",
                },
            )
            body = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise MarketDataError(f"history request failed: {exc}") from exc
    if body.get("s") != "ok":
        raise MarketDataError(body.get("message") or f"history rejected for {symbol}")
    return body.get("candles", []) or []
