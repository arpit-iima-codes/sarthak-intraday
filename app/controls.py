"""Persisted scanner controls (currently: the symbol universe).

Stored as data/controls.json. More controls will be added over time — keep
the schema additive and defaulted.
"""

from __future__ import annotations

import json
import re
from datetime import time as dtime

from .config import CONTROLS_FILE, FREEZE_TIME, MARKET_CLOSE, MARKET_OPEN
from . import marketdata

DEFAULTS: dict = {
    "universe": [],          # list of normalised Fyers symbols
    "universe_raw": "",      # what the user last typed (for the textarea)
    "freeze_time": FREEZE_TIME.strftime("%H:%M"),  # IST HH:MM
}


def load() -> dict:
    if CONTROLS_FILE.exists():
        try:
            return {**DEFAULTS, **json.loads(CONTROLS_FILE.read_text())}
        except (ValueError, OSError):
            pass
    return dict(DEFAULTS)


def save(data: dict) -> dict:
    merged = {**load(), **data}
    CONTROLS_FILE.write_text(json.dumps(merged, indent=2))
    return merged


def normalize_symbol(raw: str) -> str | None:
    s = re.sub(r"\s+", "", raw).upper()
    if not s:
        return None
    if ":" in s:
        return s
    if s.endswith("-INDEX") or s.endswith("-EQ"):
        return f"NSE:{s}"
    return f"NSE:{s}-EQ"


def parse_freeze_time(raw: str) -> str:
    """Validate an IST HH:MM freeze time; return it normalised."""
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", raw or "")
    if not m:
        raise ValueError("use HH:MM (24-hour)")
    h, mi = int(m.group(1)), int(m.group(2))
    if not (0 <= h < 24 and 0 <= mi < 60):
        raise ValueError("not a valid time")
    t = dtime(h, mi)
    if t <= MARKET_OPEN or t >= MARKET_CLOSE:
        raise ValueError(
            f"must be between {MARKET_OPEN:%H:%M} and {MARKET_CLOSE:%H:%M} IST"
        )
    return f"{h:02d}:{mi:02d}"


def parse_universe(text: str) -> list[str]:
    """Split a comma/newline list into de-duplicated normalised symbols."""
    seen: set[str] = set()
    out: list[str] = []
    for part in re.split(r"[,\n;]+", text or ""):
        sym = normalize_symbol(part)
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


def validate(auth: str, symbols: list[str]) -> list[dict]:
    """Check each symbol against the broker. Returns per-symbol results."""
    if not symbols:
        return []
    quotes = marketdata.quotes(auth, symbols)
    results = []
    for sym in symbols:
        v = quotes.get(sym) or {}
        if "error" in v:
            results.append({"symbol": sym, "ok": False, "reason": v["error"]})
        else:
            results.append(
                {
                    "symbol": sym,
                    "ok": True,
                    "name": v.get("short_name") or v.get("description") or sym,
                    "ltp": v.get("lp"),
                }
            )
    return results
