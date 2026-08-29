"""Persisted scanner controls (currently: the symbol universe).

Stored as data/controls.json. More controls will be added over time — keep
the schema additive and defaulted.
"""

from __future__ import annotations

import json
import re

from .config import CONTROLS_FILE
from . import marketdata

DEFAULTS: dict = {
    "universe": [],          # list of normalised Fyers symbols
    "universe_raw": "",      # what the user last typed (for the textarea)
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
