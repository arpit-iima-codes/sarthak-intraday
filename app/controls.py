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

    # --- buy engine (10:00-breakout) ---
    "engine_mode": "paper",            # paper | live
    "engine_arm_next_session": False,  # auto-arm the engine at the next day's open
    "engine_budget": 5000.0,           # rupees deployed per position
    "engine_total_capital": 15000.0,   # ceiling across all open positions
    "engine_target_pct": 1.0,          # bracket take-profit, % of entry
    "engine_stop_pct": 0.5,            # bracket stop-loss, % of entry
    "engine_max_positions": 3,         # max simultaneous open positions
    "engine_style": "bracket",         # bracket (target/stop) | eod (hold to close)
    "engine_square_off": "15:15",      # IST HH:MM — force-exit / stop new entries
}

ENGINE_STYLES = ("bracket", "eod")
ENGINE_MODES = ("paper", "live")


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


def parse_engine(form: dict) -> tuple[dict, list[str]]:
    """Validate the buy-engine settings form. Returns (clean_values, errors).

    Only keys that validated are returned; on any error nothing is saved.
    """
    errors: list[str] = []
    out: dict = {}

    mode = str(form.get("engine_mode", "")).strip().lower()
    if mode not in ENGINE_MODES:
        errors.append("Mode must be 'paper' or 'live'.")
    else:
        out["engine_mode"] = mode

    style = str(form.get("engine_style", "")).strip().lower()
    if style not in ENGINE_STYLES:
        errors.append("Trade style must be 'bracket' or 'eod'.")
    else:
        out["engine_style"] = style

    out["engine_arm_next_session"] = str(
        form.get("engine_arm_next_session", "")
    ).strip().lower() in ("1", "true", "on", "yes")

    def _pos_float(key: str, label: str) -> None:
        raw = form.get(key)
        try:
            v = round(float(raw), 2)
        except (TypeError, ValueError):
            errors.append(f"{label} must be a number.")
            return
        if v <= 0:
            errors.append(f"{label} must be greater than 0.")
            return
        out[key] = v

    _pos_float("engine_budget", "Budget per trade")
    _pos_float("engine_total_capital", "Total capital")
    _pos_float("engine_target_pct", "Target %")
    _pos_float("engine_stop_pct", "Stop-loss %")

    if "engine_budget" in out and "engine_total_capital" in out:
        if out["engine_budget"] > out["engine_total_capital"]:
            errors.append("Budget per trade can't exceed total capital.")

    try:
        mp = int(float(form.get("engine_max_positions")))
        if mp < 1:
            raise ValueError
        out["engine_max_positions"] = mp
    except (TypeError, ValueError):
        errors.append("Max positions must be a whole number of at least 1.")

    try:
        out["engine_square_off"] = parse_freeze_time(form.get("engine_square_off", ""))
    except ValueError as exc:
        errors.append(f"Square-off time — {exc}.")

    if errors:
        return {}, errors
    return out, []


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
