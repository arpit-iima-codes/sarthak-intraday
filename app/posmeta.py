"""User-entered Target / SL per symbol for the positions panel.

Not broker data — just the trader's own levels, kept in
data/positions_meta.json:

    { "NSE:SBIN-EQ": {"target": 620.5, "sl": 590.0} }

Keyed by the full Fyers symbol so it survives the position closing and
reopening intraday.
"""

from __future__ import annotations

import json

from .config import DATA_DIR

_FILE = DATA_DIR / "positions_meta.json"


def load() -> dict:
    if _FILE.exists():
        try:
            data = json.loads(_FILE.read_text())
            if isinstance(data, dict):
                return data
        except (ValueError, OSError):
            pass
    return {}


def _num(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        n = round(float(v), 2)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def set_levels(symbol: str, target, sl) -> dict:
    """Store (or clear) the target/SL for one symbol. Returns the full map."""
    data = load()
    t, s = _num(target), _num(sl)
    if t is None and s is None:
        data.pop(symbol, None)
    else:
        data[symbol] = {"target": t, "sl": s}
    try:
        _FILE.write_text(json.dumps(data, indent=2))
    except OSError:
        pass
    return data
