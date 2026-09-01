"""Buy engine — 10:00-breakout long entries.

Watches the scanner: when a symbol's live price crosses *up* through its frozen
10:00 high, the engine buys it, sized to a fixed rupee budget. Two exit styles:

  * ``bracket`` — exit at +target_pct or -stop_pct from the fill
  * ``eod``     — hold until the square-off time

Capital: never more than ``min(total_capital, live available balance)`` is
deployed across open positions, and never more than ``max_positions`` at once.
A breakout that can't be funded / seated is queued and filled when a slot or
capital frees up (dropped if the price falls back below the 10:00 high first).
Each symbol is entered at most once per trading day.

Modes: ``paper`` simulates fills at the live price; ``live`` places Fyers
INTRADAY market orders. "Kill" disarms new entries but open positions keep being
managed to their exit.

One background thread (single uvicorn worker). Runtime state is persisted to
``data/engine_state.json`` so a mid-day restart resumes managing open positions;
the previous day's book is archived to ``data/engine_history/<date>.json``.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from datetime import date, datetime
from datetime import time as time_cls

from . import controls, orders
from . import session as broker_session
from .config import DATA_DIR, IST, MARKET_CLOSE, MARKET_OPEN
from .scanner import scanner

log = logging.getLogger("engine")

STATE_FILE = DATA_DIR / "engine_state.json"
HISTORY_DIR = DATA_DIR / "engine_history"

TICK_SECONDS = 1.5


def _parse_hhmm(raw: str, fallback: time_cls) -> time_cls:
    try:
        h, m = (int(x) for x in str(raw).split(":"))
        return time_cls(h, m)
    except (ValueError, TypeError):
        return fallback


def _disp(symbol: str) -> str:
    return symbol.split(":")[-1].replace("-EQ", "").replace("-INDEX", "")


class BuyEngine:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._dirty = False

        self.day: date | None = None
        self.armed = False
        self.positions: list[dict] = []      # open + closed, this day
        self.traded: set[str] = set()        # symbols entered today (one-shot)
        self.pending: list[str] = []         # broke out, waiting for a slot / capital
        self.errors: list[dict] = []         # recent order failures, for the UI
        self._prev_ltp: dict[str, float] = {}  # LTP seen on the previous tick

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        HISTORY_DIR.mkdir(exist_ok=True)
        self._load()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="buy_engine", daemon=True)
        self._thread.start()
        log.info("buy engine started (armed=%s)", self.armed)

    def stop(self) -> None:
        self._stop.set()
        self._persist(force=True)

    # ------------------------------------------------------------------ #
    # persistence
    # ------------------------------------------------------------------ #
    def _load(self) -> None:
        cfg = controls.load()
        today = datetime.now(IST).date()
        if STATE_FILE.exists():
            try:
                data = json.loads(STATE_FILE.read_text())
            except (ValueError, OSError):
                data = {}
        else:
            data = {}

        if data.get("day") == today.isoformat():
            self.day = today
            self.armed = bool(data.get("armed"))
            self.positions = list(data.get("positions", []))
            self.traded = set(data.get("traded", []))
            self.pending = list(data.get("pending", []))
            self.errors = list(data.get("errors", []))
            log.info(
                "resumed engine state: %d position(s), %d open",
                len(self.positions),
                sum(1 for p in self.positions if p["status"] == "open"),
            )
        else:
            self._reset_day(today, cfg)

    def _reset_day(self, day: date, cfg: dict) -> None:
        self.day = day
        self.armed = bool(cfg.get("engine_arm_next_session"))
        self.positions = []
        self.traded = set()
        self.pending = []
        self.errors = []
        self._prev_ltp = {}
        self._dirty = True
        log.info("engine: new trading day %s (armed=%s)", day, self.armed)

    def _persist(self, force: bool = False) -> None:
        if not (self._dirty or force):
            return
        payload = {
            "day": self.day.isoformat() if self.day else None,
            "armed": self.armed,
            "positions": self.positions,
            "traded": sorted(self.traded),
            "pending": self.pending,
            "errors": self.errors[-20:],
            "saved_at": datetime.now(IST).isoformat(),
        }
        try:
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, indent=2))
            os.replace(tmp, STATE_FILE)
            self._dirty = False
        except OSError:
            log.exception("engine state persist failed")

    def _archive(self, prev_day: str) -> None:
        try:
            HISTORY_DIR.mkdir(exist_ok=True)
            (HISTORY_DIR / f"{prev_day}.json").write_text(
                json.dumps(
                    {"day": prev_day, "positions": self.positions,
                     "traded": sorted(self.traded)},
                    indent=2,
                )
            )
        except OSError:
            log.exception("engine history archive failed")

    # ------------------------------------------------------------------ #
    # main loop
    # ------------------------------------------------------------------ #
    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                with self._lock:
                    self._tick()
                    self._persist()
            except Exception:  # noqa: BLE001 - keep the thread alive
                log.exception("engine tick failed")
            self._stop.wait(TICK_SECONDS)

    def _tick(self) -> None:
        now = datetime.now(IST)
        cfg = controls.load()

        today = now.date()
        if self.day != today:
            if self.day and any(p["status"] == "open" for p in self.positions):
                log.warning("engine: rolling day with open positions still recorded")
            if self.day:
                self._archive(self.day.isoformat())
            self._reset_day(today, cfg)

        qmap = scanner.quote_map()
        # LTP each symbol had on the previous tick — a genuine breakout is a
        # cross that happens *between* two ticks. Refreshed every tick whether
        # armed or not, so re-arming never fires on a stale comparison.
        prev_ltp = self._prev_ltp
        self._prev_ltp = {s: l for s, (_h, l) in qmap.items() if l is not None}

        in_session = now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE
        square_off = _parse_hhmm(cfg.get("engine_square_off"), time_cls(15, 15))

        self._manage_open(now, cfg, qmap, square_off)

        if (
            self.armed
            and in_session
            and scanner.is_frozen
            and now.time() < square_off
        ):
            self._scan_entries(cfg, qmap, prev_ltp)

    # ------------------------------------------------------------------ #
    # position management
    # ------------------------------------------------------------------ #
    def _manage_open(
        self, now: datetime, cfg: dict, qmap: dict, square_off: time_cls
    ) -> None:
        for pos in self.positions:
            if pos["status"] != "open":
                continue
            ltp = qmap.get(pos["symbol"], (None, None))[1]
            reason = None
            if now.time() >= square_off:
                reason = "eod"
            elif pos["style"] == "bracket" and ltp is not None:
                if ltp >= pos["target_price"]:
                    reason = "target"
                elif ltp <= pos["stop_price"]:
                    reason = "stop"
            if reason:
                self._exit(pos, ltp, reason)

    def _exit(self, pos: dict, ltp: float | None, reason: str) -> None:
        price = ltp if ltp is not None else pos["entry_price"]
        confirmed = True
        if pos["mode"] == "live":
            auth = _broker_auth()
            if auth:
                res = orders.place_market(*auth, pos["symbol"], pos["qty"], "SELL")
                if res.ok:
                    if res.fill_price is not None:
                        price = res.fill_price
                    confirmed = res.fill_confirmed
                    pos["exit_order_id"] = res.id
                else:
                    self._log_error(f"{_disp(pos['symbol'])} exit order rejected: {res.message}")
                    confirmed = False
            else:
                self._log_error(f"{_disp(pos['symbol'])} exit skipped — broker not connected")
                confirmed = False

        pos["status"] = "closed"
        pos["exit_price"] = round(price, 2)
        pos["exit_at"] = datetime.now(IST).isoformat()
        pos["exit_reason"] = reason
        pos["fill_confirmed"] = confirmed
        pos["pnl"] = round((pos["exit_price"] - pos["entry_price"]) * pos["qty"], 2)
        self._dirty = True
        log.info(
            "engine EXIT %s x%d @ %.2f (%s) pnl=%.2f",
            pos["symbol"], pos["qty"], pos["exit_price"], reason, pos["pnl"],
        )

    # ------------------------------------------------------------------ #
    # entries
    # ------------------------------------------------------------------ #
    def _scan_entries(self, cfg: dict, qmap: dict, prev_ltp: dict) -> None:
        open_syms = {p["symbol"] for p in self.positions if p["status"] == "open"}

        # 1. queue every fresh upward cross of the 10:00 high
        for sym, (s_high, ltp) in qmap.items():
            if s_high is None or ltp is None:
                continue
            if sym in self.traded or sym in self.pending or sym in open_syms:
                continue
            prev = prev_ltp.get(sym)
            if prev is not None and prev <= s_high < ltp:
                self.pending.append(sym)
                self._dirty = True
                log.info("engine: %s crossed 10:00 high %.2f (%.2f -> %.2f) - queued",
                         sym, s_high, prev, ltp)

        if not self.pending:
            return

        # 2. fill the queue, oldest first, within slots + capital
        open_pos = [p for p in self.positions if p["status"] == "open"]
        max_pos = int(cfg.get("engine_max_positions", 3))
        slots = max_pos - len(open_pos)
        budget = float(cfg.get("engine_budget", 5000))

        # In live mode the real available balance is a hard ceiling; in paper
        # mode the configured capital stands alone (so testing works with an
        # empty account).
        cap = float(cfg.get("engine_total_capital", 15000))
        if cfg.get("engine_mode") == "live":
            auth = _broker_auth()
            avail = orders.available_balance(*auth) if auth else None
            if avail is not None:
                cap = min(cap, avail)
        room = cap - sum(p["entry_price"] * p["qty"] for p in open_pos)

        still: list[str] = []
        for sym in self.pending:
            s_high, ltp = qmap.get(sym, (None, None))
            if s_high is None or ltp is None:
                still.append(sym)
                continue
            if ltp <= s_high:
                # breakout failed before we could act — drop it
                log.info("engine: %s fell back below the 10:00 high - dropping queued signal", sym)
                self._dirty = True
                continue
            if slots <= 0:
                still.append(sym)
                continue
            qty = int(budget // ltp)
            if qty < 1:
                self._log_error(
                    f"{_disp(sym)} skipped — one share (₹{ltp:,.0f}) exceeds the ₹{budget:,.0f} budget"
                )
                self.traded.add(sym)
                self._dirty = True
                continue
            cost = qty * ltp
            if cost > room:
                still.append(sym)   # wait for capital to free up
                continue
            if self._enter(sym, qty, ltp, cfg):
                slots -= 1
                room -= cost
        self.pending = still

    def _enter(self, sym: str, qty: int, ltp: float, cfg: dict) -> bool:
        mode = cfg.get("engine_mode", "paper")
        style = cfg.get("engine_style", "bracket")
        tgt = float(cfg.get("engine_target_pct", 1.0))
        stp = float(cfg.get("engine_stop_pct", 0.5))
        entry = ltp
        order_id = None
        confirmed = True

        if mode == "live":
            auth = _broker_auth()
            if not auth:
                self._log_error(f"{_disp(sym)} entry skipped — broker not connected")
                return False
            res = orders.place_market(*auth, sym, qty, "BUY")
            if not res.ok:
                self._log_error(f"{_disp(sym)} entry rejected: {res.message}")
                self.traded.add(sym)   # don't hammer a rejecting symbol every tick
                self._dirty = True
                return False
            if res.fill_price is not None:
                entry = res.fill_price
            order_id = res.id
            confirmed = res.fill_confirmed

        entry = round(entry, 2)
        pos = {
            "id": uuid.uuid4().hex[:8],
            "symbol": sym,
            "qty": qty,
            "side": "BUY",
            "mode": mode,
            "style": style,
            "entry_price": entry,
            "entry_at": datetime.now(IST).isoformat(),
            "target_price": round(entry * (1 + tgt / 100), 2),
            "stop_price": round(entry * (1 - stp / 100), 2),
            "order_id": order_id,
            "fill_confirmed": confirmed,
            "status": "open",
            "exit_price": None,
            "exit_at": None,
            "exit_reason": None,
            "pnl": None,
        }
        self.positions.append(pos)
        self.traded.add(sym)
        self._dirty = True
        log.info(
            "engine ENTER %s x%d @ %.2f (%s/%s) tgt=%.2f stop=%.2f",
            sym, qty, entry, mode, style, pos["target_price"], pos["stop_price"],
        )
        return True

    # ------------------------------------------------------------------ #
    # controls / arming
    # ------------------------------------------------------------------ #
    def set_armed(self, value: bool) -> None:
        with self._lock:
            self.armed = bool(value)
            if not value:
                self.pending = []   # forget un-filled signals; open positions stay managed
            self._dirty = True
            self._persist(force=True)
        log.info("engine %s", "ARMED" if value else "KILLED (managing open positions)")

    def _log_error(self, message: str) -> None:
        self.errors.append({"at": datetime.now(IST).isoformat(), "msg": message})
        self.errors = self.errors[-20:]
        self._dirty = True
        log.warning("engine: %s", message)

    # ------------------------------------------------------------------ #
    # snapshot for the API
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        now = datetime.now(IST)
        cfg = controls.load()
        with self._lock:
            qmap = scanner.quote_map()
            positions = []
            realised = 0.0
            unrealised = 0.0
            deployed = 0.0
            open_n = 0
            for p in self.positions:
                row = dict(p)
                if p["status"] == "open":
                    open_n += 1
                    ltp = qmap.get(p["symbol"], (None, None))[1]
                    row["ltp"] = ltp
                    live_pnl = (
                        round((ltp - p["entry_price"]) * p["qty"], 2)
                        if ltp is not None else None
                    )
                    row["pnl"] = live_pnl
                    unrealised += live_pnl or 0.0
                    deployed += p["entry_price"] * p["qty"]
                else:
                    row["ltp"] = p["exit_price"]
                    realised += p["pnl"] or 0.0
                positions.append(row)

            positions.sort(key=lambda r: (r["status"] != "open", r["entry_at"]))

            cap = float(cfg.get("engine_total_capital", 15000))
            live = cfg.get("engine_mode") == "live"
            auth = _broker_auth()
            avail = orders.available_balance(*auth) if auth else None
            effective_cap = min(cap, avail) if (live and avail is not None) else cap

            square_off = cfg.get("engine_square_off", "15:15")
            in_session = (
                now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE
            )
            return {
                "server_time": now.isoformat(),
                "day": self.day.isoformat() if self.day else None,
                "armed": self.armed,
                "mode": cfg.get("engine_mode", "paper"),
                "style": cfg.get("engine_style", "bracket"),
                "in_session": in_session,
                "scanner_frozen": scanner.is_frozen,
                "square_off": square_off,
                "past_square_off": now.time() >= _parse_hhmm(square_off, time_cls(15, 15)),
                "config": {
                    "budget": cfg.get("engine_budget"),
                    "total_capital": cap,
                    "target_pct": cfg.get("engine_target_pct"),
                    "stop_pct": cfg.get("engine_stop_pct"),
                    "max_positions": cfg.get("engine_max_positions"),
                    "arm_next_session": cfg.get("engine_arm_next_session"),
                },
                "capital": {
                    "cap": cap,
                    "available_balance": avail,
                    "effective_cap": effective_cap,
                    "deployed": round(deployed, 2),
                    "free": round(effective_cap - deployed, 2),
                },
                "counts": {
                    "open": open_n,
                    "closed": len(self.positions) - open_n,
                    "max_positions": int(cfg.get("engine_max_positions", 3)),
                    "traded_today": len(self.traded),
                    "pending": len(self.pending),
                },
                "pending": [_disp(s) for s in self.pending],
                "pnl": {
                    "realised": round(realised, 2),
                    "unrealised": round(unrealised, 2),
                    "total": round(realised + unrealised, 2),
                },
                "positions": positions,
                "errors": list(reversed(self.errors[-8:])),
            }


def _broker_auth() -> tuple[str, str] | None:
    data = broker_session.load()
    if not data or not data.get("access_token"):
        return None
    return data["client_id"], data["access_token"]


engine = BuyEngine()
