"""Daily live scanner.

A single background service, started with the web app (uvicorn runs one
worker). It owns:

  * one Fyers data websocket (live LTP / OHLC ticks)
  * a scheduler loop that seeds yesterday's close, freezes the static
    section at 10:00 IST (from 1-minute history, so a mid-day restart still
    reconstructs it), and recomputes the live % move
  * socket supervision: reconnect with backoff, stale-feed watchdog,
    re-subscribe on universe change

State is in memory and resets every trading day. Nothing is persisted.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from datetime import time as time_cls

from fyers_apiv3.FyersWebsocket import data_ws

from . import controls, marketdata
from . import session as broker_session
from .config import FREEZE_TIME, IST, MARKET_CLOSE, MARKET_OPEN

log = logging.getLogger("scanner")

STALE_FEED_SECONDS = 90        # no ticks this long while connected -> rebuild
SEED_INTERVAL = 5             # seconds between pre-freeze quote refreshes
HISTORY_THROTTLE = 0.15       # seconds between history calls during freeze
SUBSCRIBE_BATCH = 500        # symbols per subscribe/unsubscribe call
SUBSCRIBE_PAUSE = 0.3        # seconds between subscribe batches (server breathing room)


def _pct(value: float | None, base: float | None) -> float | None:
    if value is None or not base:
        return None
    return round((value / base - 1.0) * 100.0, 2)


class Row:
    __slots__ = (
        "symbol", "yclose",
        "s_open", "s_high", "s_low", "s_ltp", "s_pct",
        "ltp", "live_pct", "last_tick",
    )

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.yclose = None
        self.s_open = self.s_high = self.s_low = self.s_ltp = self.s_pct = None
        self.ltp = self.live_pct = None
        self.last_tick = 0.0

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "yclose": self.yclose,
            "s_open": self.s_open,
            "s_high": self.s_high,
            "s_low": self.s_low,
            "s_pct": self.s_pct,   # s_ltp is kept internally (feeds s_pct) but not sent
            "ltp": self.ltp,
            "live_pct": self.live_pct,
        }


class ScannerService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

        self.rows: dict[str, Row] = {}
        self.universe: list[str] = []
        self.freeze_time: time_cls = FREEZE_TIME
        self.day: date | None = None
        self.frozen_at: datetime | None = None
        self._need_refreeze = False       # freeze time moved into the past
        self._last_seed = 0.0
        self._data_epoch = 0.0            # last-trade time seen in quotes/ticks
        self._session_date: date | None = None   # last session with data
        self._session_checked = 0.0

        self._sock: data_ws.FyersDataSocket | None = None
        self._subscribed: set[str] = set()
        self._resub_pending = threading.Event()   # universe/connection changed
        self.sock_status = "idle"      # idle|no-credentials|connecting|connected|disconnected|stale|error
        self.sock_since = time.time()
        self.sock_last_msg = 0.0
        self.sock_attempts = 0

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        self._stop.clear()
        self.reload_controls()
        for target in (self._scheduler_loop, self._socket_loop):
            t = threading.Thread(target=target, name=target.__name__, daemon=True)
            t.start()
            self._threads.append(t)
        log.info("scanner started (%d symbols)", len(self.universe))

    def stop(self) -> None:
        self._stop.set()
        self._teardown_socket("idle")

    def _load_freeze_time(self) -> time_cls:
        raw = controls.load().get("freeze_time") or FREEZE_TIME.strftime("%H:%M")
        try:
            h, m = (int(x) for x in raw.split(":"))
            return time_cls(h, m)
        except (ValueError, TypeError):
            return FREEZE_TIME

    def reload_controls(self) -> None:
        self.universe = list(controls.load().get("universe", []))
        new_freeze = self._load_freeze_time()
        with self._lock:
            for sym in self.universe:
                self.rows.setdefault(sym, Row(sym))
            for sym in list(self.rows):
                if sym not in self.universe:
                    self.rows.pop(sym, None)
            if new_freeze != self.freeze_time:
                self.freeze_time = new_freeze
                if datetime.now(IST).time() < new_freeze:
                    self.frozen_at = None       # forming again until the new time
                    self._need_refreeze = False
                else:
                    self._need_refreeze = True   # re-capture from history, keep
                    #                              the current snapshot until then
                log.info("freeze time set to %s", new_freeze.strftime("%H:%M"))
        self._resub_pending.set()   # picked up by _socket_loop
        log.info("controls reloaded: %d symbols", len(self.universe))

    # kept for older callers
    reload_universe = reload_controls

    def _auth(self) -> str | None:
        data = broker_session.load()
        if not data or not data.get("access_token"):
            return None
        return f"{data['client_id']}:{data['access_token']}"

    # ------------------------------------------------------------------ #
    # scheduler
    # ------------------------------------------------------------------ #
    def _scheduler_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._ensure_day()
                self._seed_from_quotes()
                self._maybe_freeze()
                self._recompute_live()
            except Exception:  # noqa: BLE001 - keep the loop alive
                log.exception("scheduler tick failed")
            self._stop.wait(3)

    def _forming(self) -> bool:
        """True while the static section still accumulates from quotes/ticks.
        Once frozen (frozen_at set) the static columns are locked to the
        1-minute-history snapshot and never take live ticks again."""
        return self.frozen_at is None

    def _ensure_day(self) -> None:
        today = datetime.now(IST).date()
        with self._lock:
            if self.day == today:
                return
            self.day = today
            self.frozen_at = None
            self._last_seed = 0.0
            self._data_epoch = 0.0
            self._session_date = None
            self._session_checked = 0.0
            self.rows = {sym: Row(sym) for sym in self.universe}
        log.info("new trading day %s - state reset", today)

    def _seed_from_quotes(self) -> None:
        """Fill yesterday's close and, before the freeze, the forming OHLC."""
        now = time.time()
        if now - self._last_seed < SEED_INTERVAL:
            return
        forming = self._forming()
        with self._lock:
            if forming:
                need = list(self.rows)
            else:
                need = [s for s, r in self.rows.items() if r.yclose is None]
        if not need:
            self._last_seed = now
            return
        auth = self._auth()
        if not auth:
            return
        try:
            quotes = marketdata.quotes(auth, need)
        except marketdata.MarketDataError as exc:
            log.warning("seed quotes failed: %s", exc)
            return
        self._last_seed = now
        epochs = [
            int(v["tt"])
            for v in quotes.values()
            if str(v.get("tt", "")).strip().isdigit()
        ]
        if epochs:
            self._data_epoch = max(self._data_epoch, max(epochs))

        # which session the quote data belongs to (last daily candle)
        if (
            self.frozen_at is None
            and self.rows
            and now - self._session_checked > 600
        ):
            sd = marketdata.last_trading_date(auth, next(iter(self.rows)), IST)
            if sd:
                self._session_date = sd
            self._session_checked = now
        with self._lock:
            for sym, v in quotes.items():
                row = self.rows.get(sym)
                if not row or "error" in v:
                    continue
                row.yclose = v.get("prev_close_price") or row.yclose
                if row.ltp is None:
                    row.ltp = v.get("lp")
                if forming:
                    row.s_open = v.get("open_price") or row.s_open
                    row.s_high = v.get("high_price") or row.s_high
                    row.s_low = v.get("low_price") or row.s_low
                    row.s_ltp = v.get("lp") or row.s_ltp
                    row.s_pct = _pct(row.s_ltp, row.yclose)
                row.live_pct = _pct(row.ltp, row.s_high)

    def _maybe_freeze(self) -> None:
        now = datetime.now(IST)
        freeze_time = self.freeze_time
        if self.frozen_at is not None and not self._need_refreeze:
            return
        if now.weekday() >= 5 or now.time() < freeze_time:
            return
        auth = self._auth()
        if not auth:
            return
        with self._lock:
            symbols = list(self.rows)
        if not symbols:
            return
        cutoff = int(
            datetime.combine(self.day, freeze_time, tzinfo=IST).timestamp()
        )

        computed: dict[str, tuple] = {}
        for sym in symbols:
            if self._stop.is_set():
                return
            try:
                candles = marketdata.history_1m(auth, sym, self.day)
            except marketdata.MarketDataError as exc:
                log.warning("freeze history failed for %s: %s", sym, exc)
                continue
            pre = [c for c in candles if c and c[0] < cutoff]
            if pre:
                o = pre[0][1]
                h = max(c[2] for c in pre)
                low = min(c[3] for c in pre)
                close = pre[-1][4]
                computed[sym] = (o, h, low, close)
            time.sleep(HISTORY_THROTTLE)

        # No pre-freeze candles for anything -> not a real trading session
        # (market holiday, or history not available yet). Don't freeze; stay
        # in "forming" mode showing the last quotes and retry next tick.
        if not computed:
            log.info("freeze skipped - no 1-minute history before %s",
                     freeze_time.strftime("%H:%M"))
            return

        with self._lock:
            for sym, row in self.rows.items():
                if sym in computed:
                    row.s_open, row.s_high, row.s_low, row.s_ltp = computed[sym]
                row.s_pct = _pct(row.s_ltp, row.yclose)
                row.live_pct = _pct(row.ltp, row.s_high)
            self.frozen_at = datetime.now(IST)
            self._need_refreeze = False
        log.info(
            "static section frozen at %s (%d/%d from history)",
            self.frozen_at.strftime("%H:%M:%S"), len(computed), len(symbols),
        )

    def _recompute_live(self) -> None:
        with self._lock:
            for row in self.rows.values():
                row.live_pct = _pct(row.ltp, row.s_high)

    # ------------------------------------------------------------------ #
    # websocket
    # ------------------------------------------------------------------ #
    def _socket_loop(self) -> None:
        while not self._stop.is_set():
            now = datetime.now(IST)
            in_session = (
                now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE
            )
            auth = self._auth()

            if auth is None:
                self._teardown_socket("no-credentials")
                self._stop.wait(5)
                continue
            if not in_session:
                self._teardown_socket("idle")
                self._stop.wait(20)
                continue

            if self._sock is None:
                self._open_socket(auth)
            elif (
                self.sock_status == "connected"
                and self.sock_last_msg
                and time.time() - self.sock_last_msg > STALE_FEED_SECONDS
            ):
                log.warning("feed stale for %ds - rebuilding socket",
                            STALE_FEED_SECONDS)
                self._teardown_socket("stale")
            elif self.sock_status in ("disconnected", "error", "stale"):
                # SDK gave up / hard error -> back off then rebuild
                backoff = min(30, 2 ** min(self.sock_attempts, 5))
                self._stop.wait(backoff)
                self._teardown_socket(self.sock_status)
                self._open_socket(auth)

            if self.sock_status == "connected" and self._resub_pending.is_set():
                self._resub_pending.clear()
                self._sync_subscription()

            self._stop.wait(3)

    def _open_socket(self, auth: str) -> None:
        self.sock_attempts += 1
        self._set_status("connecting")
        try:
            sock = data_ws.FyersDataSocket(
                access_token=auth,
                litemode=False,
                write_to_file=False,
                reconnect=True,
                reconnect_retry=50,
                on_connect=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._sock = sock
            sock.connect()
        except Exception:  # noqa: BLE001
            log.exception("socket connect failed")
            self._sock = None
            self._set_status("error")

    def _teardown_socket(self, status: str) -> None:
        sock, self._sock = self._sock, None
        self._subscribed.clear()
        self._resub_pending.clear()
        if sock is not None:
            try:
                sock.close_connection()
            except Exception:  # noqa: BLE001
                pass
        self._set_status(status)

    def _on_open(self) -> None:
        # Runs synchronously from sock.connect() — on our _socket_loop thread
        # for a fresh connect, on the SDK's ws thread for an auto-reconnect.
        # Keep it cheap: the SDK has just wiped its subscription tables, so
        # flag a full resubscribe and let _socket_loop do the chunked work.
        self.sock_attempts = 0
        self.sock_last_msg = time.time()
        self._subscribed.clear()
        self._resub_pending.set()
        self._set_status("connected")
        log.info("socket connected")

    def _on_message(self, msg: dict) -> None:
        if not isinstance(msg, dict):
            return
        self.sock_last_msg = time.time()
        sym = msg.get("symbol") or msg.get("s")
        if not sym:
            return
        with self._lock:
            row = self.rows.get(sym)
            if row is None:
                return
            ltp = msg.get("ltp")
            if ltp is not None:
                row.ltp = ltp
                row.last_tick = time.time()
            if row.yclose is None and msg.get("prev_close_price"):
                row.yclose = msg["prev_close_price"]
            if self._forming():
                row.s_open = msg.get("open_price") or row.s_open
                row.s_high = msg.get("high_price") or row.s_high
                row.s_low = msg.get("low_price") or row.s_low
                row.s_ltp = row.ltp
                row.s_pct = _pct(row.s_ltp, row.yclose)
            row.live_pct = _pct(row.ltp, row.s_high)

    def _on_error(self, msg) -> None:
        log.warning("socket error: %s", msg)
        self._set_status("error")

    def _on_close(self, msg) -> None:
        log.info("socket closed: %s", msg)
        if self.sock_status not in ("idle", "no-credentials"):
            self._set_status("disconnected")

    def _set_status(self, status: str) -> None:
        if status != self.sock_status:
            self.sock_status = status
            self.sock_since = time.time()

    def _sync_subscription(self) -> None:
        """Bring the socket's subscriptions in line with the universe.

        Diff-based: after a (re)connect _subscribed is empty, so this
        resubscribes the whole universe; on a universe edit it only moves the
        delta. Sent in batches with a pause between them — the SDK blocks the
        caller ~0.5s per internal frame and the server dislikes one huge burst,
        so at 2000 symbols this takes a few seconds. Runs on _socket_loop only.
        """
        sock = self._sock
        if sock is None or self.sock_status != "connected":
            return
        with self._lock:
            wanted = set(self.rows)
        add = sorted(wanted - self._subscribed)
        remove = sorted(self._subscribed - wanted)

        for action, symbols in (("unsubscribe", remove), ("subscribe", add)):
            for i in range(0, len(symbols), SUBSCRIBE_BATCH):
                if self._stop.is_set() or self._sock is not sock:
                    return
                batch = symbols[i : i + SUBSCRIBE_BATCH]
                try:
                    if action == "subscribe":
                        sock.subscribe(symbols=batch, data_type="SymbolUpdate")
                        self._subscribed |= set(batch)
                    else:
                        sock.unsubscribe(symbols=batch, data_type="SymbolUpdate")
                        self._subscribed -= set(batch)
                except Exception:  # noqa: BLE001
                    log.exception("%s batch failed (%d symbols)", action, len(batch))
                    return
                if i + SUBSCRIBE_BATCH < len(symbols):
                    self._stop.wait(SUBSCRIBE_PAUSE)
        if add or remove:
            log.info(
                "subscription synced: +%d -%d (%d live)",
                len(add), len(remove), len(self._subscribed),
            )

    # ------------------------------------------------------------------ #
    # snapshot for the API
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        now = datetime.now(IST)
        with self._lock:
            rows = [r.as_dict() for r in self.rows.values()]
        last_msg = (
            round(time.time() - self.sock_last_msg) if self.sock_last_msg else None
        )
        # which session the static columns represent
        if self.frozen_at is not None:
            data_date = self.day.isoformat() if self.day else None
        elif self._session_date:
            data_date = self._session_date.isoformat()
        elif self._data_epoch:
            data_date = datetime.fromtimestamp(self._data_epoch, IST).date().isoformat()
        else:
            data_date = None
        return {
            "server_time": now.isoformat(),
            "day": self.day.isoformat() if self.day else None,
            "data_date": data_date,
            "data_stale": bool(data_date and self.day and data_date != self.day.isoformat()),
            "freeze_time": self.freeze_time.strftime("%H:%M"),
            "frozen": self.frozen_at is not None,
            "frozen_at": self.frozen_at.isoformat() if self.frozen_at else None,
            "market_open": MARKET_OPEN <= now.time() <= MARKET_CLOSE
            and now.weekday() < 5,
            "socket": {
                "status": self.sock_status,
                "for_seconds": round(time.time() - self.sock_since),
                "last_msg_seconds": last_msg,
                "attempts": self.sock_attempts,
            },
            "universe_count": len(rows),
            "rows": rows,
        }


scanner = ScannerService()
