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

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from datetime import time as time_cls

from fyers_apiv3.FyersWebsocket import data_ws

from . import controls, marketdata
from . import session as broker_session
from .config import DATA_DIR, FREEZE_TIME, IST, MARKET_CLOSE, MARKET_OPEN

FREEZE_FILE = DATA_DIR / "scanner_freeze.json"

log = logging.getLogger("scanner")

STALE_FEED_SECONDS = 90        # no ticks this long while connected -> rebuild
CONNECT_TIMEOUT = 20           # seconds to wait for sock.connect() before giving up
SEED_INTERVAL = 5             # seconds between pre-freeze quote refreshes
SEED_MAX_SYMBOLS = 250        # per pass (5 quote requests) - the rest next tick
SEED_BACKOFF_MAX = 120        # seconds; ceiling when the broker is throttling
FREEZE_WORKERS = 4           # parallel 1-min-history fetches during a rebuild
FREEZE_RATE = 3             # ...capped to this many history calls/sec (Fyers limit)
REBUILD_CHUNK = 60          # symbols reconstructed per scheduler pass
REBUILD_MAX_TRIES = 4       # give up on a symbol's history after this many passes
SUBSCRIBE_BATCH = 500        # symbols per subscribe/unsubscribe call
SUBSCRIBE_PAUSE = 0.3        # seconds between subscribe batches (server breathing room)


class _RateGate:
    """Spread calls across threads to at most `per_sec` per second."""

    def __init__(self, per_sec: float) -> None:
        self._min = 1.0 / per_sec
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            sleep_for = self._next - now
            self._next = max(now, self._next) + self._min
        if sleep_for > 0:
            time.sleep(sleep_for)


def momentum_settings() -> tuple[int, float]:
    """(how many to shortlist, the minimum open->freeze gain in %)."""
    cfg = controls.load()
    try:
        top = max(1, int(cfg.get("engine_momentum_top", 10)))
    except (TypeError, ValueError):
        top = 10
    try:
        floor = float(cfg.get("engine_momentum_min_pct", 5.0))
    except (TypeError, ValueError):
        floor = 5.0
    return top, floor


def _pct(value: float | None, base: float | None) -> float | None:
    if value is None or not base:
        return None
    return round((value / base - 1.0) * 100.0, 2)


class Row:
    __slots__ = (
        "symbol", "yclose",
        "s_open", "s_high", "s_low", "s_ltp",
        "ltp", "live_pct", "last_tick",
    )

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.yclose = None
        self.s_open = self.s_high = self.s_low = self.s_ltp = None
        self.ltp = self.live_pct = None
        self.last_tick = 0.0

    @property
    def s_pct(self) -> float | None:
        """Yesterday's close -> the frozen price, in %. Derived, because the
        two inputs land at different times: the static section is rebuilt from
        history while yclose arrives with a quote or a tick. Computing this at
        assignment time meant a yclose that showed up afterwards never made it
        into the number, and the column stayed blank for the session."""
        return _pct(self.s_ltp, self.yclose)

    @property
    def mom_pct(self) -> float | None:
        """Open -> freeze-high move, in %. Derived rather than stored so it is
        right whichever path filled the static section (quotes, ticks, a
        history rebuild or a restored snapshot)."""
        return _pct(self.s_high, self.s_open)

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "yclose": self.yclose,
            "s_open": self.s_open,
            "s_high": self.s_high,
            "s_low": self.s_low,
            "s_pct": self.s_pct,   # s_ltp is kept internally (feeds s_pct) but not sent
            "mom_pct": self.mom_pct,
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
        self._first_tick_ist: datetime | None = None   # first live tick today
        self._freeze_gate = _RateGate(FREEZE_RATE)
        self._rebuild: dict | None = None   # in-progress mid-day history rebuild
        self._need_refreeze = False       # freeze time moved into the past
        self._last_seed = 0.0
        self._seed_interval = SEED_INTERVAL   # grows while the broker throttles
        self._data_epoch = 0.0            # last-trade time seen in quotes/ticks
        self._session_date: date | None = None   # last session with data
        self._session_checked = 0.0

        self._sock: data_ws.FyersDataSocket | None = None
        self._subscribed: set[str] = set()
        # symbols the feed refuses (delisted / renamed) - kept out of the
        # subscription so they stop churning the socket, cleared on a universe
        # edit or a new day
        self._bad_symbols: set[str] = set()
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
        self._restore_freeze()
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
            # a corrected ticker deserves another try
            self._bad_symbols.clear()
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
        Once frozen (frozen_at set) — or once a mid-day history rebuild has
        started — the static columns are locked and never take live ticks."""
        return self.frozen_at is None and self._rebuild is None

    def _ensure_day(self) -> None:
        today = datetime.now(IST).date()
        with self._lock:
            if self.day == today:
                return
            self.day = today
            self.frozen_at = None
            self._rebuild = None
            self._first_tick_ist = None
            self._last_seed = 0.0
            self._seed_interval = SEED_INTERVAL
            self._data_epoch = 0.0
            self._session_date = None
            self._session_checked = 0.0
            self._bad_symbols.clear()   # re-test yesterday's rejects
            self.rows = {sym: Row(sym) for sym in self.universe}
        # yesterday's snapshot is stale now
        try:
            FREEZE_FILE.unlink(missing_ok=True)
        except OSError:
            pass
        log.info("new trading day %s - state reset", today)

    # ------------------------------------------------------------------ #
    # frozen-snapshot persistence — the 10:00 values never change, so a
    # restart should reload them, not re-fetch ~1000 histories.
    # ------------------------------------------------------------------ #
    def _save_freeze(self) -> None:
        with self._lock:
            if self.frozen_at is None or self.day is None:
                return
            static = {
                s: [r.s_open, r.s_high, r.s_low, r.s_ltp]
                for s, r in self.rows.items()
                if r.s_high is not None
            }
            if not static:
                return   # nothing worth persisting (don't clobber a good file)
            payload = {
                "day": self.day.isoformat(),
                "freeze_time": self.freeze_time.strftime("%H:%M"),
                "frozen_at": self.frozen_at.isoformat(),
                "static": static,
            }
        try:
            tmp = FREEZE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload))
            os.replace(tmp, FREEZE_FILE)
        except OSError:
            log.warning("freeze snapshot save failed", exc_info=True)

    def _restore_freeze(self) -> None:
        """Reload today's frozen static section from disk, if the freeze time
        still matches. Any universe symbols missing from it are queued for a
        (small) history rebuild."""
        if not FREEZE_FILE.exists():
            return
        try:
            data = json.loads(FREEZE_FILE.read_text())
        except (ValueError, OSError):
            return
        today = datetime.now(IST).date()
        if data.get("day") != today.isoformat():
            return
        if data.get("freeze_time") != self.freeze_time.strftime("%H:%M"):
            return
        static = data.get("static") or {}
        restored = 0
        with self._lock:
            for sym, vals in static.items():
                row = self.rows.get(sym)
                if row and isinstance(vals, list) and len(vals) == 4:
                    row.s_open, row.s_high, row.s_low, row.s_ltp = vals
                    restored += 1
            if not restored:
                return
            self.day = today
            try:
                self.frozen_at = datetime.fromisoformat(data["frozen_at"])
            except (KeyError, ValueError, TypeError):
                self.frozen_at = datetime.now(IST)
            missing = [s for s, r in self.rows.items() if r.s_high is None]
            if missing:
                self._rebuild = {
                    "todo": missing, "tries": {}, "ok": restored, "t0": time.time(),
                }
        log.info(
            "static section restored from snapshot: %d symbols (frozen %s)%s",
            restored, data.get("frozen_at"),
            f", {len(missing)} to reconstruct" if missing else "",
        )

    def _seed_from_quotes(self) -> None:
        """Fill yesterday's close and, before the freeze, the forming OHLC."""
        now = time.time()
        if now - self._last_seed < self._seed_interval:
            return
        # Only do the heavy full-universe seed while genuinely pre-freeze; once
        # the freeze time has passed just top up any missing yesterday-close
        # (a post-freeze rebuild reconstructs OHLC from history, not quotes).
        forming = self._forming() and datetime.now(IST).time() < self.freeze_time
        with self._lock:
            if forming:
                need = list(self.rows)
            else:
                need = [s for s, r in self.rows.items() if r.yclose is None]
        if not need:
            self._last_seed = now
            self._seed_interval = SEED_INTERVAL
            return
        auth = self._auth()
        if not auth:
            return
        # A whole universe is 20+ requests back to back, which is what trips the
        # broker's rate limit. Take a slice per pass; the rest follow on later
        # ticks, since a symbol stays in `need` until its yclose lands.
        need = need[:SEED_MAX_SYMBOLS]
        # Stamp the attempt *before* the call: the old code returned early on
        # failure without stamping, so a throttled sweep was retried on the very
        # next 3s tick - which kept it throttled indefinitely.
        self._last_seed = now
        try:
            quotes = marketdata.quotes(auth, need)
        except marketdata.RateLimited as exc:
            self._seed_interval = min(SEED_BACKOFF_MAX, max(SEED_INTERVAL, self._seed_interval) * 2)
            log.warning("seed quotes throttled (%s) - backing off to %ds",
                        exc, self._seed_interval)
            return
        except marketdata.MarketDataError as exc:
            self._seed_interval = min(SEED_BACKOFF_MAX, max(SEED_INTERVAL, self._seed_interval) * 2)
            log.warning("seed quotes failed: %s - retrying in %ds", exc, self._seed_interval)
            return
        self._seed_interval = SEED_INTERVAL
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
                row.live_pct = _pct(row.ltp, row.s_high)

    def _freeze_one(self, auth: str, sym: str, cutoff: int):
        """Fetch one symbol's pre-freeze OHLC. Returns (sym, (o,h,l,c)) or (sym, None).

        One quick retry only — the caller re-queues persistent failures across
        passes, which spreads load better when Fyers is rate-limiting.
        """
        candles = None
        for attempt in range(2):
            self._freeze_gate.wait()
            try:
                candles = marketdata.history_1m(auth, sym, self.day)
                break
            except marketdata.MarketDataError:
                if attempt == 1:
                    return sym, None
                time.sleep(0.6)
        pre = [c for c in (candles or []) if c and c[0] < cutoff]
        if not pre:
            return sym, None
        return sym, (
            pre[0][1],
            max(c[2] for c in pre),
            min(c[3] for c in pre),
            pre[-1][4],
        )

    def _maybe_freeze(self) -> None:
        now = datetime.now(IST)
        freeze_time = self.freeze_time
        # keep going while a mid-day history rebuild is still draining
        if self.frozen_at is not None and not self._need_refreeze and self._rebuild is None:
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

        # Fast path: if the live feed was already flowing before today's freeze
        # time, the forming static section IS the pre-freeze snapshot — lock it
        # in, no per-symbol history (matters a lot for a big universe). Needs
        # the *feed* (not just the process) up before the freeze — a token that
        # only started working after 10:00 would give post-10:00 highs.
        freeze_dt = datetime.combine(now.date(), freeze_time, tzinfo=IST)
        feed_before_freeze = (
            self._first_tick_ist is not None and self._first_tick_ist < freeze_dt
        )
        if (
            self._rebuild is None
            and not self._need_refreeze
            and feed_before_freeze
        ):
            with self._lock:
                have = sum(1 for r in self.rows.values() if r.s_high is not None)
                if have:
                    for row in self.rows.values():
                        row.live_pct = _pct(row.ltp, row.s_high)
                    self.frozen_at = datetime.now(IST)
            if have:
                log.info(
                    "static section frozen at %s (live snapshot, %d/%d symbols)",
                    self.frozen_at.strftime("%H:%M:%S"), have, len(symbols),
                )
                self._save_freeze()
                return
            # nothing forming (socket was down / holiday) -> fall through to history

        # A live feed is proof the token works. If it's been dead a while, a
        # history rebuild would just fail every call (expired token) — defer
        # starting one until the feed is back rather than burn attempts.
        if (
            self._rebuild is None
            and self.sock_last_msg
            and time.time() - self.sock_last_msg > 120
        ):
            return

        # Mid-day rebuild: reconstruct the pre-freeze OHLC from 1-min history,
        # a chunk per pass, rate-limited, so a ~2000-symbol universe doesn't
        # blow the Fyers limit. Progressively usable — freeze once most are in.
        self._rebuild_step(auth, symbols, cutoff)

    def _rebuild_step(self, auth: str, symbols: list[str], cutoff: int) -> None:
        if self._rebuild is None:
            self._rebuild = {"todo": list(symbols), "tries": {}, "ok": 0, "t0": time.time()}
            log.info("static rebuild started for %d symbols (history)", len(symbols))
        rb = self._rebuild
        chunk = rb["todo"][:REBUILD_CHUNK]
        rb["todo"] = rb["todo"][REBUILD_CHUNK:]
        if not chunk:
            return

        computed: dict[str, tuple] = {}
        with ThreadPoolExecutor(max_workers=FREEZE_WORKERS) as ex:
            futures = [ex.submit(self._freeze_one, auth, s, cutoff) for s in chunk]
            for fut in as_completed(futures):
                if self._stop.is_set():
                    ex.shutdown(cancel_futures=True)
                    return
                sym, val = fut.result()
                if val is not None:
                    computed[sym] = val

        with self._lock:
            for sym, val in computed.items():
                row = self.rows.get(sym)
                if row is None:
                    continue
                row.s_open, row.s_high, row.s_low, row.s_ltp = val
                row.live_pct = _pct(row.ltp, row.s_high)
        rb["ok"] += len(computed)

        for sym in chunk:
            if sym in computed:
                continue
            rb["tries"][sym] = rb["tries"].get(sym, 0) + 1
            if rb["tries"][sym] < REBUILD_MAX_TRIES:
                rb["todo"].append(sym)

        covered = rb["ok"]
        total = len(symbols)
        if self.frozen_at is None and covered >= 0.6 * total:
            with self._lock:
                self.frozen_at = datetime.now(IST)
            log.info("static section frozen (rebuild %d/%d, still filling)", covered, total)

        if not rb["todo"]:
            elapsed = time.time() - rb["t0"]
            if covered < 0.5 * total:
                # near-total failure — almost always an expired Fyers token or a
                # dead feed. Don't "freeze" on nothing; drop the attempt and let
                # the next pass retry once the token / feed is back.
                log.warning(
                    "static rebuild aborted: only %d/%d reconstructed in %.0fs "
                    "(Fyers token expired or history unavailable?) - will retry",
                    covered, total, elapsed,
                )
                self._rebuild = None
                return
            with self._lock:
                if self.frozen_at is None:
                    self.frozen_at = datetime.now(IST)
                self._need_refreeze = False
            log.info("static rebuild complete: %d/%d in %.0fs", covered, total, elapsed)
            self._rebuild = None

        # persist progress so a restart resumes instead of re-fetching
        if self.frozen_at is not None:
            self._save_freeze()

    def _recompute_live(self) -> None:
        with self._lock:
            for row in self.rows.values():
                row.live_pct = _pct(row.ltp, row.s_high)

    # ------------------------------------------------------------------ #
    # websocket
    # ------------------------------------------------------------------ #
    def _socket_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._socket_tick()
            except Exception:  # noqa: BLE001 - an uncaught exception here used
                # to kill this thread for good: sock_status froze wherever it
                # was (once for 40+ hours, attempts stuck at 0) since nothing
                # ever ran again to notice or recover.
                log.exception("socket loop tick failed")
                self._set_status("error")
            self._stop.wait(3)

    def _socket_tick(self) -> None:
        now = datetime.now(IST)
        in_session = now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE
        auth = self._auth()

        if auth is None:
            self._teardown_socket("no-credentials")
            self._stop.wait(5)
            return
        if not in_session:
            self._teardown_socket("idle")
            self._stop.wait(20)
            return

        if self._sock is None:
            self._open_socket(auth)
        elif (
            self.sock_status == "connected"
            and self.sock_last_msg
            and time.time() - self.sock_last_msg > STALE_FEED_SECONDS
        ):
            log.warning("feed stale for %ds - rebuilding socket", STALE_FEED_SECONDS)
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
            # sock.connect() validates the token against Fyers over HTTP before
            # ever spawning its websocket thread, with no timeout of its own -
            # a hung/unresponsive call there has previously frozen this whole
            # loop for 40+ hours (attempts stuck at 0, status stuck forever,
            # since nothing after this line ever ran again). Bound it: if it
            # hasn't returned in CONNECT_TIMEOUT, give up on *this* attempt and
            # let the loop retry: a slow-but-alive call may still complete and
            # fire on_connect late (a stray, self-correcting on the next
            # teardown) rather than wedging the feed indefinitely.
            done = threading.Event()

            def _do_connect() -> None:
                try:
                    sock.connect()
                finally:
                    done.set()

            threading.Thread(target=_do_connect, name="ws_connect", daemon=True).start()
            if not done.wait(timeout=CONNECT_TIMEOUT):
                log.warning(
                    "socket connect() did not return within %ss - abandoning this "
                    "attempt, will retry", CONNECT_TIMEOUT,
                )
                self._set_status("error")
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
        if self._first_tick_ist is None:
            self._first_tick_ist = datetime.now(IST)
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
            row.live_pct = _pct(row.ltp, row.s_high)

    @staticmethod
    def _invalid_symbols(msg) -> list[str]:
        """Symbols the server refused, from a -300 'valid symbol' complaint."""
        if not isinstance(msg, dict):
            return []
        bad = msg.get("invalid_symbols")
        if isinstance(bad, str):
            bad = [bad]
        return [str(s) for s in bad] if isinstance(bad, list) else []

    def _on_error(self, msg) -> None:
        bad = self._invalid_symbols(msg)
        if bad:
            # The server rejecting a few tickers is NOT a connection failure.
            # Treating it as one tore down a healthy socket on every resubscribe
            # and reopened it, so the feed never lived long enough to deliver a
            # tick. Drop them instead and keep the connection. A universe edit
            # or a new day clears the list, so a corrected symbol is retried.
            with self._lock:
                fresh = [s for s in bad if s not in self._bad_symbols]
                self._bad_symbols.update(bad)
                for sym in bad:
                    self.rows.pop(sym, None)
                    self._subscribed.discard(sym)
            if fresh:
                log.warning(
                    "dropping %d symbol(s) the feed rejected: %s",
                    len(fresh), ", ".join(sorted(fresh)),
                )
            return
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
            wanted = set(self.rows) - self._bad_symbols
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
    # cheap views for the buy engine
    # ------------------------------------------------------------------ #
    @property
    def is_frozen(self) -> bool:
        return self.frozen_at is not None

    def momentum_universe(self) -> list[str]:
        """Today's tradable shortlist: the strongest movers from the open into
        the freeze, best first.

        Only symbols that gained at least ``engine_momentum_min_pct`` between
        the day's open and the frozen 10:00 high qualify, and only the top
        ``engine_momentum_top`` of those. Empty until the static section is
        frozen - before that the high is still forming and a ranking off it
        would be noise.
        """
        if self.frozen_at is None:
            return []
        top, floor = momentum_settings()
        with self._lock:
            ranked = [
                (r.mom_pct, s) for s, r in self.rows.items()
                if r.mom_pct is not None and r.mom_pct >= floor
            ]
        # symbol breaks a tie, so the shortlist is stable across reads
        ranked.sort(key=lambda t: (-t[0], t[1]))
        return [s for _, s in ranked[:top]]

    def quote_map(self) -> dict[str, tuple[float | None, float | None]]:
        """{symbol: (frozen 10:00 high, live LTP)} — one cheap locked read."""
        with self._lock:
            return {s: (r.s_high, r.ltp) for s, r in self.rows.items()}

    # ------------------------------------------------------------------ #
    # snapshot for the API
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        now = datetime.now(IST)
        with self._lock:
            rows = [r.as_dict() for r in self.rows.values()]
            rb = self._rebuild
            rebuild = (
                {
                    "done": rb["ok"],
                    "total": len(self.rows),
                    "elapsed_s": round(time.time() - rb["t0"]),
                }
                if rb is not None
                else None
            )
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
        mom_top, mom_floor = momentum_settings()
        return {
            "server_time": now.isoformat(),
            "day": self.day.isoformat() if self.day else None,
            "data_date": data_date,
            "data_stale": bool(data_date and self.day and data_date != self.day.isoformat()),
            "freeze_time": self.freeze_time.strftime("%H:%M"),
            "frozen": self.frozen_at is not None,
            "frozen_at": self.frozen_at.isoformat() if self.frozen_at else None,
            # present only while a mid-day history reconstruction is running;
            # None once it's finished (or was never needed today)
            "rebuild": rebuild,
            "market_open": MARKET_OPEN <= now.time() <= MARKET_CLOSE
            and now.weekday() < 5,
            "socket": {
                "status": self.sock_status,
                "for_seconds": round(time.time() - self.sock_since),
                "last_msg_seconds": last_msg,
                "attempts": self.sock_attempts,
            },
            "universe_count": len(rows),
            # the engine's tradable shortlist, best first - the Momentum view
            # renders exactly this, so screen and engine can never disagree
            "momentum": self.momentum_universe(),
            "momentum_top": mom_top,
            "momentum_min_pct": mom_floor,
            "rows": rows,
        }


scanner = ScannerService()
