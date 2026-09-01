# sarthak-intraday

Screener & trading terminal. FastAPI backend, server-rendered UI. Independent —
no shared code with any other project; the only broker dependency is the
official `fyers-apiv3` package.

## Pages

| Path        | What |
|-------------|------|
| `/`         | Fyers OAuth login |
| `/dashboard`| account name + available/total balance |
| `/scanner`  | daily live scanner + buy-engine strip (arm/kill, P&L) in the right pane |
| `/controls` | scanner + engine controls |

## Layout

```
app/
  main.py        FastAPI app, routes, lifespan (starts the scanner)
  config.py      settings + IST market timings
  fyers.py       Fyers OAuth login + profile/funds
  marketdata.py  Fyers REST: quotes, 1-min history (httpx)
  controls.py    controls store (data/controls.json), symbol + engine settings
  scanner.py     ScannerService — websocket feed, 10:00 freeze, socket supervision
  engine.py      BuyEngine — 10:00-breakout entries, target/stop/EOD exits, paper|live
  orders.py      Fyers INTRADAY market orders + available balance (live mode)
  session.py     broker session -> data/broker_session.json
  templates/     base, login, scanner, controls
  static/        style.css, scanner.js, engine.js (right-pane strip), positions.js
deploy/
  sarthak-intraday.service   systemd unit (uvicorn, ONE worker, :8092)
  nginx.conf                 reverse proxy for 80.225.196.44.nip.io (HTTPS)
  websocket_upgrade.conf     $connection_upgrade map
```

## Login flow (Fyers API v3 — OAuth2)

Fyers no longer allows programmatic OTP/TOTP login for third-party apps (the old
`vagator` endpoints reject every request with `-1025`, and the hosted login is
behind Cloudflare). This uses the supported redirect flow:

```
GET  /login              -> 303 to Fyers /api/v3/generate-authcode  (state in cookie)
     user authenticates on Fyers' hosted page (Client ID + PIN + TOTP/OTP)
GET  /fyers/callback?auth_code=...&state=...
     POST /api/v3/validate-authcode  -> access_token
     GET  /api/v3/profile            -> confirm the token
```

Token stored server-side in `data/broker_session.json`. All Fyers REST calls use
`https://api-t1.fyers.in` (`api.fyers.in` is not a valid API host).

## Scanner

One background service in the web process (hence a single uvicorn worker) owns
one Fyers data websocket.

- **Static columns** (Symbol, Y.Close, Open, High, Low, LTP, % ↑ close): frozen
  at the configured **freeze time** (default 10:00 IST, set on `/controls`).
  Normally the running feed already has the pre-freeze values, so the freeze is
  instant regardless of universe size. A mid-day restart instead reconstructs
  the snapshot from Fyers 1-minute history — chunked and rate-limited
  (`FREEZE_RATE`), so a ~2000-symbol universe takes several minutes and
  freezes progressively (usable at ~60% coverage). Changing the freeze time
  re-captures for the new time.
- **Universe** lives in `data/controls.json`; edit on `/controls`. Currently
  ~1,160 NSE equities (the high-volume liquid set; symbols Fyers doesn't quote
  as `NSE:…-EQ` are dropped).
- **Live columns** (LTP, % ↑ 10:00-high): updated from socket ticks.
- In-memory only; resets each trading day. Nothing persisted.
- Browser gets a Server-Sent-Events stream (`/api/scanner/stream`, ~1s);
  `EventSource` auto-reconnects. `/api/scanner/state` is the one-shot JSON.
- Client-side column sort. Header shows IST clock, freeze status, and socket
  status (connected / disconnected / stalled + "Ns ago").
- Socket resilience: SDK auto-reconnect + a watchdog that rebuilds on a stale
  feed, backs off on hard errors, and re-subscribes when the universe changes.

## Buy engine

A background thread (`engine.py`) watches the scanner. When a symbol's live
price crosses **up** through its frozen 10:00 high it takes a long, sized to a
fixed rupee budget (`qty = floor(budget / ltp)`).

- **Exit styles:** `bracket` (+target% / −stop% from the fill) or `eod` (hold
  until the square-off time). Every open position is force-flattened at the
  square-off time regardless.
- **Capital:** total across open positions never exceeds the configured
  *total capital*; in **live** mode it's also capped by the real available
  balance. Never more than *max positions* at once. A breakout that can't be
  funded/seated is queued and filled when room frees up (dropped if the price
  falls back below the 10:00 high first).
- **One entry per symbol per day** — win or lose, it's done.
- **Modes:** `paper` simulates fills at the live price; `live` places Fyers
  INTRADAY market orders. Switch on `/controls`, or tap the `PAPER`/`LIVE`
  badge in the scanner strip (going live asks for confirmation).
- **Start / Kill** — one toggle button in the strip at the top of the
  `/scanner` right pane. Kill stops *new* entries only — open positions keep
  being managed to their exit. "Arm automatically at the start of the next
  trading day" on `/controls` sets the default each morning.
- Runtime state → `data/engine_state.json` (mid-day restart resumes managing
  open positions); the previous day's book → `data/engine_history/<date>.json`.

## Setup

1. Fyers API app at https://myapi.fyers.in/dashboard, redirect URI
   `https://80.225.196.44.nip.io/fyers/callback`.
2. `.env`:
   ```
   FYERS_CLIENT_ID=ABCD1234XY-100
   FYERS_SECRET_KEY=...
   FYERS_REDIRECT_URI=https://80.225.196.44.nip.io/fyers/callback
   FYERS_FY_ID=XS89030
   GATE_USER=sarthak          # front-door login screen
   GATE_PASSWORD=...          # empty = gate disabled
   ```
3. `sudo systemctl restart sarthak-intraday`

The **front-door gate** (`GATE_USER` / `GATE_PASSWORD`) is a styled
username/password screen at `/gate` shown before anything else — the app
itself has no per-user auth, every page otherwise gates only on the shared
server-side Fyers token. `/health` stays open. "Lock" in the top bar clears it.

## Run locally

```
./run.sh            # uvicorn --reload on 127.0.0.1:8092
```

## URLs

- App:     https://80.225.196.44.nip.io/
- Health:  https://80.225.196.44.nip.io/health
- Scanner: https://80.225.196.44.nip.io/api/scanner/state

## Deployment notes

- Service: `systemctl {status,restart,stop} sarthak-intraday`,
  `journalctl -u sarthak-intraday -f`.
- HTTPS via certbot (`80.225.196.44.nip.io`, auto-renew). Reachable on 443 via
  nginx; ports other than 80/443/22 are firewalled.
- Single worker is deliberate — the scanner holds in-process state and threads.
