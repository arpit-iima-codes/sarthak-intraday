# sarthak-intraday

Screener & trading terminal. FastAPI backend, server-rendered UI. Independent —
no shared code with any other project; the only broker dependency is the
official `fyers-apiv3` package.

## Pages

| Path        | What |
|-------------|------|
| `/`         | Fyers OAuth login |
| `/dashboard`| account name + available/total balance |
| `/scanner`  | daily live scanner (below) |
| `/controls` | scanner controls — symbol universe (extensible) |

## Layout

```
app/
  main.py        FastAPI app, routes, lifespan (starts the scanner)
  config.py      settings + IST market timings
  fyers.py       Fyers OAuth login + profile/funds
  marketdata.py  Fyers REST: quotes, 1-min history (httpx)
  controls.py    controls store (data/controls.json), symbol parsing + validation
  scanner.py     ScannerService — websocket feed, 10:00 freeze, socket supervision
  session.py     broker session -> data/broker_session.json
  templates/     base, login, dashboard, scanner, controls
  static/        style.css, scanner.js
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
  at 10:00 IST, computed from Fyers 1-minute history so a mid-day restart still
  reconstructs the snapshot. Before 10:00 they show the forming values.
- **Live columns** (LTP, % ↑ 10:00-high): updated from socket ticks.
- In-memory only; resets each trading day. Nothing persisted.
- Browser gets a Server-Sent-Events stream (`/api/scanner/stream`, ~1s);
  `EventSource` auto-reconnects. `/api/scanner/state` is the one-shot JSON.
- Client-side column sort. Header shows IST clock, freeze status, and socket
  status (connected / disconnected / stalled + "Ns ago").
- Socket resilience: SDK auto-reconnect + a watchdog that rebuilds on a stale
  feed, backs off on hard errors, and re-subscribes when the universe changes.

## Setup

1. Fyers API app at https://myapi.fyers.in/dashboard, redirect URI
   `https://80.225.196.44.nip.io/fyers/callback`.
2. `.env`:
   ```
   FYERS_CLIENT_ID=ABCD1234XY-100
   FYERS_SECRET_KEY=...
   FYERS_REDIRECT_URI=https://80.225.196.44.nip.io/fyers/callback
   FYERS_FY_ID=XS89030
   ```
3. `sudo systemctl restart sarthak-intraday`

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
