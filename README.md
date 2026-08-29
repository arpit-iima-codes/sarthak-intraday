# sarthak-intraday

Screener & trading terminal. FastAPI backend, server-rendered UI.

Current scope: **login screen** — sign in to Fyers with Client ID + PIN + a
6-digit TOTP/OTP and establish a broker connection. Screener and order terminal
build on top of this.

## Layout

```
app/
  main.py       FastAPI app + routes
  config.py     settings (pydantic-settings, reads .env)
  fyers.py      Fyers API v3 login flow (async, httpx)
  session.py    broker session persisted to data/broker_session.json
  templates/    Jinja2 (base, login, dashboard)
  static/       style.css
deploy/
  sarthak-intraday.service   systemd unit (uvicorn, 2 workers, :8092)
  nginx.conf                 reverse proxy for 80.225.196.44.nip.io
  websocket_upgrade.conf     $connection_upgrade map (for future live stream)
```

## Setup

1. Create a Fyers API app at https://myapi.fyers.in/dashboard.
   Set its redirect URI to `https://80.225.196.44.nip.io/fyers/callback`.
2. Put the credentials in `.env`:
   ```
   FYERS_CLIENT_ID=ABCD1234XY-100
   FYERS_SECRET_KEY=...
   FYERS_REDIRECT_URI=https://80.225.196.44.nip.io/fyers/callback
   ```
3. `sudo systemctl restart sarthak-intraday`

## Run locally

```
./run.sh            # uvicorn --reload on 127.0.0.1:8092
```

## URLs

- App:      https://80.225.196.44.nip.io/
- Health:   https://80.225.196.44.nip.io/health
- Session:  https://80.225.196.44.nip.io/api/session

## Login flow (Fyers API v3 — OAuth2)

Fyers no longer allows programmatic OTP/TOTP login for third-party apps (the old
`vagator` endpoints reject every request with `-1025`). This uses the supported
redirect flow:

```
GET  /login              -> 303 to Fyers /api/v3/generate-authcode  (state in cookie)
     user authenticates on Fyers' hosted page (Client ID + PIN + TOTP/OTP)
GET  /fyers/callback?auth_code=...&state=...
     POST /api/v3/validate-authcode  (appIdHash + auth_code) -> access_token
     GET  /api/v3/profile            -> confirm the token
```

The access token is stored server-side in `data/broker_session.json` and reused
for every Fyers call.

## Deployment notes

- Service: `systemctl {status,restart,stop} sarthak-intraday`, logs via
  `journalctl -u sarthak-intraday -f`.
- Reachable on port 80 via nginx (ports other than 80/443/22 are firewalled).
- HTTP only for now. For HTTPS, run certbot for `80.225.196.44.nip.io` and add
  the 443 server block.
