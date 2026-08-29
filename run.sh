#!/usr/bin/env bash
# Local dev server with autoreload. Production runs via the systemd unit.
cd "$(dirname "$0")"
exec .venv/bin/uvicorn app.main:app --host "${HOST:-127.0.0.1}" --port "${PORT:-8092}" --reload
