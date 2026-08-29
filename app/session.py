"""Broker session persistence.

Single-user tool: the broker token lives in one JSON file on disk, so it
survives restarts and is shared by every worker process.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .config import SESSION_FILE


def load() -> dict | None:
    if not SESSION_FILE.exists():
        return None
    try:
        return json.loads(SESSION_FILE.read_text())
    except (ValueError, OSError):
        return None


def save(data: dict) -> dict:
    record = {**data, "connected_at": datetime.now(timezone.utc).isoformat()}
    SESSION_FILE.write_text(json.dumps(record, indent=2))
    SESSION_FILE.chmod(0o600)
    return record


def clear() -> None:
    SESSION_FILE.unlink(missing_ok=True)
