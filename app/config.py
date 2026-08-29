"""Application settings, loaded from environment / .env."""

from __future__ import annotations

import secrets
from datetime import time as dtime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic_settings import BaseSettings, SettingsConfigDict

IST = ZoneInfo("Asia/Kolkata")

# Scanner timing (IST)
MARKET_OPEN = dtime(9, 15)
FREEZE_TIME = dtime(10, 0)     # static section is frozen at this time
MARKET_CLOSE = dtime(15, 30)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


def _load_or_create_secret() -> str:
    f = DATA_DIR / "session_secret"
    if f.exists():
        return f.read_text().strip()
    value = secrets.token_hex(32)
    f.write_text(value)
    f.chmod(0o600)
    return value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_prefix="",
        extra="ignore",
    )

    # Web
    host: str = "127.0.0.1"
    port: int = 8092
    public_url: str = "https://80.225.196.44.nip.io"

    # Fyers app credentials (from https://myapi.fyers.in/dashboard)
    fyers_client_id: str = ""
    fyers_secret_key: str = ""
    fyers_redirect_uri: str = ""

    # Your Fyers login ID (e.g. "XA12345"). When set, the login form only
    # asks for PIN + TOTP/OTP.
    fyers_fy_id: str = ""

    @property
    def redirect_uri(self) -> str:
        return self.fyers_redirect_uri or f"{self.public_url}/fyers/callback"

    @property
    def creds_ready(self) -> bool:
        return bool(self.fyers_client_id and self.fyers_secret_key)

    @property
    def fy_id_ready(self) -> bool:
        return bool(self.fyers_fy_id)


@lru_cache
def get_settings() -> Settings:
    return Settings()


SESSION_SECRET = _load_or_create_secret()
SESSION_FILE = DATA_DIR / "broker_session.json"
CONTROLS_FILE = DATA_DIR / "controls.json"
