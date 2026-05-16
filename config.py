from __future__ import annotations
"""Centralized config loaded from env vars."""
import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Required
    REOON_API_KEY: str = ""

    # Delivery (one of these is required for email delivery)
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASS: str = ""
    DELIVERY_EMAIL: str = "sam@digitalnetworkingagency.com"

    # Runtime
    DATABASE_URL: str = "sqlite+aiosqlite:///./data/leads.db"
    DATA_DIR: str = "./data"

    # Scheduling
    CRON_ENABLED: bool = True
    CRON_DAY: str = "mon"  # weekday
    CRON_HOUR: int = 6     # 06:00 UTC

    # Batch sizing
    DEFAULT_TARGET: int = 2000   # fresh Tier A leads per scheduled batch
    MAX_VERIFY_CONCURRENCY: int = 3
    SCRAPE_CONCURRENCY: int = 6

    # Reoon
    REOON_BASE_URL: str = "https://emailverifier.reoon.com"
    REOON_IPS: str = "104.26.8.96,104.26.9.96,172.67.75.34"

    # API auth (optional — protects /run endpoint)
    API_TOKEN: str = ""

    # Public URL (Railway sets RAILWAY_PUBLIC_DOMAIN automatically)
    PUBLIC_BASE_URL: str = ""


settings = Settings()
os.makedirs(settings.DATA_DIR, exist_ok=True)
