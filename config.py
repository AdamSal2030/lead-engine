from __future__ import annotations
"""Centralized config loaded from env vars."""
import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Email verifiers — MillionVerifier is primary (1000 RPM), Reoon is fallback
    MV_API_KEY: str = ""

    REOON_API_KEY: str = ""
    REOON_API_KEYS: str = ""

    # Skrapp email finder (Layer 4 — fires only when free extraction fails)
    SKRAPP_API_KEY: str = ""
    SKRAPP_ENABLED: bool = True  # auto-disables when quota exhausted; restart to retry

    # Instantly unibox integration (reply tracking)
    INSTANTLY_API_KEY: str = ""  # base64 'uuid:secret' bearer token
    INSTANTLY_SYNC_INTERVAL_MINUTES: int = 30

    # Delivery (one of these is required for email delivery)
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASS: str = ""
    DELIVERY_EMAIL: str = "sam@digitalnetworkingagency.com"

    # Runtime
    DATABASE_URL: str = "sqlite+aiosqlite:///./data/leads.db"
    DATA_DIR: str = "./data"

    # Perpetual loop — keeps running forever, one batch after another
    PERPETUAL_ENABLED: bool = True
    BATCH_SIZE: int = 500            # starting target — auto-grows from here
    BATCH_SIZE_MAX: int = 3000       # ceiling
    BATCH_SIZE_GROWTH: int = 250     # +N each successful batch (until cap)
    BETWEEN_BATCH_SECONDS: int = 5
    EXHAUSTED_RETRY_SECONDS: int = 1800  # 30 min instead of 1 hour

    # Batch persistence — keep running until target hit
    PARTIAL_BATCHES: bool = False    # False = wait for new URLs; True = finish at pool exhaustion
    BATCH_MAX_HOURS: int = 72        # safety net: deliver partial after this much wall-time
    RETRY_SITEMAP_SECONDS: int = 1800  # if pool dries mid-batch, refetch sitemaps after this

    # Backwards-compat
    DEFAULT_TARGET: int = 500
    MAX_VERIFY_CONCURRENCY: int = 30     # MV handles 1000 RPM → 30 concurrent is comfortable
    SCRAPE_CONCURRENCY: int = 20         # 30 was too aggressive — caused resource starvation
    EMAIL_FIND_CONCURRENCY: int = 5      # parallel page fetches per founder website
    SKRAPP_CONCURRENCY: int = 2          # parallel Skrapp calls — keep low to avoid 429

    # Reoon
    REOON_BASE_URL: str = "https://emailverifier.reoon.com"
    REOON_IPS: str = "104.26.8.96,104.26.9.96,172.67.75.34"

    # API auth (optional — protects /run endpoint)
    API_TOKEN: str = ""

    # Dashboard auth (HTTP Basic — protects /, /download, /leads/recent)
    DASH_USERNAME: str = ""
    DASH_PASSWORD: str = ""

    # Public URL (Railway sets RAILWAY_PUBLIC_DOMAIN automatically)
    PUBLIC_BASE_URL: str = ""


settings = Settings()
os.makedirs(settings.DATA_DIR, exist_ok=True)
