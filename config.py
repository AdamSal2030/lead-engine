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
    SKRAPP_DAILY_CAP: int = 3000  # hard per-day call ceiling (safety rail; 0 = unlimited)

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
    BATCH_SIZE: int = 2000           # target 2000 verified leads per batch
    BATCH_SIZE_MAX: int = 5000       # ceiling
    BATCH_SIZE_GROWTH: int = 500     # +N each successful batch (until cap)
    BETWEEN_BATCH_SECONDS: int = 5
    EXHAUSTED_RETRY_SECONDS: int = 180   # 3 min — re-check sitemaps for new articles

    # Batch persistence — keep running until target hit
    PARTIAL_BATCHES: bool = True     # deliver partial batch when pool is empty rather than waiting
    BATCH_MAX_HOURS: int = 6         # deliver partial after this many hours regardless
    RETRY_SITEMAP_SECONDS: int = 180  # wait 3 min then re-fetch sitemaps mid-batch
    MAX_WAIT_ITERATIONS: int = 3     # break after 3 × 3min = 9min total waiting

    # Backwards-compat
    DEFAULT_TARGET: int = 2000
    MAX_VERIFY_CONCURRENCY: int = 40     # MV handles 1000 RPM → 40 concurrent fine
    SCRAPE_CONCURRENCY: int = 20         # raised from 15 for throughput — still within 512MB
    EMAIL_FIND_CONCURRENCY: int = 6      # parallel page fetches per founder website
    SKRAPP_CONCURRENCY: int = 6          # parallel Skrapp calls

    # Reoon
    REOON_BASE_URL: str = "https://emailverifier.reoon.com"
    REOON_IPS: str = "104.26.8.96,104.26.9.96,172.67.75.34"

    # Claude API — powers smart article parsing + niche/hook extraction
    ANTHROPIC_API_KEY: str = ""
    CLAUDE_PARSE_ENABLED: bool = True   # use Claude as fallback when regex parser fails
    CLAUDE_MAX_PER_DAY: int = 5000      # daily cap on Haiku calls (~$0.30/day at 5000 calls)

    # Hunter.io — Layer 5 email finder (after Skrapp)
    HUNTER_API_KEY: str = ""
    HUNTER_ENABLED: bool = True

    # API auth (optional — protects /run endpoint)
    API_TOKEN: str = ""

    # Dashboard auth (HTTP Basic — protects /, /download, /leads/recent)
    DASH_USERNAME: str = ""
    DASH_PASSWORD: str = ""

    # Public URL (Railway sets RAILWAY_PUBLIC_DOMAIN automatically)
    PUBLIC_BASE_URL: str = ""


settings = Settings()

# Railway injects DATABASE_URL as "postgresql://..." but SQLAlchemy async needs
# "postgresql+asyncpg://...". Patch the URL transparently at startup.
if settings.DATABASE_URL.startswith("postgresql://"):
    settings.DATABASE_URL = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif settings.DATABASE_URL.startswith("postgres://"):
    # Heroku/Railway legacy alias
    settings.DATABASE_URL = settings.DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

os.makedirs(settings.DATA_DIR, exist_ok=True)
