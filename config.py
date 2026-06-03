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

    # Skrapp niche targeting — Skrapp only spends a credit when the lead's classified
    # niche is in this set. This is how we "search Skrapp for the niches we need":
    # off-target leads still get FREE email extraction, they just never burn a credit.
    # Empty string = no gating (fire on every name+domain). Labels MUST match the
    # niche labels produced by pipeline/niche.py exactly.
    # "Founder / Startup" is the unclassified catch-all — kept in by default so we
    # don't lose volume on leads our classifier couldn't label; drop it for stricter
    # targeting.
    SKRAPP_TARGET_NICHES: str = (
        "Marketing Agency,Coaching,Consulting,Author / Speaker,Real Estate,"
        "SaaS / Tech,Creative Services,Recruiting & HR,Legal & Finance,"
        "Fitness & Wellness,Education & Training,E-commerce,Founder / Startup"
    )

    # Instantly unibox integration (reply + bounce tracking)
    # Supports multiple Instantly accounts/workspaces — bounces & replies are
    # pulled from ALL configured keys. Add more by setting INSTANTLY_API_KEY_2,
    # _3, etc. in the environment.
    INSTANTLY_API_KEY: str = ""    # base64 'uuid:secret' bearer token
    INSTANTLY_API_KEY_2: str = ""  # second account/workspace key
    INSTANTLY_API_KEY_3: str = ""  # third account/workspace key
    INSTANTLY_SYNC_INTERVAL_MINUTES: int = 30

    def instantly_keys(self) -> list[str]:
        """All configured Instantly bearer tokens, de-duped, blanks removed."""
        seen: list[str] = []
        for k in (self.INSTANTLY_API_KEY, self.INSTANTLY_API_KEY_2, self.INSTANTLY_API_KEY_3):
            k = (k or "").strip()
            if k and k not in seen:
                seen.append(k)
        return seen

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

    # Bounce control — catch-all domains are the #1 silent-bounce risk
    ACCEPT_CATCH_ALL: bool = False  # if False, verifier never accepts catch-all leads
    EXPORT_CATCH_ALL: bool = False  # if False, exports/deliveries exclude catch-all leads

    # Email guessing — when False the pipeline NEVER constructs guessed pattern
    # emails (jane@, jsmith@, jane.smith@, founder@, info@ ...). Only confirmed
    # addresses are used: article-body emails, emails actually scraped off the
    # founder's website, and finder APIs (Skrapp / Hunter). Guessed patterns are
    # the #1 bounce source, so this is OFF by default for deliverability.
    ALLOW_EMAIL_GUESSING: bool = False

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

    # LLM provider — which backend powers article parsing + daily intelligence.
    #   "claude" (default) → Anthropic API (uses ANTHROPIC_API_KEY)
    #   "ollama"           → Ollama /api/chat at LLM_BASE_URL (default localhost:11434)
    #   "openai"           → any OpenAI-compatible endpoint (Ollama OpenAI mode,
    #                        Groq, Together, OpenRouter, DeepInfra, vLLM …);
    #                        LLM_BASE_URL must include the version path (…/v1)
    # Open-model calls fall back to Claude on error when ANTHROPIC_API_KEY is set,
    # so yield never drops if the open model hiccups. The daily cap applies only
    # to the "claude" provider (open models have no per-call cost).
    LLM_PROVIDER: str = "claude"
    LLM_BASE_URL: str = ""              # e.g. http://1.2.3.4:11434  or  https://api.groq.com/openai/v1
    LLM_MODEL: str = ""                 # e.g. qwen2.5:7b  or  llama-3.1-8b-instant
    LLM_API_KEY: str = ""              # bearer token for hosted endpoints (blank for local Ollama)

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

    # Residential proxy for blocked interview networks (Voyage / ShoutOut etc.).
    # Format: http://user:pass@host:port  — set in Railway env, NEVER commit it.
    # When empty, scraping behaves exactly as before (Wayback fallback). When
    # set, only the blocked networks route through it (see pipeline/netutil.py).
    PROXY_URL: str = ""


settings = Settings()

# Railway injects DATABASE_URL as "postgresql://..." but SQLAlchemy async needs
# "postgresql+asyncpg://...". Patch the URL transparently at startup.
if settings.DATABASE_URL.startswith("postgresql://"):
    settings.DATABASE_URL = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif settings.DATABASE_URL.startswith("postgres://"):
    # Heroku/Railway legacy alias
    settings.DATABASE_URL = settings.DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

os.makedirs(settings.DATA_DIR, exist_ok=True)
