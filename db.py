from __future__ import annotations
"""SQLite via SQLAlchemy async. Schema for URL state, raw leads, verified leads, batches."""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, Text, ForeignKey, UniqueConstraint, Index, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base

from config import settings

Base = declarative_base()


class SeenURL(Base):
    __tablename__ = "seen_urls"
    id = Column(Integer, primary_key=True)
    url = Column(String(800), unique=True, index=True, nullable=False)
    source = Column(String(50))
    status = Column(String(20))  # parsed, no_website, no_emails, error
    first_seen = Column(DateTime, default=datetime.utcnow)


class RawLead(Base):
    __tablename__ = "raw_leads"
    id = Column(Integer, primary_key=True)
    source_url = Column(String(800), index=True, nullable=False)
    source = Column(String(50))
    name = Column(String(200))
    website = Column(String(500))
    company = Column(Text)
    role = Column(String(100))
    email_candidates = Column(Text)  # JSON list
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("ix_raw_website", "website"),)


class VerifiedLead(Base):
    __tablename__ = "verified_leads"
    id = Column(Integer, primary_key=True)
    source_url = Column(String(800), nullable=False)
    source = Column(String(50))
    name = Column(String(200))
    first_name = Column(String(100))
    last_name = Column(String(100))
    website = Column(String(500))
    company = Column(Text)
    role = Column(String(100))
    email = Column(String(200), unique=True, index=True, nullable=False)
    reoon_status = Column(String(40))
    reoon_score = Column(Integer)
    is_catch_all = Column(Boolean)
    tier = Column(String(2))  # A only — Tier B is filtered out
    niche = Column(String(80))   # e.g. "Marketing Agency", "Coaching", "SaaS / Tech"
    hook = Column(Text)          # Claude-generated personalisation icebreaker sentence
    batch_id = Column(Integer, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    # Reply tracking via Instantly unibox sync
    responded = Column(Boolean, default=False, index=True)
    responded_at = Column(DateTime)
    # Bounce tracking via Instantly unibox sync
    bounced = Column(Boolean, default=False, index=True)
    bounced_at = Column(DateTime)


class Counter(Base):
    """Tiny key/value table for persistent counters (e.g. Reoon API calls)."""
    __tablename__ = "counters"
    key = Column(String(50), primary_key=True)
    value = Column(Integer, default=0, nullable=False)


class SourceWeight(Base):
    """Dynamic per-source quality weights set by the intelligence engine.
    weight > 1.0 → process more from this source first
    weight < 1.0 → deprioritise (still scraped, just later in queue)
    """
    __tablename__ = "source_weights"
    source = Column(String(100), primary_key=True)
    weight = Column(Float, default=1.0, nullable=False)
    reason = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow)


class IntelligenceReport(Base):
    """Stores each intelligence cycle's analysis and recommendations."""
    __tablename__ = "intelligence_reports"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    cycle = Column(Integer, default=0)
    niche_metrics = Column(Text)    # JSON: {niche → {total, bounced, replied, …}}
    source_metrics = Column(Text)   # JSON: {source → {total, bounced, replied, …}}
    expand_niches = Column(Text)    # JSON list of niches to grow
    reduce_niches = Column(Text)    # JSON list to deprioritise
    test_niches = Column(Text)      # JSON list of new niches to try
    source_weights = Column(Text)   # JSON: {source → weight float}
    narrative = Column(Text)        # Claude's full plain-English analysis
    top_insight = Column(Text)      # Single most important finding


class Batch(Base):
    __tablename__ = "batches"
    id = Column(Integer, primary_key=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)
    target = Column(Integer)
    delivered_count = Column(Integer, default=0)
    csv_path = Column(String(500))
    delivered_email = Column(String(200))
    status = Column(String(20), default="running")  # running, completed, failed
    trigger = Column(String(20))  # cron, manual, api
    notes = Column(Text)


engine = create_async_engine(settings.DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Additive column migrations — idempotent, safe to run on every startup.
    # Works for both SQLite (PRAGMA) and PostgreSQL (information_schema).
    from sqlalchemy import text
    migrations = [
        # responded / responded_at columns added for Instantly unibox reply tracking
        ("verified_leads", "responded", "ALTER TABLE verified_leads ADD COLUMN responded BOOLEAN DEFAULT FALSE"),
        ("verified_leads", "responded_at", "ALTER TABLE verified_leads ADD COLUMN responded_at TIMESTAMP"),
        # niche + hook added for multi-niche segmentation and personalisation
        ("verified_leads", "niche", "ALTER TABLE verified_leads ADD COLUMN niche VARCHAR(80)"),
        ("verified_leads", "hook", "ALTER TABLE verified_leads ADD COLUMN hook TEXT"),
        # bounce tracking — synced from Instantly
        ("verified_leads", "bounced", "ALTER TABLE verified_leads ADD COLUMN bounced BOOLEAN DEFAULT FALSE"),
        ("verified_leads", "bounced_at", "ALTER TABLE verified_leads ADD COLUMN bounced_at TIMESTAMP"),
    ]
    async with engine.begin() as conn:
        # Detect dialect to choose the right column-existence query
        dialect = conn.dialect.name  # "sqlite" or "postgresql"
        for table, col, ddl in migrations:
            try:
                if dialect == "sqlite":
                    r = await conn.execute(text(f"PRAGMA table_info({table})"))
                    cols = {row[1] for row in r.all()}
                else:
                    # PostgreSQL — query information_schema
                    r = await conn.execute(text(
                        "SELECT column_name FROM information_schema.columns "
                        f"WHERE table_name='{table}' AND column_name='{col}'"
                    ))
                    cols = {row[0] for row in r.all()}
                if col not in cols:
                    await conn.execute(text(ddl))
            except Exception:
                # Best effort — don't crash startup if migration fails
                pass
