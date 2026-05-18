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


class Counter(Base):
    """Tiny key/value table for persistent counters (e.g. Reoon API calls)."""
    __tablename__ = "counters"
    key = Column(String(50), primary_key=True)
    value = Column(Integer, default=0, nullable=False)


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

    # SQLite doesn't auto-ALTER existing tables when models gain new columns.
    # Apply additive migrations manually here. Each is idempotent.
    from sqlalchemy import text
    migrations = [
        # responded / responded_at columns added for Instantly unibox reply tracking
        ("verified_leads", "responded", "ALTER TABLE verified_leads ADD COLUMN responded BOOLEAN DEFAULT 0"),
        ("verified_leads", "responded_at", "ALTER TABLE verified_leads ADD COLUMN responded_at DATETIME"),
        # niche + hook added for multi-niche segmentation and personalisation
        ("verified_leads", "niche", "ALTER TABLE verified_leads ADD COLUMN niche VARCHAR(80)"),
        ("verified_leads", "hook", "ALTER TABLE verified_leads ADD COLUMN hook TEXT"),
    ]
    async with engine.begin() as conn:
        for table, col, ddl in migrations:
            try:
                # Check if column exists
                r = await conn.execute(text(f"PRAGMA table_info({table})"))
                cols = {row[1] for row in r.all()}
                if col not in cols:
                    await conn.execute(text(ddl))
            except Exception:
                # Best effort — don't crash startup if migration fails
                pass
