from __future__ import annotations
"""Resolve company name → domain using Clearbit's free autocomplete API.
No auth required. Used when sources give us a company name but no website."""
import asyncio
import httpx
import logging
import urllib.parse
import re
from sqlalchemy import select, insert
from db import SessionLocal, Base
from sqlalchemy import Column, Integer, String

log = logging.getLogger("company_resolver")


class CompanyDomainCache(Base):
    """Cache to avoid repeated Clearbit calls (free but rate-limited)."""
    __tablename__ = "company_domain_cache"
    id = Column(Integer, primary_key=True)
    query_key = Column(String(300), unique=True, index=True, nullable=False)
    domain = Column(String(200))  # empty if no match


def _key(name: str) -> str:
    return re.sub(r"\s+", " ", name.lower().strip())[:280]


async def _cache_get(name: str):
    async with SessionLocal() as s:
        row = (await s.execute(
            select(CompanyDomainCache).where(CompanyDomainCache.query_key == _key(name))
        )).scalar_one_or_none()
        return row


async def _cache_save(name: str, domain: str | None):
    try:
        async with SessionLocal() as s:
            s.add(CompanyDomainCache(query_key=_key(name), domain=domain or ""))
            await s.commit()
    except Exception:
        pass


_BAD_DOMAINS = {"clearbit.com", "google.com", "facebook.com", "linkedin.com",
                "twitter.com", "x.com", "instagram.com", "youtube.com"}


async def resolve_to_domain(company_name: str) -> str | None:
    """Return a domain for the company, or None if not confidently resolvable."""
    if not company_name or len(company_name) < 3:
        return None
    company_name = company_name.strip()

    cached = await _cache_get(company_name)
    if cached:
        return cached.domain or None

    try:
        async with httpx.AsyncClient(timeout=8) as cli:
            r = await cli.get(
                "https://autocomplete.clearbit.com/v1/companies/suggest",
                params={"query": company_name},
                headers={"User-Agent": "Mozilla/5.0"},
            )
        if r.status_code != 200:
            await _cache_save(company_name, None)
            return None
        data = r.json()
        if not data:
            await _cache_save(company_name, None)
            return None

        # Find best match — prefer exact-ish name match
        cn_lower = company_name.lower()
        best = data[0]  # default: top result
        for c in data[:5]:
            n = (c.get("name") or "").lower()
            if n == cn_lower or n.startswith(cn_lower) or cn_lower in n:
                best = c
                break

        domain = best.get("domain", "").lower()
        if not domain or domain in _BAD_DOMAINS:
            await _cache_save(company_name, None)
            return None

        await _cache_save(company_name, domain)
        return domain
    except Exception as e:
        log.debug(f"clearbit resolve fail '{company_name}': {e}")
        return None
