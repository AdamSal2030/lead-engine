from __future__ import annotations
"""CSV import — the new front of the pipeline.

Replaces scraping. You export a CSV from Skrapp Lead Search / Instantly
SuperSearch (or any tool) and upload it; we:

  1. flexibly map columns (email, name, company, title, website, industry…)
  2. dedup against everything already in the portal (never re-add an email)
  3. re-verify each email with MillionVerifier — keep deliverable, drop bad,
     reject catch-all (the silent-bounce risk)
  4. niche-classify and store as a clean Tier-A lead

The emails from these tools are already verified, so MV mostly just confirms
them — but it catches the few stale/risky ones so your bounce stays near-zero.
"""
import csv
import io
import re
import asyncio
import logging
from datetime import datetime
from sqlalchemy import select
from db import SessionLocal, VerifiedLead
from pipeline import mv_verifier as mv
from pipeline.niche import classify
from config import settings

log = logging.getLogger("importer")

# Flexible header matching — normalised (lowercase, alnum only) → field.
EMAIL_COLS = {"email", "emailaddress", "workemail", "email1", "professionalemail",
              "verifiedemail", "personalemail", "emailfinder", "mostprobableemail"}
FIRST_COLS = {"firstname", "first", "givenname", "fname"}
LAST_COLS = {"lastname", "last", "surname", "familyname", "lname"}
NAME_COLS = {"name", "fullname", "contactname", "leadname"}
COMPANY_COLS = {"company", "companyname", "organization", "organisation",
                "employer", "businessname", "account"}
TITLE_COLS = {"title", "jobtitle", "role", "position", "jobposition", "seniority"}
WEBSITE_COLS = {"website", "domain", "companydomain", "companywebsite", "url",
                "websiteurl", "companyurl"}
INDUSTRY_COLS = {"industry", "niche", "sector", "category", "vertical"}
LOCATION_COLS = {"location", "country", "city", "state", "region", "geo"}

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _build_colmap(headers: list[str]) -> dict[str, int]:
    """Map our field names → column index, by fuzzy header match."""
    norm_to_idx: dict[str, int] = {}
    for i, h in enumerate(headers):
        n = _norm(h)
        if n and n not in norm_to_idx:
            norm_to_idx[n] = i

    def pick(candidates: set[str]) -> int | None:
        # exact normalized match first
        for c in candidates:
            if c in norm_to_idx:
                return norm_to_idx[c]
        # then substring (e.g. "primaryemail" contains "email")
        for n, i in norm_to_idx.items():
            if any(c in n for c in candidates):
                return i
        return None

    return {
        "email": pick(EMAIL_COLS),
        "first": pick(FIRST_COLS),
        "last": pick(LAST_COLS),
        "name": pick(NAME_COLS),
        "company": pick(COMPANY_COLS),
        "title": pick(TITLE_COLS),
        "website": pick(WEBSITE_COLS),
        "industry": pick(INDUSTRY_COLS),
        "location": pick(LOCATION_COLS),
    }


def _get(row: list[str], idx: int | None) -> str:
    if idx is None or idx >= len(row):
        return ""
    return (row[idx] or "").strip()


def parse_csv(content: bytes) -> tuple[list[dict], dict]:
    """Parse raw CSV bytes → (rows, meta). Each row dict has normalized fields."""
    text = content.decode("utf-8-sig", errors="replace")
    # Sniff delimiter (comma/semicolon/tab)
    sample = text[:4096]
    delim = ","
    try:
        delim = csv.Sniffer().sniff(sample, delimiters=",;\t").delimiter
    except Exception:
        pass
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    rows = list(reader)
    if not rows:
        return [], {"headers": [], "colmap": {}, "total_rows": 0}
    headers = rows[0]
    colmap = _build_colmap(headers)
    out = []
    for r in rows[1:]:
        if not any((c or "").strip() for c in r):
            continue
        email = _get(r, colmap["email"]).lower()
        name = _get(r, colmap["name"])
        first = _get(r, colmap["first"])
        last = _get(r, colmap["last"])
        if not name and (first or last):
            name = f"{first} {last}".strip()
        if name and not first:
            parts = name.split()
            first = parts[0] if parts else ""
            last = parts[-1] if len(parts) > 1 else ""
        out.append({
            "email": email,
            "name": name,
            "first_name": first,
            "last_name": last,
            "company": _get(r, colmap["company"]),
            "role": _get(r, colmap["title"]),
            "website": _get(r, colmap["website"]),
            "industry": _get(r, colmap["industry"]),
            "location": _get(r, colmap["location"]),
        })
    meta = {"headers": headers, "colmap": colmap, "total_rows": len(out),
            "email_column_found": colmap["email"] is not None}
    return out, meta


# MV results we treat as deliverable enough to keep. We drop invalid/disposable
# and (by default) catch-all. "unknown" is kept — source pre-verified it.
_KEEP_RESULTS = {"ok", "unknown"}
_DROP_RESULTS = {"invalid", "disposable"}


async def ingest_csv(content: bytes, source: str, verify: bool = True) -> dict:
    """Ingest a CSV upload. Returns stats dict."""
    rows, meta = parse_csv(content)
    stats = {
        "source": source,
        "total_rows": meta["total_rows"],
        "email_column_found": meta["email_column_found"],
        "no_email": 0, "bad_format": 0, "duplicates": 0,
        "verified_ok": 0, "dropped_unverifiable": 0, "catch_all_dropped": 0,
        "added": 0,
    }
    if not meta["email_column_found"]:
        stats["error"] = ("No email column detected in the CSV. Headers seen: "
                          + ", ".join(meta["headers"][:20]))
        return stats

    # Collapse to unique, well-formed emails (keep first occurrence's fields)
    seen_in_file: dict[str, dict] = {}
    for row in rows:
        e = row["email"]
        if not e:
            stats["no_email"] += 1
            continue
        if not EMAIL_RE.match(e):
            stats["bad_format"] += 1
            continue
        if e not in seen_in_file:
            seen_in_file[e] = row

    emails = list(seen_in_file.keys())
    if not emails:
        return stats

    # Dedup against the DB in one shot
    async with SessionLocal() as s:
        existing = set()
        CHUNK = 500
        for i in range(0, len(emails), CHUNK):
            batch = emails[i:i + CHUNK]
            found = (await s.execute(
                select(VerifiedLead.email).where(VerifiedLead.email.in_(batch))
            )).scalars().all()
            existing.update(found)
    new_emails = [e for e in emails if e not in existing]
    stats["duplicates"] = len(emails) - len(new_emails)

    # Verify (concurrently) + collect keepers
    sem = asyncio.Semaphore(settings.MAX_VERIFY_CONCURRENCY)

    async def check(email: str) -> tuple[str, str | None, bool]:
        """Return (email, mv_result_or_None, keep)."""
        if not verify:
            return email, None, True
        async with sem:
            res = await mv.verify(email)
        if res is None:
            # MV down / quota — trust the source (it was pre-verified)
            return email, None, True
        result = (res.get("result") or "").lower()
        # Catch-all detection: MV marks subresult/result
        if result in _DROP_RESULTS:
            return email, result, False
        # MV result "catch_all" — reject unless explicitly allowed
        if result == "catch_all" or res.get("subresult") == "catch_all":
            return email, "catch_all", settings.ACCEPT_CATCH_ALL
        return email, result, result in _KEEP_RESULTS

    results = await asyncio.gather(*[check(e) for e in new_emails])

    # Insert keepers
    to_add = []
    for email, result, keep in results:
        if not keep:
            if result == "catch_all":
                stats["catch_all_dropped"] += 1
            else:
                stats["dropped_unverifiable"] += 1
            continue
        if result == "ok":
            stats["verified_ok"] += 1
        row = seen_in_file[email]
        niche = classify(row["role"], row["company"], row["industry"], row["website"])
        to_add.append(VerifiedLead(
            source_url="csv_import",
            source=source[:50],
            name=row["name"][:200],
            first_name=row["first_name"][:100],
            last_name=row["last_name"][:100],
            website=row["website"][:500] or None,
            company=row["company"] or None,
            role=row["role"][:100] or None,
            email=email,
            reoon_status=result,
            is_catch_all=(result == "catch_all"),
            tier="A",
            niche=niche,
            hook="",
            created_at=datetime.utcnow(),
        ))

    # Commit (handle the rare race-dup with a per-row fallback)
    if to_add:
        async with SessionLocal() as s:
            s.add_all(to_add)
            try:
                await s.commit()
                stats["added"] = len(to_add)
            except Exception:
                await s.rollback()
                added = 0
                for lead in to_add:
                    async with SessionLocal() as s2:
                        s2.add(lead)
                        try:
                            await s2.commit()
                            added += 1
                        except Exception:
                            await s2.rollback()
                stats["added"] = added
    log.info(f"CSV import [{source}]: {stats}")
    return stats
