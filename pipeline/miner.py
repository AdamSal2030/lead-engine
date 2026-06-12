from __future__ import annotations
"""Team-Page Miner — turn the domains we already own into {name, domain} tuples.

We hold ~30k company domains (raw + verified leads). Each company's /about,
/team, /leadership page LISTS its founders/owners/execs by name. We:

  1. fetch those pages (scraper + optional proxy)
  2. extract people via structured/regex FIRST (schema.org Person, "Name, Title")
     — fast, free, no LLM — and fall back to the configured LLM (your Ollama)
     only on messy pages
  3. dedupe against people we already have
  4. Skrapp-find each new person's real email → MV-verify → store

This is the multiplier: one domain → several decision-makers → several Skrapp
lookups, all from data we already own. Burns credits, produces clean leads.
"""
import asyncio
import json
import logging
import re
import urllib.parse
import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select, func
from db import SessionLocal, RawLead, VerifiedLead
from pipeline import finder as skrapp
from pipeline.importer import ingest_rows
from pipeline.niche import classify
from pipeline.netutil import proxy_client_kwargs
from config import settings

log = logging.getLogger("miner")

TEAM_PATHS = ["", "/about", "/about-us", "/team", "/our-team", "/leadership",
              "/company/team", "/meet-the-team", "/people", "/staff", "/contact"]

ROLE_WORDS = ("founder", "co-founder", "cofounder", "ceo", "owner", "president",
              "principal", "partner", "managing director", "chief", "director",
              "head of", "vp ", "vice president", "coo", "cto", "cmo", "cfo",
              "managing partner", "creative director", "proprietor")

# A plausible person name: 2-3 capitalised alphabetic words.
NAME_RE = re.compile(r"\b([A-Z][a-z'’]{1,19})\s+([A-Z][a-z'’]{1,19})(?:\s+([A-Z][a-z'’]{1,19}))?\b")

_BAD_NAME_TOKENS = {
    "The", "Our", "Your", "We", "Us", "Team", "About", "Home", "Contact", "Privacy",
    "Terms", "Service", "Services", "Company", "Read", "More", "Learn", "Get", "Meet",
    "All", "Rights", "Reserved", "Copyright", "Cookie", "Policy", "Menu", "Search",
    "View", "See", "Click", "Email", "Phone", "Call", "Free", "New", "Best", "Top",
    "Why", "How", "What", "When", "Where", "Sign", "Log", "Book", "Schedule",
}

_miner_progress: dict = {"running": False, "domains_done": 0, "people_found": 0,
                         "skrapp_hits": 0, "added": 0, "target_domains": 0,
                         "done": False, "msg": ""}
_stop_flag = {"stop": False}


def get_progress() -> dict:
    return dict(_miner_progress)


def request_stop():
    _stop_flag["stop"] = True


def _domain_of(website: str) -> str:
    if not website:
        return ""
    w = website if website.startswith("http") else "https://" + website
    try:
        return urllib.parse.urlparse(w).netloc.replace("www.", "").lower()
    except Exception:
        return ""


def _valid_name(first: str, last: str) -> bool:
    if not first or not last:
        return False
    if first in _BAD_NAME_TOKENS or last in _BAD_NAME_TOKENS:
        return False
    if len(first) < 2 or len(last) < 2:
        return False
    if not first[0].isupper() or not last[0].isupper():
        return False
    return True


def _extract_jsonld_people(html: str) -> list[dict]:
    """Pull schema.org Person objects (name + jobTitle)."""
    people = []
    for m in re.finditer(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html,
                         re.DOTALL | re.IGNORECASE):
        try:
            data = json.loads(m.group(1).strip())
        except Exception:
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
            elif isinstance(node, dict):
                t = node.get("@type", "")
                t = t if isinstance(t, str) else (t[0] if isinstance(t, list) and t else "")
                if t == "Person" and node.get("name"):
                    nm = str(node["name"]).strip()
                    parts = nm.split()
                    if len(parts) >= 2 and _valid_name(parts[0], parts[-1]):
                        people.append({"name": nm, "first": parts[0], "last": parts[-1],
                                       "role": str(node.get("jobTitle") or "").strip()})
                for v in node.values():
                    if isinstance(v, (dict, list)):
                        stack.append(v)
    return people


def _extract_text_people(html: str) -> list[dict]:
    """Heuristic: capitalised 2-word names sitting next to a role keyword."""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "noscript"]):
        tag.decompose()
    text = soup.get_text("\n")
    people = []
    seen = set()
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    for i, ln in enumerate(lines):
        if len(ln) > 120:
            continue
        low = ln.lower()
        # window: this line + neighbours (name and role often adjacent)
        window = " ".join(lines[max(0, i - 1):i + 2]).lower()
        if not any(r in window for r in ROLE_WORDS):
            continue
        for mt in NAME_RE.finditer(ln):
            first, last = mt.group(1), mt.group(3) or mt.group(2)
            if not _valid_name(first, last):
                continue
            key = (first.lower(), last.lower())
            if key in seen:
                continue
            seen.add(key)
            # role = nearest role word in the window
            role = next((r for r in ROLE_WORDS if r in window), "")
            people.append({"name": f"{first} {last}", "first": first, "last": last,
                           "role": role.title().strip()})
    return people


_LLM_SYS = ("Extract the real PEOPLE (founders, owners, executives, team members) named on "
            "this company page. Return ONLY a JSON array like "
            '[{"name":"Jane Smith","role":"Founder"}]. Names must be real human names '
            "(first + last). If none, return [].")


async def _extract_llm(text: str) -> list[dict]:
    """LLM fallback (uses settings.LLM_PROVIDER — your Ollama when configured)."""
    try:
        from pipeline.llm import chat_json
        raw = await chat_json(_LLM_SYS, text[:4000], claude_model="claude-haiku-4-5-20251001",
                              max_tokens=300, allow_claude_fallback=False)
        if not raw:
            return []
        raw = raw.strip()
        if "```" in raw:
            raw = raw.split("```")[1].lstrip("json").strip()
        arr = json.loads(raw)
        out = []
        for p in arr if isinstance(arr, list) else []:
            nm = str(p.get("name") or "").strip()
            parts = nm.split()
            if len(parts) >= 2 and _valid_name(parts[0], parts[-1]):
                out.append({"name": nm, "first": parts[0], "last": parts[-1],
                            "role": str(p.get("role") or "").strip()})
        return out
    except Exception:
        return []


async def _fetch(url: str) -> str | None:
    pkw = proxy_client_kwargs(url)
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; LeadBot/1.0)"}, **pkw) as cli:
            r = await cli.get(url)
            if r.status_code == 200 and "text/html" in r.headers.get("content-type", "").lower():
                return r.text
    except Exception:
        pass
    return None


async def mine_domain(domain: str, use_llm: bool) -> list[dict]:
    """Fetch a domain's team pages and return de-duped people [{first,last,role}]."""
    base = f"https://{domain}"
    htmls = await asyncio.gather(*[_fetch(base + p) for p in TEAM_PATHS[:6]])
    people: dict[tuple, dict] = {}
    combined_text = ""
    for html in htmls:
        if not isinstance(html, str):
            continue
        for p in _extract_jsonld_people(html) + _extract_text_people(html):
            people[(p["first"].lower(), p["last"].lower())] = p
        combined_text += BeautifulSoup(html, "lxml").get_text(" ")[:2000]
    # LLM fallback only when structured/regex found nothing and we have content
    if not people and use_llm and combined_text.strip():
        for p in await _extract_llm(combined_text):
            people[(p["first"].lower(), p["last"].lower())] = p
    return list(people.values())


async def _all_domains() -> list[str]:
    async with SessionLocal() as s:
        raw = (await s.execute(select(RawLead.website).where(RawLead.website.isnot(None)))).scalars().all()
        ver = (await s.execute(select(VerifiedLead.website).where(VerifiedLead.website.isnot(None)))).scalars().all()
    seen = set()
    out = []
    for w in list(raw) + list(ver):
        d = _domain_of(w)
        if d and "." in d and d not in seen and len(d) < 60:
            seen.add(d)
            out.append(d)
    return out


async def _existing_people() -> set:
    """(first, last, domain) we already have — so we never re-Skrapp them."""
    async with SessionLocal() as s:
        rows = (await s.execute(select(VerifiedLead.first_name, VerifiedLead.last_name,
                                       VerifiedLead.website))).all()
    out = set()
    for f, l, w in rows:
        d = _domain_of(w)
        if f and l and d:
            out.add((f.lower(), l.lower(), d))
    return out


async def run_miner(limit_domains: int = 2000, after: int = 0, dry_run: bool = False,
                    fetch_concurrency: int = 16) -> dict:
    """Mine team pages of our domains → people → Skrapp → verify → store."""
    global _miner_progress
    _miner_progress = {"running": True, "domains_done": 0, "people_found": 0,
                       "skrapp_hits": 0, "added": 0, "target_domains": limit_domains,
                       "done": False, "msg": "loading domains", "dry_run": dry_run}

    domains = await _all_domains()
    domains = domains[after: after + limit_domains]
    existing = await _existing_people()
    from pipeline.llm import active_provider
    # LLM fallback uses ONLY an open-model provider (your Ollama) — never Claude.
    # Regex/structured does the bulk; when Ollama isn't wired, miner is regex-only.
    use_llm = active_provider() in ("ollama", "openai")

    sem = asyncio.Semaphore(fetch_concurrency)
    skrapp_sem = asyncio.Semaphore(settings.SKRAPP_CONCURRENCY)
    total_added = 0
    domains_done = 0
    people_found = 0
    skrapp_hits = 0
    batch_rows: list[dict] = []

    async def handle(domain: str):
        nonlocal domains_done, people_found, skrapp_hits, total_added, batch_rows
        async with sem:
            people = await mine_domain(domain, use_llm)
        domains_done += 1
        fresh = [p for p in people if (p["first"].lower(), p["last"].lower(), domain) not in existing]
        people_found += len(fresh)
        if dry_run:
            return
        for p in fresh:
            async with skrapp_sem:
                res = await skrapp.find_email(p["first"], p["last"], domain)
            if res and res.get("email"):
                skrapp_hits += 1
                niche = classify(p.get("role"), None, None, "https://" + domain)
                batch_rows.append({
                    "email": res["email"].lower(), "name": p["name"],
                    "first_name": p["first"], "last_name": p["last"],
                    "company": "", "role": p.get("role") or "",
                    "website": "https://" + domain, "industry": niche, "location": "",
                })

    # process in chunks so we can commit + update progress periodically
    CHUNK = 200
    for i in range(0, len(domains), CHUNK):
        await asyncio.gather(*[handle(d) for d in domains[i:i + CHUNK]])
        if batch_rows and not dry_run:
            stats = await ingest_rows(batch_rows, source="team_miner", verify=True)
            total_added += stats.get("added", 0)
            batch_rows = []
        _miner_progress.update(domains_done=domains_done, people_found=people_found,
                               skrapp_hits=skrapp_hits, added=total_added,
                               msg=f"{domains_done}/{len(domains)} domains, {people_found} new people, {total_added} leads")
        if _stop_flag["stop"]:
            _miner_progress["msg"] = "Stopped on request"
            break
        if not dry_run and skrapp.get_state().get("quota_exhausted"):
            _miner_progress["msg"] = "Skrapp quota exhausted — stopping"
            break
        # Yield safety-guard: auto-stop if we're wasting credits (low lead yield)
        if not dry_run and skrapp_hits >= 300 and (total_added / max(skrapp_hits, 1)) < 0.05:
            _miner_progress["msg"] = (f"AUTO-STOPPED: yield {100*total_added/max(skrapp_hits,1):.1f}% "
                                      f"too low ({total_added} leads / {skrapp_hits} credits) — not worth it")
            break

    async with SessionLocal() as s:
        portal_total = (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one()
    _miner_progress.update(running=False, done=True, portal_total=portal_total,
                           msg=(f"DRY RUN: {domains_done} domains scanned, {people_found} NEW people found "
                                f"(projected Skrapp tuples). No credits spent." if dry_run else
                                f"Done: {domains_done} domains, {people_found} new people, "
                                f"{skrapp_hits} Skrapp hits, {total_added} new leads."))
    log.info(f"Miner finished: {get_progress()}")
    return get_progress()
