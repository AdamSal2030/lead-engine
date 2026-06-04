from __future__ import annotations
"""Pull leads from a Skrapp LIST via Skrapp's official API.

Skrapp has no search API, but it DOES expose saved lists:
  GET /api/v2/account                      → account + lists + credits
  GET /api/v2/list/{listId}                → list metadata
  GET /api/v2/list/{listId}/leads?start&size → the leads (with revealed emails)

Workflow: run a Lead Search in Skrapp's UI, "Save to list" (reveals verified
emails using your credits), then the portal pulls the whole list through here
and ingests it (dedupe + MV re-verify + niche tag). Uses the SKRAPP_API_KEY you
already have — no fragile session tokens.
"""
import logging
import httpx
from config import settings

log = logging.getLogger("skrapp_lists")

BASE = "https://api.skrapp.io/api/v2"


def _headers() -> dict:
    return {"X-Access-Key": settings.SKRAPP_API_KEY, "Accept": "application/json"}


def _first(d: dict, *keys: str) -> str:
    """Return first non-empty value among the given keys (case/shape tolerant)."""
    for k in keys:
        v = d.get(k)
        if isinstance(v, dict):
            # nested objects like {"name": ...} or {"email": ...}
            v = v.get("name") or v.get("value") or v.get("email")
        if v:
            return str(v).strip()
    return ""


def _map_lead(lead: dict) -> dict:
    """Map a Skrapp list-lead object → our importer row shape."""
    email = _first(lead, "email", "professionalEmail", "mostProbableEmail", "bestEmail").lower()
    first = _first(lead, "firstName", "first_name", "givenName")
    last = _first(lead, "lastName", "last_name", "familyName")
    name = _first(lead, "fullName", "name") or f"{first} {last}".strip()
    company = _first(lead, "companyName", "company", "organization", "currentCompany")
    role = _first(lead, "position", "jobTitle", "title", "role")
    website = _first(lead, "domain", "companyDomain", "website", "companyWebsite")
    industry = _first(lead, "industry", "companyIndustry", "sector")
    location = _first(lead, "location", "country", "city")
    if name and not first:
        parts = name.split()
        first = parts[0] if parts else ""
        last = parts[-1] if len(parts) > 1 else ""
    return {
        "email": email, "name": name, "first_name": first, "last_name": last,
        "company": company, "role": role, "website": website,
        "industry": industry, "location": location,
    }


def _extract_leads(payload) -> list[dict]:
    """Skrapp responses vary; find the leads array wherever it lives."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("leads", "data", "items", "results", "rows"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        # nested {"list": {"leads": [...]}}
        for v in payload.values():
            if isinstance(v, dict):
                inner = _extract_leads(v)
                if inner:
                    return inner
    return []


async def fetch_account() -> dict:
    """Account info — credits + any lists Skrapp exposes."""
    if not settings.SKRAPP_API_KEY:
        return {"ok": False, "error": "No SKRAPP_API_KEY configured"}
    try:
        async with httpx.AsyncClient(timeout=20, headers=_headers()) as cli:
            r = await cli.get(f"{BASE}/account")
        if r.status_code != 200:
            return {"ok": False, "status": r.status_code, "body": r.text[:300]}
        return {"ok": True, "account": r.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


async def fetch_list_leads(list_id: str, max_leads: int = 100000,
                           page_size: int = 100) -> tuple[list[dict], dict]:
    """Pull every lead from a Skrapp list. Returns (rows, meta)."""
    meta = {"list_id": list_id, "fetched": 0, "with_email": 0, "pages": 0,
            "http_status": None, "error": None}
    if not settings.SKRAPP_API_KEY:
        meta["error"] = "No SKRAPP_API_KEY configured in environment"
        return [], meta

    rows: list[dict] = []
    start = 0
    try:
        async with httpx.AsyncClient(timeout=30, headers=_headers()) as cli:
            while len(rows) < max_leads:
                r = await cli.get(
                    f"{BASE}/list/{list_id}/leads",
                    params={"start": start, "size": page_size},
                )
                meta["http_status"] = r.status_code
                if r.status_code == 401 or r.status_code == 403:
                    meta["error"] = f"Skrapp auth failed (HTTP {r.status_code}) — check SKRAPP_API_KEY"
                    break
                if r.status_code != 200:
                    meta["error"] = f"Skrapp HTTP {r.status_code}: {r.text[:200]}"
                    break
                leads = _extract_leads(r.json())
                if not leads:
                    break
                meta["pages"] += 1
                for ld in leads:
                    row = _map_lead(ld)
                    rows.append(row)
                    if row["email"]:
                        meta["with_email"] += 1
                if len(leads) < page_size:
                    break
                start += page_size
    except Exception as e:
        meta["error"] = str(e)[:200]
    meta["fetched"] = len(rows)
    return rows, meta
