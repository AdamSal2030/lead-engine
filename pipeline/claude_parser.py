from __future__ import annotations
"""Claude Haiku-powered article parser.

Fires as fallback when the regex parser returns None (no name or no website found).
Receives clean extracted text (~500 tokens) instead of raw HTML — ~75% cheaper than
sending the full HTML blob.

Cost: ~$0.000060 per article (Haiku, clean text). Only called on regex failures
AND only when the article passes the interview pre-screen.
Daily cap (CLAUDE_MAX_PER_DAY) prevents runaway spend on bad URL pools.
"""
import json
import logging
from datetime import date
from config import settings

log = logging.getLogger("claude_parser")

_client = None

# In-memory daily call counter — resets on each calendar day (UTC)
_calls_today: int = 0
_calls_date: date | None = None


def _within_daily_limit() -> bool:
    global _calls_today, _calls_date
    today = date.today()
    if _calls_date != today:
        _calls_date = today
        _calls_today = 0
    return _calls_today < settings.CLAUDE_MAX_PER_DAY


def _increment_call() -> None:
    global _calls_today
    _calls_today += 1


def get_daily_usage() -> dict:
    """Return current day's call count and cap (for dashboard/API)."""
    global _calls_today, _calls_date
    today = date.today()
    if _calls_date != today:
        _calls_today = 0
    return {
        "calls_today": _calls_today,
        "cap": settings.CLAUDE_MAX_PER_DAY,
        "remaining": max(0, settings.CLAUDE_MAX_PER_DAY - _calls_today),
    }


def _get_client():
    if not settings.ANTHROPIC_API_KEY or not settings.CLAUDE_PARSE_ENABLED:
        return None
    global _client
    if _client is None:
        import anthropic
        _client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
    return _client


SYSTEM_PROMPT = """\
You are a lead-data extraction assistant. Given a page title, meta description, and article text from an entrepreneur/founder/professional interview or profile page, extract structured data about the featured person.

Return ONLY a single JSON object — no markdown, no explanation, just JSON.

Keys:
- "name": Full name of the featured person (2-4 words, properly capitalised, e.g. "Jane Smith"). Must look like a real person's name, not a company or article title.
- "company": Their business / company name (string, may be empty if unclear).
- "website": Their personal business website URL. Must be a real domain — NOT social media (instagram, facebook, linkedin, twitter, youtube, tiktok), NOT linktr.ee, NOT the interview site itself.
- "role": Primary role — e.g. "Founder", "CEO", "Owner", "Coach", "Consultant", "Agency Owner", "Designer", "Photographer", "Author", "Therapist", "Realtor", "Attorney", "Chef".
- "niche": 2–4 word industry/niche label — e.g. "Marketing Agency", "Life Coaching", "E-commerce", "SaaS", "Real Estate", "Fitness", "Photography", "Financial Planning", "Interior Design", "PR Agency", "Recruiting", "Wedding Planning", "Graphic Design", "Consulting", "Legal Services".
- "hook": One concrete personalisation sentence pulled directly from the article — something specific they said or achieved. Use for cold outreach icebreaker. Example: "Your piece mentioned scaling your coaching practice to 60 clients in under a year — impressive trajectory."

If you cannot confidently find BOTH "name" AND "website" return exactly: {"skip": true}

Hard rules:
- website must start with http or be a bare domain (you may normalise to https://)
- website must NOT be a social platform or link aggregator
- name must be 2–4 capitalised words that look like a real person
- hook must be specific to this person, not a generic sentence
"""


async def parse_with_claude(url: str, clean_text: str) -> dict | None:
    """Parse article with Claude Haiku. Accepts clean extracted text (NOT raw HTML).
    Returns structured dict or None. Respects daily call cap."""
    client = _get_client()
    if not client:
        return None

    if not _within_daily_limit():
        log.info(f"Claude daily cap ({settings.CLAUDE_MAX_PER_DAY}) reached — skipping {url}")
        return None

    try:
        _increment_call()
        msg = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=350,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": f"URL: {url}\n\n{clean_text}"}],
        )
        raw = msg.content[0].text.strip()

        # Strip accidental markdown fences
        if "```" in raw:
            parts = raw.split("```")
            raw = parts[1] if len(parts) > 1 else parts[0]
            if raw.startswith("json"):
                raw = raw[4:]

        data = json.loads(raw.strip())

        if data.get("skip"):
            return None

        name = (data.get("name") or "").strip()
        website = (data.get("website") or "").strip()

        if not name or not website:
            return None

        # Reject social / aggregator URLs
        bad_hosts = [
            "instagram.com", "facebook.com", "linkedin.com", "twitter.com",
            "youtube.com", "tiktok.com", "linktr.ee", "beacons.ai", "stan.store",
            "threads.net", "pinterest.com", "snapchat.com",
        ]
        if any(b in website for b in bad_hosts):
            return None

        if not website.startswith("http"):
            website = "https://" + website

        return {
            "name": name,
            "company": (data.get("company") or "").strip(),
            "website": website,
            "role": (data.get("role") or "Founder").strip(),
            "niche": (data.get("niche") or "").strip(),
            "hook": (data.get("hook") or "").strip(),
        }

    except json.JSONDecodeError:
        log.debug(f"Claude parse JSON error for {url}")
        return None
    except Exception as e:
        log.debug(f"Claude parse error {url}: {e}")
        return None
