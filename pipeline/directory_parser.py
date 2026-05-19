from __future__ import annotations
"""Directory / listing page parser.

Handles structured professional directory profiles — NOT interview articles.
These pages list a person or company with their website. We use them to find
prospects who have never been published in an interview but would make great
leads for paid feature placements.

Supported directory types:
  - Clutch.co — agency/company profiles (clutch_parse)
  - IndieHackers — founder profiles (ih_parse)
  - DesignRush — agency profiles (designrush_parse)

Returned dict shape matches the interview parser output so the orchestrator
(process_one_url) can treat both paths identically.
"""
import re
import logging
from bs4 import BeautifulSoup
from pipeline.parser import fetch, extract_emails

log = logging.getLogger("directory_parser")

SOCIALS = frozenset([
    "instagram.com", "facebook.com", "twitter.com", "x.com", "linkedin.com",
    "youtube.com", "tiktok.com", "threads.net", "pinterest.com", "snapchat.com",
    "linktr.ee", "beacons.ai",
])

# Hosts that are the directory site itself (not the founder's site)
SELF_HOSTS = frozenset([
    "clutch.co", "clutch.com", "designrush.com", "indiehackers.com",
    "goodfirms.co", "upwork.com", "fiverr.com", "freelancer.com",
])


def _clean_url(url: str) -> str | None:
    if not url:
        return None
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = "https://" + url
    domain = url.split("/")[2].lower().replace("www.", "")
    if any(s in domain for s in SOCIALS | SELF_HOSTS):
        return None
    return url


def _extract_person_name(soup: BeautifulSoup, title_text: str) -> str | None:
    """Try to find a person name from heading text."""
    from pipeline.parser import clean_name
    # 1. Direct from H1/H2
    for tag in ["h1", "h2"]:
        for h in soup.find_all(tag):
            t = h.get_text(strip=True)
            n = clean_name(t)
            if n:
                return n
    # 2. From page title
    if title_text:
        n = clean_name(title_text)
        if n:
            return n
    return None


def _extract_website(soup: BeautifulSoup, page_url: str) -> str | None:
    """Extract external website link from a directory profile page."""
    page_domain = page_url.split("/")[2].lower()
    for a in soup.find_all("a", href=True):
        h = a.get("href", "").strip()
        if not h.startswith("http"):
            continue
        link_domain = h.split("/")[2].lower().replace("www.", "")
        if link_domain == page_domain.replace("www.", ""):
            continue  # same site
        if any(s in link_domain for s in SOCIALS):
            continue
        if any(s in link_domain for s in SELF_HOSTS):
            continue
        # Looks like an external business website
        return h.split("?")[0].split("#")[0]
    return None


async def parse_clutch_profile(url: str) -> dict | None:
    """Parse a Clutch.co company profile.
    Extracts: company name, website, industry/niche from the profile page.
    Sets name = company name (no personal name available — Skrapp will use domain search).
    """
    html = await fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    title = soup.find("h1")
    company_name = title.get_text(strip=True) if title else ""
    if not company_name:
        return None

    # Website: look for external link in the profile's header/sidebar
    website = _extract_website(soup, url)
    if not website:
        return None

    # Try to extract a person name from "About" / "CEO" / "Founder" mentions
    body_text = soup.get_text(" ", strip=True)
    person_name = None

    # Clutch often has "Founded by [Name]" or "CEO: [Name]"
    for pat in [
        r"(?:Founded by|CEO:|Founder:|Founded by CEO)\s+([A-Z][a-z]+ [A-Z][a-z]+)",
        r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:CEO|Founder|Co-Founder|Owner|President)",
    ]:
        m = re.search(pat, body_text)
        if m:
            from pipeline.parser import _try_parse_segment
            candidate = _try_parse_segment(m.group(1))
            if candidate:
                person_name = candidate
                break

    # Niche: use service category tags on Clutch
    from pipeline.niche import classify
    # Try to pull focus areas from the page text
    niche = classify(None, company_name, None, website)

    return {
        "source_url": url,
        "source": "Clutch",
        "name": person_name or company_name,
        "website": website,
        "role": "Founder",
        "company": company_name,
        "niche": niche,
        "hook": "",
        "article_emails": [],
        "_parsed_by": "directory",
        "_is_company": person_name is None,  # flag: no personal name found
    }


async def parse_indiehackers_profile(url: str) -> dict | None:
    """Parse an IndieHackers founder profile.
    IndieHackers has clean founder profiles with product name + website.
    """
    html = await fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    title_el = soup.find("h1")
    title_text = title_el.get_text(strip=True) if title_el else ""

    # IndieHackers profile H1 is often the founder's name or product name
    from pipeline.parser import clean_name
    person_name = clean_name(title_text)

    # Website
    website = _extract_website(soup, url)
    if not website:
        return None

    # Extract emails from page
    article_emails = extract_emails(str(soup))

    from pipeline.niche import classify
    niche = classify("Founder", None, None, website)

    return {
        "source_url": url,
        "source": "IndieHackers",
        "name": person_name or "IndieHacker Founder",
        "website": website,
        "role": "Founder",
        "company": "",
        "niche": niche,
        "hook": "",
        "article_emails": sorted(article_emails),
        "_parsed_by": "directory",
    }


async def parse_designrush_profile(url: str) -> dict | None:
    """Parse a DesignRush agency profile."""
    html = await fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    title_el = soup.find("h1")
    company_name = title_el.get_text(strip=True) if title_el else ""
    if not company_name:
        return None

    website = _extract_website(soup, url)
    if not website:
        return None

    # Try person name extraction
    body_text = soup.get_text(" ", strip=True)
    person_name = None
    for pat in [
        r"(?:CEO|Founder|Managing Director|Owner):\s*([A-Z][a-z]+ [A-Z][a-z]+)",
        r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:CEO|Founder|Managing Director|Owner)",
    ]:
        m = re.search(pat, body_text)
        if m:
            from pipeline.parser import _try_parse_segment
            candidate = _try_parse_segment(m.group(1))
            if candidate:
                person_name = candidate
                break

    from pipeline.niche import classify
    niche = classify(None, company_name, None, website)

    return {
        "source_url": url,
        "source": "DesignRush",
        "name": person_name or company_name,
        "website": website,
        "role": "Founder",
        "company": company_name,
        "niche": niche,
        "hook": "",
        "article_emails": [],
        "_parsed_by": "directory",
        "_is_company": person_name is None,
    }
