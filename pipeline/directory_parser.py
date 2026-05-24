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


async def parse_hn_showhn(url: str) -> dict | None:
    """Parse a Hacker News Show HN story via the Firebase REST API.

    Each Show HN post is a founder showing their product. The story's `url`
    field IS the founder's website. The `by` field is their HN username; their
    user profile's `about` often contains an email address.
    """
    import httpx as _httpx
    m = re.search(r"id=(\d+)", url)
    if not m:
        return None
    item_id = m.group(1)

    try:
        async with _httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
            )
            if r.status_code != 200:
                return None
            story = r.json()
    except Exception:
        return None

    if not story or story.get("type") != "story":
        return None

    # Must have an external URL (the product website)
    product_url = (story.get("url") or "").strip()
    if not product_url or not product_url.startswith("http"):
        return None

    title = (story.get("title") or "").strip()
    if not title.lower().startswith("show hn"):
        return None  # not a Show HN — skip

    # Filter out non-product URLs (docs, repos, news articles)
    JUNK_HOSTS = {
        "github.com", "gitlab.com", "docs.google.com", "youtube.com",
        "twitter.com", "x.com", "reddit.com", "linkedin.com", "medium.com",
        "notion.so", "figma.com", "news.ycombinator.com",
    }
    try:
        prod_domain = product_url.split("/")[2].lower().replace("www.", "")
    except IndexError:
        return None
    if any(j in prod_domain for j in JUNK_HOSTS):
        return None

    # Clean title: "Show HN: My App – does stuff" → "My App"
    clean_title = re.sub(r"^Show HN:\s*", "", title, flags=re.IGNORECASE).strip()
    clean_title = re.split(r"\s+[–—-]{1,2}\s+", clean_title)[0].strip()

    # Fetch author profile for email / real name
    author = (story.get("by") or "").strip()
    person_name = None
    article_emails: list[str] = []

    if author:
        try:
            async with _httpx.AsyncClient(timeout=10) as client:
                r = await client.get(
                    f"https://hacker-news.firebaseio.com/v0/user/{author}.json"
                )
                if r.status_code == 200:
                    user = r.json() or {}
                    about = user.get("about") or ""
                    # Strip HTML tags that HN sometimes wraps around about text
                    about_clean = re.sub(r"<[^>]+>", " ", about)
                    # Extract email
                    em = re.search(
                        r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b",
                        about_clean,
                    )
                    if em:
                        article_emails = [em.group(0)]
                    # Extract real name: "Hi, I'm Jane Smith" / "I am Jane Smith"
                    nm = re.search(
                        r"(?:I'm|I am|My name is|name[:\s]+)\s+([A-Z][a-z]+ [A-Z][a-z]+)",
                        about_clean,
                    )
                    if nm:
                        person_name = nm.group(1)
        except Exception:
            pass

    from pipeline.niche import classify

    hook = f"Saw your Show HN post about {clean_title[:55]} — interesting product." if clean_title else ""

    return {
        "source_url": url,
        "source": "HackerNews",
        "name": person_name or author,
        "website": product_url,
        "role": "Founder",
        "company": clean_title,
        "niche": classify("Founder", clean_title, None, product_url),
        "hook": hook,
        "article_emails": article_emails,
        "_parsed_by": "directory",
    }


async def parse_betalist_startup(url: str) -> dict | None:
    """Parse a BetaList startup page.

    BetaList pages are server-rendered and include the startup name, tagline,
    and a direct link to the startup's website. Founder name is sometimes in
    the page; if not, Skrapp domain-search fills it in later.
    """
    html = await fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    title_el = soup.find("h1")
    company_name = title_el.get_text(strip=True) if title_el else ""
    if not company_name:
        return None

    # BetaList pages have a prominent "Visit Website" or "Launch" CTA button
    website = None
    for a in soup.find_all("a", href=True):
        h = (a.get("href") or "").strip()
        text = a.get_text(strip=True).lower()
        if not h.startswith("http"):
            continue
        link_domain = h.split("/")[2].lower() if "//" in h else ""
        if "betalist.com" in link_domain:
            continue
        if any(s in link_domain for s in SOCIALS | SELF_HOSTS):
            continue
        # Prefer clearly-labelled launch/website buttons
        if any(kw in text for kw in ("visit", "launch", "website", "try", "get")):
            website = h.split("?")[0].rstrip("/")
            break
    if not website:
        website = _extract_website(soup, url)
    if not website:
        return None

    # Try to find founder name
    body_text = soup.get_text(" ", strip=True)
    person_name = None
    for pat in [
        r"(?:Founded by|Made by|Created by|By)\s+([A-Z][a-z]+ [A-Z][a-z]+)",
        r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:Founder|CEO|Co-Founder|Owner)",
    ]:
        nm = re.search(pat, body_text)
        if nm:
            from pipeline.parser import _try_parse_segment
            candidate = _try_parse_segment(nm.group(1))
            if candidate:
                person_name = candidate
                break

    article_emails = extract_emails(str(soup))

    from pipeline.niche import classify

    return {
        "source_url": url,
        "source": "BetaList",
        "name": person_name or company_name,
        "website": website,
        "role": "Founder",
        "company": company_name,
        "niche": classify(None, company_name, None, website),
        "hook": "",
        "article_emails": sorted(article_emails),
        "_parsed_by": "directory",
        "_is_company": person_name is None,
    }


async def parse_goodfirms_profile(url: str) -> dict | None:
    """Parse a GoodFirms company profile page.

    GoodFirms is an IT/software agency directory similar to Clutch. Each
    profile includes the company name, website, and sometimes the CEO/founder.
    """
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

    body_text = soup.get_text(" ", strip=True)
    person_name = None
    for pat in [
        r"(?:CEO|Founder|Co-Founder|Owner|Managing Director|President)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)",
        r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:CEO|Founder|Co-Founder|Owner|Managing Director)",
    ]:
        nm = re.search(pat, body_text)
        if nm:
            from pipeline.parser import _try_parse_segment
            candidate = _try_parse_segment(nm.group(1))
            if candidate:
                person_name = candidate
                break

    from pipeline.niche import classify

    return {
        "source_url": url,
        "source": "GoodFirms",
        "name": person_name or company_name,
        "website": website,
        "role": "Founder",
        "company": company_name,
        "niche": classify(None, company_name, None, website),
        "hook": "",
        "article_emails": [],
        "_parsed_by": "directory",
        "_is_company": person_name is None,
    }


async def parse_g2_product(url: str) -> dict | None:
    """Parse a G2 software product page.

    G2 product pages list the company name, category, and have a 'Visit Website'
    button with the company's real website URL. No Claude needed.
    """
    html = await fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    title_el = soup.find("h1")
    company_name = title_el.get_text(strip=True) if title_el else ""
    if not company_name:
        return None

    # G2 has a prominent "Visit Website" / "Visit x.com" link
    website = None
    for a in soup.find_all("a", href=True):
        h = (a.get("href") or "").strip()
        text = a.get_text(strip=True).lower()
        if not h.startswith("http"):
            continue
        link_domain = h.split("/")[2].lower() if "//" in h else ""
        if "g2.com" in link_domain:
            continue
        if any(s in link_domain for s in SOCIALS | SELF_HOSTS):
            continue
        if any(kw in text for kw in ("visit website", "visit", "get started", "free trial", "try free")):
            website = h.split("?")[0].rstrip("/")
            break
    if not website:
        website = _extract_website(soup, url)
    if not website:
        return None

    body_text = soup.get_text(" ", strip=True)
    person_name = None
    for pat in [
        r"(?:CEO|Founder|Co-Founder|Owner|President)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)",
        r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:CEO|Founder|Co-Founder|Owner|President)",
    ]:
        nm = re.search(pat, body_text)
        if nm:
            from pipeline.parser import _try_parse_segment
            candidate = _try_parse_segment(nm.group(1))
            if candidate:
                person_name = candidate
                break

    from pipeline.niche import classify

    return {
        "source_url": url,
        "source": "G2",
        "name": person_name or company_name,
        "website": website,
        "role": "Founder",
        "company": company_name,
        "niche": classify(None, company_name, None, website),
        "hook": "",
        "article_emails": [],
        "_parsed_by": "directory",
        "_is_company": person_name is None,
    }


async def parse_capterra_product(url: str) -> dict | None:
    """Parse a Capterra software product page.

    Similar to G2 — each product page has a company name, category, and a
    direct link to the company website.
    """
    html = await fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    title_el = soup.find("h1")
    company_name = title_el.get_text(strip=True) if title_el else ""
    if not company_name:
        return None

    # Capterra has "Visit Website" CTA
    website = None
    for a in soup.find_all("a", href=True):
        h = (a.get("href") or "").strip()
        text = a.get_text(strip=True).lower()
        if not h.startswith("http"):
            continue
        link_domain = h.split("/")[2].lower() if "//" in h else ""
        if "capterra.com" in link_domain or "gartner.com" in link_domain:
            continue
        if any(s in link_domain for s in SOCIALS | SELF_HOSTS):
            continue
        if any(kw in text for kw in ("visit website", "visit", "get started", "free trial", "try")):
            website = h.split("?")[0].rstrip("/")
            break
    if not website:
        website = _extract_website(soup, url)
    if not website:
        return None

    body_text = soup.get_text(" ", strip=True)
    person_name = None
    for pat in [
        r"(?:CEO|Founder|Co-Founder|Owner|President)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)",
        r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:CEO|Founder|Co-Founder|Owner|President)",
    ]:
        nm = re.search(pat, body_text)
        if nm:
            from pipeline.parser import _try_parse_segment
            candidate = _try_parse_segment(nm.group(1))
            if candidate:
                person_name = candidate
                break

    from pipeline.niche import classify

    return {
        "source_url": url,
        "source": "Capterra",
        "name": person_name or company_name,
        "website": website,
        "role": "Founder",
        "company": company_name,
        "niche": classify(None, company_name, None, website),
        "hook": "",
        "article_emails": [],
        "_parsed_by": "directory",
        "_is_company": person_name is None,
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
