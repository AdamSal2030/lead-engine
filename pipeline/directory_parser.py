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
import json
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


# Decision-maker roles worth reaching (used by both JSON-LD jobTitle and text regex)
_FOUNDER_ROLES = (
    "CEO", "Chief Executive Officer", "Chief Executive", "Founder", "Co-Founder",
    "Cofounder", "Co Founder", "Founder & CEO", "Owner", "Co-Owner", "President",
    "Managing Director", "Managing Partner", "Principal", "Creator", "Director",
    "Partner", "Proprietor",
)
_ROLE_ALT = "|".join(re.escape(r) for r in sorted(_FOUNDER_ROLES, key=len, reverse=True))
# Broader name matcher than the old "[A-Z][a-z]+ [A-Z][a-z]+":
# 2-3 tokens, allows initials, hyphens, apostrophes and accented letters.
_NAME_RX = r"[A-ZÀ-Ý][A-Za-zÀ-ÿ'’\.\-]+(?:\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ'’\.\-]+){1,2}"


def _walk_jsonld(node) -> list[str]:
    """Recursively collect Person names from a parsed JSON-LD node."""
    found: list[str] = []
    if isinstance(node, list):
        for x in node:
            found.extend(_walk_jsonld(x))
        return found
    if not isinstance(node, dict):
        return found
    types = node.get("@type")
    types = types if isinstance(types, list) else [types]
    if "Person" in types:
        nm = node.get("name")
        if isinstance(nm, str) and nm.strip():
            found.append(nm.strip())
    # Explicit decision-maker fields
    for key in ("founder", "founders", "employee", "employees", "author"):
        if key in node:
            found.extend(_walk_jsonld(node[key]))
    # Nested containers
    for key in ("@graph", "mainEntity", "about", "subOrganization", "memberOf"):
        if key in node:
            found.extend(_walk_jsonld(node[key]))
    return found


def _names_from_jsonld(soup: BeautifulSoup) -> list[str]:
    """Pull candidate Person names from all schema.org JSON-LD blocks on the page."""
    out: list[str] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            # Some sites concatenate multiple JSON objects or have trailing commas
            continue
        out.extend(_walk_jsonld(data))
    return out


def extract_founder_name(soup: BeautifulSoup, body_text: str | None = None) -> str | None:
    """Best-effort founder/decision-maker name from a directory profile page.

    Order of preference:
      1. schema.org JSON-LD (founder / employee / Person nodes) — most reliable
      2. Role-adjacent regex over the page text ("CEO: Jane Smith",
         "Jane Smith, Founder", "Founded by Jane Smith")

    Every candidate is validated through the strict name parser, so company
    names and junk get rejected.
    """
    from pipeline.parser import _try_parse_segment

    def _norm(s: str) -> str:
        # Normalise curly apostrophes so the strict validator accepts O’Brien etc.
        return s.replace("’", "'").replace("‘", "'")

    # 1. Structured data — strongest signal
    for cand in _names_from_jsonld(soup):
        name = _try_parse_segment(_norm(cand))
        if name:
            return name

    # 2. Role-adjacent text. The role/keyword scaffolding is case-insensitive via
    # scoped (?i:...) groups, but the captured ({_NAME_RX}) stays case-sensitive so
    # capitalisation still identifies a real name (and not lowercase prose).
    if body_text is None:
        body_text = soup.get_text(" ", strip=True)
    patterns = [
        rf"(?i:{_ROLE_ALT})(?i:\s*(?:[:\-–—]|is(?:\s+the)?|,)?\s*)({_NAME_RX})",
        rf"({_NAME_RX})(?i:\s*,?\s+(?:is\s+(?:the\s+)?)?(?:{_ROLE_ALT})\b)",
        rf"(?i:(?:Founded|Created|Started|Led|Run)\s+by\s+)({_NAME_RX})",
    ]
    for pat in patterns:
        for m in re.finditer(pat, body_text):
            name = _try_parse_segment(_norm(m.group(1)))
            if name:
                return name
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

    # Founder/decision-maker name — JSON-LD structured data first, then role-adjacent text
    person_name = extract_founder_name(soup)

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

    # Founder name — JSON-LD first, then role-adjacent text
    person_name = extract_founder_name(soup)

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

    person_name = extract_founder_name(soup)

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


async def parse_trustpilot_business(url: str) -> dict | None:
    """Parse a Trustpilot business review page.

    KEY TRICK: the company domain is in the URL itself, so we NEVER need to
    scrape the page just to find the website — we already have it.
      https://www.trustpilot.com/review/acme.com  →  website = https://acme.com

    We still fetch the page to get the company name (for email personalisation),
    but if the fetch fails we can still produce a lead using just the domain and
    decision-maker pattern emails (founder@, ceo@, hello@, info@).
    """
    # Extract domain directly from URL — this is always available
    m = re.match(
        r"https://www\.trustpilot\.com/review/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,})/?$",
        url,
    )
    if not m:
        return None
    domain = m.group(1).lower()
    website = f"https://{domain}"

    # Skip obvious non-business TLDs and known junk
    BAD_TLDS = {".gov", ".edu", ".mil"}
    if any(domain.endswith(t) for t in BAD_TLDS):
        return None

    # Fetch for company name (best-effort; we succeed even without it)
    company_name = domain.split(".")[0].replace("-", " ").title()  # fallback
    person_name = None
    article_emails: list[str] = []

    html = await fetch(url, timeout=12)
    if html:
        soup = BeautifulSoup(html, "lxml")
        h1 = soup.find("h1")
        if h1:
            raw = h1.get_text(strip=True)
            # Trustpilot H1 pattern: "Reviews of Acme Inc" or just "Acme Inc"
            clean = re.sub(r"^Reviews?\s+(?:of|for)\s+", "", raw, flags=re.IGNORECASE).strip()
            if clean and len(clean) < 100:
                company_name = clean
        # CEO/founder sometimes mentioned on the business page
        person_name = extract_founder_name(soup)
        article_emails = list(extract_emails(str(soup)))

    from pipeline.niche import classify

    return {
        "source_url": url,
        "source": "Trustpilot",
        "name": person_name or company_name,
        "website": website,
        "role": "Founder",
        "company": company_name,
        "niche": classify(None, company_name, None, website),
        "hook": "",
        "article_emails": article_emails,
        "_parsed_by": "directory",
        "_is_company": person_name is None,
    }


async def parse_appsumo_product(url: str) -> dict | None:
    """Parse an AppSumo product listing.

    AppSumo is a marketplace for SaaS lifetime deals. Each product page is
    server-side rendered and includes the product name, tagline, and a link to
    the company website. Reaches bootstrapped founders before they're published.
    """
    html = await fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    title_el = soup.find("h1")
    product_name = title_el.get_text(strip=True) if title_el else ""
    if not product_name:
        return None

    # Website: look for CTA buttons and external links
    website = None
    APPSUMO_JUNK = {"appsumo.com", "partners.appsumo.com", "help.appsumo.com"}
    for a in soup.find_all("a", href=True):
        h = (a.get("href") or "").strip()
        text = a.get_text(strip=True).lower()
        if not h.startswith("http"):
            continue
        link_domain = h.split("/")[2].lower().replace("www.", "") if "//" in h else ""
        if any(j in link_domain for j in APPSUMO_JUNK):
            continue
        if any(s in link_domain for s in SOCIALS | SELF_HOSTS):
            continue
        # Prefer CTA buttons
        if any(kw in text for kw in ("get", "visit", "try", "start", "access", "lifetime")):
            website = h.split("?")[0].rstrip("/")
            break
    if not website:
        website = _extract_website(soup, url)
    if not website:
        return None

    person_name = extract_founder_name(soup)

    article_emails = list(extract_emails(str(soup)))
    from pipeline.niche import classify

    return {
        "source_url": url,
        "source": "AppSumo",
        "name": person_name or product_name,
        "website": website,
        "role": "Founder",
        "company": product_name,
        "niche": classify(None, product_name, None, website),
        "hook": "",
        "article_emails": article_emails,
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

    # Founder name — JSON-LD first, then role-adjacent text
    person_name = extract_founder_name(soup)

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
