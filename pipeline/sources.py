from __future__ import annotations
"""Source sitemap collectors. Each returns list[str] of founder-interview URLs."""
import re
import httpx
import logging

log = logging.getLogger("sources")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml", "Accept-Language": "en-US,en;q=0.9"}

# Fallback UAs for sites that block Railway datacenter IPs (Cloudflare etc.)
UA_FALLBACKS = [
    UA,  # default Chrome on Mac
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


async def fetch(url: str, timeout: int = 20, try_fallbacks: bool = False) -> str | None:
    uas = UA_FALLBACKS if try_fallbacks else [UA]
    for ua in uas:
        try:
            headers = {"User-Agent": ua, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}
            async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True) as cli:
                r = await cli.get(url)
                if r.status_code == 200 and len(r.text) > 50:
                    return r.text
        except Exception as e:
            log.debug(f"fetch fail {url} (ua={ua[:30]}): {e}")
            continue
    return None


async def fetch_via_wayback(url: str, timeout: int = 45) -> str | None:
    """Last-resort fallback: fetch via Wayback Machine archive."""
    wb = f"https://web.archive.org/web/2026/{url}"
    return await fetch(wb, timeout=timeout, try_fallbacks=True)


async def canvasrebel_urls() -> list[str]:
    # Direct first; if blocked (Cloudflare on Railway IPs), fall back to Wayback Machine archive
    text = await fetch("https://canvasrebel.com/post-sitemap.xml", try_fallbacks=True)
    if not text:
        log.info("CanvasRebel direct sitemap blocked, falling back to Wayback Machine")
        text = await fetch("https://web.archive.org/web/2026/https://canvasrebel.com/post-sitemap.xml",
                           try_fallbacks=True, timeout=45)
    if not text:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", text)
    return [u for u in urls if re.match(r"https://canvasrebel\.com/meet-[^/]+/?$", u)]


# In-memory cache of Authority Magazine RSS items: {url: {name, company}}
AUTHORITY_CACHE: dict[str, dict] = {}


def _parse_authority_title(title: str) -> dict | None:
    """Authority Magazine title pattern: '<Topic>: <Name> Of <Company> On How...'
    Returns {name, company} or None."""
    # Strip any '<![CDATA[ ... ]]>' wrapping
    title = title.strip()
    # Common patterns:
    #   "<Topic>: First Last Of Company On <How to do X>"
    #   "<Topic>: First Last, Founder of Company, On <...>"
    #   "First Last Of Company On <Topic>"
    m = re.search(r"[:—–-]\s*([A-Z][\w\.\-'À-ÿ]+(?:\s+[A-Z][\w\.\-'À-ÿ]+){1,3})\s+(?:Of|of|At|at|From|from)\s+([A-Z][\w\.\&\-' ]{1,60}?)\s+(?:On|on)\s",
                  title)
    if not m:
        m = re.search(r"^([A-Z][\w\.\-'À-ÿ]+(?:\s+[A-Z][\w\.\-'À-ÿ]+){1,3})\s+(?:Of|of)\s+([A-Z][\w\.\&\-' ]{1,60}?)\s+(?:On|on)\s",
                      title)
    if m:
        name = re.sub(r"\s+", " ", m.group(1).strip())
        company = re.sub(r"\s+", " ", m.group(2).strip()).rstrip(",")
        # Sanity check name
        words = name.split()
        if 2 <= len(words) <= 4 and all(w[0].isupper() for w in words):
            return {"name": name, "company": company}
    return None


async def authority_magazine_urls() -> list[str]:
    """Pull latest Authority Magazine items via Medium RSS.
    Pre-extracts name+company from titles and caches for the parser to use."""
    text = await fetch("https://medium.com/feed/authority-magazine", try_fallbacks=True, timeout=20)
    if not text:
        return []
    # Parse items
    items = re.findall(r"<item>(.*?)</item>", text, re.DOTALL)
    urls = []
    for item in items:
        title_m = re.search(r"<title><!\[CDATA\[(.*?)\]\]></title>", item, re.DOTALL)
        link_m = re.search(r"<link>([^<]+)</link>", item)
        if not (title_m and link_m): continue
        title, link = title_m.group(1), link_m.group(1).strip()
        parsed = _parse_authority_title(title)
        if not parsed: continue
        AUTHORITY_CACHE[link] = parsed
        urls.append(link)
    return urls


async def boldjourney_urls() -> list[str]:
    text = await fetch("https://boldjourney.com/news-sitemap.xml", try_fallbacks=True)
    if not text:
        log.info("BoldJourney direct sitemap blocked, falling back to Wayback Machine")
        text = await fetch("https://web.archive.org/web/2026/https://boldjourney.com/news-sitemap.xml",
                           try_fallbacks=True, timeout=45)
    if not text:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", text)
    return [u for u in urls if re.match(r"https://boldjourney\.com/meet-[^/]+/?$", u)]


async def brainz_urls() -> list[str]:
    """Brainz Magazine — DISABLED.
    Wix structure stores author info inside JS-rendered components. Our meta-tag parser
    pulled the site name ('Brainz Magazine') as the author and extracted academic citations
    as 'emails'. Needs a Selenium/Playwright approach to reach the real author bio.
    Re-enable once a proper parser is built."""
    return []


async def founderhour_urls() -> list[str]:
    """TheFounderHour has founder interview articles on their blog."""
    text = await fetch("https://www.thefounderhour.com/sitemap.xml")
    if not text:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", text)
    return [u for u in urls
            if "/blog/" in u
            and u.rstrip("/").split("/")[-1].count("-") >= 4
            and not any(x in u for x in ["/category/", "/tag/", "/page/"])]


async def valiantceo_urls() -> list[str]:
    """ValiantCEO has founder interview articles. URL pattern: /firstname-lastname-headline/"""
    text = await fetch("https://valiantceo.com/post-sitemap.xml")
    if not text:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", text)
    # Keep only article-looking URLs (4+ hyphens in slug, no /category/, /tag/, /author/)
    return [u for u in urls
            if re.match(r"https://valiantceo\.com/[a-z0-9\-]+/?$", u)
            and u.rstrip("/").split("/")[-1].count("-") >= 4
            and not any(x in u for x in ["/category/", "/tag/", "/author/", "/page/"])]


VOYAGE_SITES = [
    # Original 20
    "voyagela.com", "voyageatl.com", "voyagemia.com", "voyagedallas.com",
    "voyagehouston.com", "voyageraleigh.com", "voyagestl.com", "voyagekc.com",
    "voyageaustin.com", "voyagechicago.com", "voyageohio.com", "voyageminnesota.com",
    "voyageutah.com", "voyagebaltimore.com", "voyagecharlotte.com",
    "voyagevirginia.com", "voyagewisconsin.com", "voyagewashington.com",
    "voyagealabama.com", "voyagemichigan.com",
    # Additional Voyage network cities
    "voyagephoenix.com", "voyagedenver.com", "voyagesf.com", "voyagerichmond.com",
    "voyageindy.com", "voyagesd.com", "voyagememphis.com", "voyagephilly.com",
    "voyagenashville.com", "voyageportland.com", "voyageseattle.com",
]

# ShoutOut interview sites — same CMS/network as Voyage, direct slugs (no date prefix)
SHOUTOUT_SITES = [
    "shoutoutla.com", "shoutoutatl.com", "shoutoutdfw.com",
    "shoutoutsocal.com", "shoutoutnorcal.com",
]


async def voyage_urls(site: str) -> list[str]:
    text = await fetch(f"https://{site}/post-sitemap.xml", try_fallbacks=True)
    if not text:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", text)
    pat = re.compile(rf"https://{re.escape(site)}/\d{{4}}/\d{{2}}/\d{{2}}/[^/]+/?$")
    return [u for u in urls if pat.match(u) and u.rstrip("/").split("/")[-1].count("-") >= 5]


async def shoutout_urls(site: str) -> list[str]:
    """ShoutOut interview sites — same network as Voyage but no date prefix in URL.
    Pattern: https://shoutoutla.com/meet-first-last-of-company/"""
    text = await fetch(f"https://{site}/post-sitemap.xml", try_fallbacks=True)
    if not text:
        text = await fetch(f"https://{site}/news-sitemap.xml", try_fallbacks=True)
    if not text:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", text)
    out = []
    for u in urls:
        slug = u.rstrip("/").split("/")[-1]
        if slug.count("-") < 4:
            continue
        # Direct slug (ShoutOut style): site.com/meet-slug/
        if re.match(rf"https://{re.escape(site)}/[^/]+/?$", u):
            out.append(u)
        # Date-based (in case some ShoutOut sites use Voyage date format)
        elif re.match(rf"https://{re.escape(site)}/\d{{4}}/\d{{2}}/\d{{2}}/[^/]+/?$", u):
            out.append(u)
    return out


async def ideamensch_urls() -> list[str]:
    """IdeaMensch founder interviews. Thousands of interviews — URL: ideamensch.com/[slug]/
    Each page has a clean H1 with the founder's name and a website link."""
    all_urls = []
    for n in range(1, 30):
        text = await fetch(f"https://ideamensch.com/post-sitemap{n}.xml", try_fallbacks=True)
        if not text:
            break
        urls = re.findall(r"<loc>([^<]+)</loc>", text)
        if not urls:
            break
        filtered = [
            u for u in urls
            if re.match(r"https://ideamensch\.com/[a-z0-9][a-z0-9\-]+/?$", u)
            and not any(x in u for x in ["/category/", "/tag/", "/page/", "/author/", "/about"])
            and 1 <= u.rstrip("/").split("/")[-1].count("-") <= 5
        ]
        all_urls.extend(filtered)
        # Stop if a sitemap page returns 0 matching URLs (hit the end)
        if not filtered and n > 2:
            break
    return all_urls


async def wordpress_sitemap_urls(site: str, max_pages: int = 10) -> list[str]:
    """Generic WordPress post-sitemap collector (paginated post-sitemapN.xml).
    Used for CEO Weekly, Famous Times, etc."""
    all_urls = []
    for n in range(1, max_pages + 1):
        text = await fetch(f"https://{site}/post-sitemap{n}.xml", try_fallbacks=True)
        if not text:
            break
        urls = re.findall(r"<loc>([^<]+)</loc>", text)
        if not urls:
            break
        # Keep only URLs that have at least 4 hyphens in the slug (filters category/tag pages)
        urls = [u for u in urls
                if re.match(rf"https?://(?:www\.)?{re.escape(site)}/[^/]+/?$", u)
                and u.rstrip("/").split("/")[-1].count("-") >= 4]
        all_urls.extend(urls)
    return all_urls


PR_SITES = [
    # Original interview/PR sites (founders, CEOs)
    "ceoweekly.com", "famoustimes.com", "disruptmagazine.com", "ceomonthly.com",
    "americanentrepreneurship.com", "ceoblognation.com",
    # Broader entrepreneur + niche-professional interview sites
    "addicted2success.com",       # entrepreneurs, coaches, mindset
    "thriveglobal.com",           # wellness, coaches, executives
    "beingentrepreneur.com",      # SMB founders across niches
    "gritdaily.com",              # startups, agencies, creatives
    "influencive.com",            # personal brand, coaching, marketing
]


async def collect_all_urls() -> dict[str, list[str]]:
    """Returns dict of source_name -> list of URLs."""
    out = {}
    out["canvasrebel"] = await canvasrebel_urls()
    out["boldjourney"] = await boldjourney_urls()
    out["valiantceo"] = await valiantceo_urls()
    out["founderhour"] = await founderhour_urls()
    out["authority_magazine"] = await authority_magazine_urls()
    out["brainz_magazine"] = await brainz_urls()
    out["ideamensch"] = await ideamensch_urls()
    for site in VOYAGE_SITES:
        out[site.replace(".com", "")] = await voyage_urls(site)
    for site in SHOUTOUT_SITES:
        out[site.replace(".com", "")] = await shoutout_urls(site)
    for site in PR_SITES:
        out[site.replace(".com", "")] = await wordpress_sitemap_urls(site)
    total = sum(len(v) for v in out.values())
    log.info(f"Collected {total} URLs across {len(out)} sources")
    return out


def source_label(url: str) -> str:
    if "boldjourney" in url: return "BoldJourney"
    if "canvasrebel" in url: return "CanvasRebel"
    if "ceoweekly" in url: return "CEOWeekly"
    if "famoustimes" in url: return "FamousTimes"
    if "disruptmagazine" in url: return "DisruptMagazine"
    if "valiantceo" in url: return "ValiantCEO"
    if "ceomonthly" in url: return "CEOMonthly"
    if "ceoblognation" in url: return "CEOBlogNation"
    if "thefounderhour" in url: return "TheFounderHour"
    if "americanentrepreneurship" in url: return "AmericanEntrepreneurship"
    if "ideamensch" in url: return "IdeaMensch"
    if "addicted2success" in url: return "Addicted2Success"
    if "thriveglobal" in url: return "ThriveGlobal"
    if "beingentrepreneur" in url: return "BeingEntrepreneur"
    if "gritdaily" in url: return "GritDaily"
    if "influencive" in url: return "Influencive"
    if "medium.com/authority-magazine" in url or "authority-magazine" in url: return "AuthorityMagazine"
    if "brainzmagazine" in url: return "Brainz"
    for s in SHOUTOUT_SITES:
        if s in url:
            return s.replace(".com", "").replace("shoutout", "ShoutOut").title()
    for s in VOYAGE_SITES:
        if s in url:
            return s.replace(".com", "").replace("voyage", "Voyage").title()
    return "Other"
