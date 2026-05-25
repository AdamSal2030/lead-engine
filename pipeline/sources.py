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
    # New York
    "voyageny.com",
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
    for n in range(1, 60):
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


async def wordpress_sitemap_urls(site: str, max_pages: int = 25,
                                  strict: bool = False) -> list[str]:
    """Generic WordPress post-sitemap collector (paginated post-sitemapN.xml).

    strict=False (default): require 4+ hyphens in slug (filters category/tag pages)
    strict=True: ALSO require at least one interview keyword in the slug
                 (use for news-wire sites that mix interviews with plain news articles)
    """
    INTERVIEW_SLUG_KEYWORDS = (
        "meet-", "founder", "ceo", "owner", "entrepreneur", "coach",
        "consultant", "realtor", "attorney", "therapist", "photographer",
        "designer", "author", "speaker", "interview", "spotlight", "profile",
        "podcast", "creative", "artist", "blogger", "influencer",
    )
    all_urls = []
    for n in range(1, max_pages + 1):
        text = await fetch(f"https://{site}/post-sitemap{n}.xml", try_fallbacks=True)
        if not text:
            break
        urls = re.findall(r"<loc>([^<]+)</loc>", text)
        if not urls:
            break
        filtered = []
        for u in urls:
            slug = u.rstrip("/").split("/")[-1].lower()
            if not re.match(rf"https?://(?:www\.)?{re.escape(site)}/[^/]+/?$", u):
                continue
            if slug.count("-") < 4:
                continue
            if strict and not any(kw in slug for kw in INTERVIEW_SLUG_KEYWORDS):
                continue
            filtered.append(u)
        all_urls.extend(filtered)
    return all_urls


# High-quality dedicated interview sites — standard filter (4+ hyphens is enough)
PR_SITES = [
    "ceoweekly.com", "famoustimes.com", "disruptmagazine.com", "ceomonthly.com",
    "americanentrepreneurship.com", "ceoblognation.com",
    "addicted2success.com",
    "thriveglobal.com",
    "beingentrepreneur.com",
    "gritdaily.com",
    "influencive.com",
]

# NewsAnchored network — mix of interview profiles + press releases.
# Use strict=True URL filter to only pick up article slugs that look like profiles.
PR_SITES_STRICT = [
    "nyweekly.com",        "lawire.com",           "kivodaily.com",
    "usinsider.com",       "usbusinessnews.com",   "worldreporter.com",
    "marketdaily.com",     "economicinsider.com",  "portlandnews.com",
    "miamiwire.com",       "nywire.com",           "atlwire.com",
    "texastoday.com",      "sanfranciscopost.com", "cagazette.com",
    "californiaobserver.com", "thechicagojournal.com", "womensjournal.com",
    "blknews.com",         "influencerdaily.com",  "artistweekly.com",
    "usreporter.com",      "theamericannews.com",  "realestatetoday.com",
]


async def clutch_urls(max_pages: int = 20) -> list[str]:
    """Clutch.co company directory profiles — non-published founders/agency owners.
    Uses the Clutch sitemap to find company profile pages."""
    all_urls = []
    # Clutch has a sitemap index; try profile sitemaps
    text = await fetch("https://clutch.co/sitemap_index.xml", try_fallbacks=True)
    if text:
        sub_sitemaps = re.findall(r"<loc>([^<]+profile[^<]*)</loc>", text)
        for sm in sub_sitemaps[:max_pages]:
            sm_text = await fetch(sm, try_fallbacks=True)
            if not sm_text:
                continue
            urls = re.findall(r"<loc>([^<]+)</loc>", sm_text)
            for u in urls:
                if re.match(r"https://clutch\.co/profile/[a-z0-9\-]+/?$", u):
                    all_urls.append(u)
    # Fallback: try direct profile sitemap pages
    if not all_urls:
        for n in range(1, max_pages + 1):
            text = await fetch(f"https://clutch.co/sitemap_profiles_{n}.xml", try_fallbacks=True)
            if not text:
                break
            urls = re.findall(r"<loc>([^<]+)</loc>", text)
            filtered = [u for u in urls if re.match(r"https://clutch\.co/profile/[a-z0-9\-]+/?$", u)]
            all_urls.extend(filtered)
            if not filtered:
                break
    log.info(f"Clutch: found {len(all_urls)} profile URLs")
    return all_urls[:2000]  # cap to prevent overwhelming the queue


async def indiehackers_urls() -> list[str]:
    """IndieHackers founder product pages — bootstrapped/indie founders."""
    all_urls = []
    for n in range(1, 10):
        text = await fetch(f"https://www.indiehackers.com/post-sitemap{n}.xml", try_fallbacks=True)
        if not text:
            # Try main sitemap
            text = await fetch("https://www.indiehackers.com/sitemap.xml", try_fallbacks=True)
            if not text:
                break
            urls = re.findall(r"<loc>([^<]+)</loc>", text)
            # Product/interview pages
            filtered = [u for u in urls
                        if re.match(r"https://www\.indiehackers\.com/product/[a-z0-9\-]+/?$", u)
                        or re.match(r"https://www\.indiehackers\.com/interview/[a-z0-9\-]+/?$", u)]
            all_urls.extend(filtered)
            break
        urls = re.findall(r"<loc>([^<]+)</loc>", text)
        filtered = [u for u in urls
                    if re.match(r"https://www\.indiehackers\.com/(product|interview)/[a-z0-9\-]+/?$", u)]
        all_urls.extend(filtered)
        if not filtered:
            break
    log.info(f"IndieHackers: found {len(all_urls)} URLs")
    return all_urls[:1000]


async def designrush_urls(max_pages: int = 12) -> list[str]:
    """DesignRush agency directory — design, marketing, tech agencies."""
    all_urls = []
    for n in range(1, max_pages + 1):
        text = await fetch(f"https://www.designrush.com/sitemap{n}.xml", try_fallbacks=True)
        if not text:
            text = await fetch("https://www.designrush.com/sitemap.xml", try_fallbacks=True)
        if not text:
            break
        urls = re.findall(r"<loc>([^<]+)</loc>", text)
        filtered = [u for u in urls
                    if re.match(r"https://www\.designrush\.com/agency/[a-z0-9\-]+/[a-z0-9\-]+/?$", u)]
        all_urls.extend(filtered)
        if not filtered and n > 1:
            break
    log.info(f"DesignRush: found {len(all_urls)} URLs")
    return all_urls[:1000]


async def hackernews_showhn_urls(limit: int = 600) -> list[str]:
    """Hacker News 'Show HN' posts — founders showing real products they built.

    Uses the public HN Firebase REST API (no auth, no credits). Each story
    includes the product URL (the founder's actual website) and their HN
    username. directory_parser.parse_hn_showhn() handles extraction.
    """
    try:
        async with httpx.AsyncClient(timeout=20, headers=HEADERS) as client:
            r = await client.get(
                "https://hacker-news.firebaseio.com/v0/showstories.json"
            )
            if r.status_code != 200:
                log.warning(f"HN ShowHN API returned {r.status_code}")
                return []
            ids = r.json()[:limit]
            urls = [f"https://news.ycombinator.com/item?id={sid}" for sid in ids]
            log.info(f"HN ShowHN: found {len(urls)} stories")
            return urls
    except Exception as e:
        log.warning(f"HN ShowHN error: {e}")
        return []


async def betalist_urls(max_pages: int = 20) -> list[str]:
    """BetaList startup directory — pre-launch startups with founder websites.

    BetaList publishes an RSS feed and has individual startup pages. Each
    startup page has a direct link to the founder's product/company site.
    """
    all_urls = []
    # Try RSS first (structured, reliable)
    try:
        text = await fetch("https://betalist.com/startups.rss", try_fallbacks=True)
        if text:
            urls = re.findall(r"<link>([^<]+)</link>", text)
            filtered = [u.strip() for u in urls
                        if re.match(r"https://betalist\.com/startups/[a-z0-9\-]+/?$", u.strip())]
            all_urls.extend(filtered)
    except Exception:
        pass
    # Sitemap fallback
    if not all_urls:
        text = await fetch("https://betalist.com/sitemap.xml", try_fallbacks=True)
        if text:
            urls = re.findall(r"<loc>([^<]+)</loc>", text)
            filtered = [u for u in urls
                        if re.match(r"https://betalist\.com/startups/[a-z0-9\-]+/?$", u)]
            all_urls.extend(filtered)
    # Paginated browse pages as last resort
    if not all_urls:
        for n in range(1, max_pages + 1):
            suffix = f"?page={n}" if n > 1 else ""
            text = await fetch(f"https://betalist.com/startups{suffix}", try_fallbacks=True)
            if not text:
                break
            found = re.findall(r'href="(/startups/[a-z0-9\-]+)"', text)
            page_urls = [f"https://betalist.com{slug}" for slug in set(found)]
            all_urls.extend(page_urls)
            if not found:
                break
    all_urls = list(dict.fromkeys(all_urls))  # dedupe preserving order
    log.info(f"BetaList: found {len(all_urls)} URLs")
    return all_urls[:500]


async def trustpilot_urls(max_sitemaps: int = 10) -> list[str]:
    """Trustpilot business review pages — millions of small businesses worldwide.

    KEY TRICK: the company domain is embedded directly in the URL:
      https://www.trustpilot.com/review/acme.com  →  domain = acme.com
    This means the parser can skip website extraction entirely and use the
    domain from the URL to generate email patterns immediately. No Claude needed.
    """
    all_urls: list[str] = []
    # Trustpilot publishes paginated business-entity sitemaps
    sitemap_index = await fetch("https://www.trustpilot.com/sitemap.xml", try_fallbacks=True)
    sub_sitemaps: list[str] = []
    if sitemap_index:
        sub_sitemaps = re.findall(
            r"<loc>(https://www\.trustpilot\.com/sitemap[^<]*business[^<]*)</loc>",
            sitemap_index,
        )
    if not sub_sitemaps:
        # Known pattern as fallback
        sub_sitemaps = [
            f"https://www.trustpilot.com/sitemap_business-entity_{n}.xml"
            for n in range(1, max_sitemaps + 1)
        ]
    for sm in sub_sitemaps[:max_sitemaps]:
        text = await fetch(sm, try_fallbacks=True)
        if not text:
            continue
        urls = re.findall(r"<loc>([^<]+)</loc>", text)
        for u in urls:
            # Only keep review pages (domain is in the path)
            m = re.match(
                r"https://www\.trustpilot\.com/review/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,})/?$",
                u,
            )
            if m:
                all_urls.append(u)
    all_urls = list(dict.fromkeys(all_urls))
    log.info(f"Trustpilot: found {len(all_urls)} business URLs")
    return all_urls[:8000]


async def appsumo_urls(max_pages: int = 20) -> list[str]:
    """AppSumo product listing pages — indie SaaS founders selling lifetime deals.

    AppSumo pages are server-side rendered and each product has a direct link
    to the company/product website. Great for reaching bootstrapped founders
    who haven't been featured in traditional interview sites.
    """
    all_urls: list[str] = []
    # Try sitemap first
    text = await fetch("https://appsumo.com/sitemap.xml", try_fallbacks=True)
    if text:
        sub = re.findall(r"<loc>([^<]+)</loc>", text)
        product_sitemaps = [u for u in sub if "product" in u.lower()]
        for sm in product_sitemaps[:max_pages]:
            sm_text = await fetch(sm, try_fallbacks=True)
            if not sm_text:
                continue
            urls = re.findall(r"<loc>([^<]+)</loc>", sm_text)
            filtered = [
                u for u in urls
                if re.match(r"https://appsumo\.com/products/[a-z0-9\-]+/?$", u)
            ]
            all_urls.extend(filtered)
    # Fallback: paginated browse
    if not all_urls:
        for page in range(1, max_pages + 1):
            suffix = f"?page={page}" if page > 1 else ""
            text = await fetch(f"https://appsumo.com/browse/{suffix}", try_fallbacks=True)
            if not text:
                break
            found = re.findall(r'href="(/products/[a-z0-9\-]+)"', text)
            page_urls = list({f"https://appsumo.com{slug}" for slug in found})
            all_urls.extend(page_urls)
            if not found:
                break
    all_urls = list(dict.fromkeys(all_urls))
    log.info(f"AppSumo: found {len(all_urls)} product URLs")
    return all_urls[:3000]


async def goodfirms_urls(max_pages: int = 15) -> list[str]:
    """GoodFirms company directory — IT and software agencies with founder info.

    Similar to Clutch but covers a different pool of agencies. Has a public
    sitemap with company profile pages.
    """
    all_urls = []
    text = await fetch("https://www.goodfirms.co/sitemap.xml", try_fallbacks=True)
    if text:
        sub = re.findall(r"<loc>([^<]*company[^<]*)</loc>", text)
        for sm_url in sub[:max_pages]:
            sm_text = await fetch(sm_url, try_fallbacks=True)
            if not sm_text:
                continue
            urls = re.findall(r"<loc>([^<]+)</loc>", sm_text)
            filtered = [u for u in urls
                        if re.match(r"https://www\.goodfirms\.co/company/[a-z0-9\-]+/?$", u)]
            all_urls.extend(filtered)
    # Direct paginated sitemap fallback
    if not all_urls:
        for n in range(1, max_pages + 1):
            sm_text = await fetch(
                f"https://www.goodfirms.co/sitemap_companies_{n}.xml", try_fallbacks=True
            )
            if not sm_text:
                break
            urls = re.findall(r"<loc>([^<]+)</loc>", sm_text)
            filtered = [u for u in urls
                        if re.match(r"https://www\.goodfirms\.co/company/[a-z0-9\-]+/?$", u)]
            all_urls.extend(filtered)
            if not filtered:
                break
    log.info(f"GoodFirms: found {len(all_urls)} URLs")
    return all_urls[:2000]


async def collect_all_urls() -> dict[str, list[str]]:
    """Returns dict of source_name -> list of URLs.

    All sources are fetched CONCURRENTLY (asyncio.gather) so total wall-time
    ≈ slowest individual source rather than sum-of-all. Each source gets a
    90-second timeout — long enough for paginated sitemaps, short enough to
    keep the batch startup snappy.
    """
    import asyncio as _asyncio

    SOURCE_TIMEOUT = 90  # seconds per source

    async def safe(name: str, coro):
        """Run a source coroutine with a timeout. Returns (name, result)."""
        try:
            urls = await _asyncio.wait_for(coro, timeout=SOURCE_TIMEOUT)
            return name, (urls or [])
        except _asyncio.TimeoutError:
            log.warning(f"Source {name} timed out after {SOURCE_TIMEOUT}s")
            return name, []
        except Exception as exc:
            log.warning(f"Source {name} error: {exc}")
            return name, []

    tasks = []
    # Core interview sources
    tasks.append(safe("canvasrebel", canvasrebel_urls()))
    tasks.append(safe("boldjourney", boldjourney_urls()))
    tasks.append(safe("valiantceo", valiantceo_urls()))
    tasks.append(safe("founderhour", founderhour_urls()))
    tasks.append(safe("authority_magazine", authority_magazine_urls()))
    tasks.append(safe("brainz_magazine", brainz_urls()))
    tasks.append(safe("ideamensch", ideamensch_urls()))
    # Voyage network (32 sites)
    for site in VOYAGE_SITES:
        tasks.append(safe(site.replace(".com", ""), voyage_urls(site)))
    # ShoutOut network (5 sites)
    for site in SHOUTOUT_SITES:
        tasks.append(safe(site.replace(".com", ""), shoutout_urls(site)))
    # PR / interview magazine sites
    for site in PR_SITES:
        tasks.append(safe(site.replace(".com", ""), wordpress_sitemap_urls(site)))
    # NewsAnchored network — strict slug filter to avoid plain news articles
    for site in PR_SITES_STRICT:
        tasks.append(safe(site.replace(".com", ""), wordpress_sitemap_urls(site, strict=True)))
    # Non-published directory sources
    tasks.append(safe("clutch", clutch_urls()))
    tasks.append(safe("indiehackers", indiehackers_urls()))
    tasks.append(safe("designrush", designrush_urls()))
    # Zero-Claude founder sources (API/structured data, no interview scraping)
    tasks.append(safe("hackernews", hackernews_showhn_urls()))
    tasks.append(safe("betalist", betalist_urls()))
    tasks.append(safe("goodfirms", goodfirms_urls()))
    tasks.append(safe("trustpilot", trustpilot_urls()))
    tasks.append(safe("appsumo", appsumo_urls()))

    results = await _asyncio.gather(*tasks)
    out = {name: urls for name, urls in results}

    total = sum(len(v) for v in out.values())
    active = sum(1 for v in out.values() if v)
    log.info(f"Collected {total} URLs across {active}/{len(out)} active sources")
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
    # NewsAnchored network
    if "nyweekly" in url: return "NYWeekly"
    if "lawire" in url: return "LAWire"
    if "kivodaily" in url: return "KivoDaily"
    if "usinsider" in url: return "USInsider"
    if "usbusinessnews" in url: return "USBusinessNews"
    if "worldreporter" in url: return "WorldReporter"
    if "marketdaily" in url: return "MarketDaily"
    if "economicinsider" in url: return "EconomicInsider"
    if "portlandnews" in url: return "PortlandNews"
    if "miamiwire" in url: return "MiamiWire"
    if "nywire" in url: return "NYWire"
    if "atlwire" in url: return "ATLWire"
    if "texastoday" in url: return "TexasToday"
    if "sanfranciscopost" in url: return "SanFranciscoPost"
    if "cagazette" in url: return "CAGazette"
    if "californiaobserver" in url: return "CaliforniaObserver"
    if "thechicagojournal" in url: return "ChicagoJournal"
    if "womensjournal" in url: return "WomensJournal"
    if "blknews" in url: return "BLKNews"
    if "influencerdaily" in url: return "InfluencerDaily"
    if "artistweekly" in url: return "ArtistWeekly"
    if "usreporter" in url: return "USReporter"
    if "theamericannews" in url: return "TheAmericanNews"
    if "realestatetoday" in url: return "RealEstateToday"
    # Directory sources
    if "clutch.co" in url: return "Clutch"
    if "indiehackers.com" in url: return "IndieHackers"
    if "designrush.com" in url: return "DesignRush"
    if "news.ycombinator.com" in url: return "HackerNews"
    if "betalist.com" in url: return "BetaList"
    if "goodfirms.co" in url: return "GoodFirms"
    if "trustpilot.com" in url: return "Trustpilot"
    if "appsumo.com" in url: return "AppSumo"
    for s in SHOUTOUT_SITES:
        if s in url:
            return s.replace(".com", "").replace("shoutout", "ShoutOut").title()
    for s in VOYAGE_SITES:
        if s in url:
            return s.replace(".com", "").replace("voyage", "Voyage").title()
    return "Other"
