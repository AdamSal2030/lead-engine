from __future__ import annotations
"""Source sitemap collectors. Each returns list[str] of founder-interview URLs."""
import re
import httpx
import logging

log = logging.getLogger("sources")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml", "Accept-Language": "en-US,en;q=0.9"}


async def fetch(url: str, timeout: int = 20) -> str | None:
    try:
        async with httpx.AsyncClient(headers=HEADERS, timeout=timeout, follow_redirects=True) as cli:
            r = await cli.get(url)
            if r.status_code == 200:
                return r.text
    except Exception as e:
        log.debug(f"fetch fail {url}: {e}")
    return None


async def canvasrebel_urls() -> list[str]:
    text = await fetch("https://canvasrebel.com/post-sitemap.xml")
    if not text:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", text)
    return [u for u in urls if re.match(r"https://canvasrebel\.com/meet-[^/]+/?$", u)]


async def boldjourney_urls() -> list[str]:
    text = await fetch("https://boldjourney.com/news-sitemap.xml")
    if not text:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", text)
    return [u for u in urls if re.match(r"https://boldjourney\.com/meet-[^/]+/?$", u)]


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
    "voyagela.com", "voyageatl.com", "voyagemia.com", "voyagedallas.com",
    "voyagehouston.com", "voyageraleigh.com", "voyagestl.com", "voyagekc.com",
    "voyageaustin.com", "voyagechicago.com", "voyageohio.com", "voyageminnesota.com",
    "voyageutah.com", "voyagebaltimore.com", "voyagecharlotte.com",
    "voyagevirginia.com", "voyagewisconsin.com", "voyagewashington.com",
    "voyagealabama.com", "voyagemichigan.com",
]


async def voyage_urls(site: str) -> list[str]:
    text = await fetch(f"https://{site}/post-sitemap.xml")
    if not text:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", text)
    pat = re.compile(rf"https://{re.escape(site)}/\d{{4}}/\d{{2}}/\d{{2}}/[^/]+/?$")
    return [u for u in urls if pat.match(u) and u.rstrip("/").split("/")[-1].count("-") >= 5]


async def wordpress_sitemap_urls(site: str, max_pages: int = 10) -> list[str]:
    """Generic WordPress post-sitemap collector (paginated post-sitemapN.xml).
    Used for CEO Weekly, Famous Times, etc."""
    all_urls = []
    for n in range(1, max_pages + 1):
        text = await fetch(f"https://{site}/post-sitemap{n}.xml")
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


PR_SITES = ["ceoweekly.com", "famoustimes.com", "disruptmagazine.com"]


async def collect_all_urls() -> dict[str, list[str]]:
    """Returns dict of source_name -> list of URLs."""
    out = {}
    out["canvasrebel"] = await canvasrebel_urls()
    out["boldjourney"] = await boldjourney_urls()
    out["valiantceo"] = await valiantceo_urls()
    for site in VOYAGE_SITES:
        out[site.replace(".com", "")] = await voyage_urls(site)
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
    for s in VOYAGE_SITES:
        if s in url:
            return s.replace(".com", "").replace("voyage", "Voyage").title()
    return "Other"
