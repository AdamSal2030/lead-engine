from __future__ import annotations
"""Parse founder interview pages, extract name + website + role + emails."""
import re
import urllib.parse
import httpx
import logging
from bs4 import BeautifulSoup
from pipeline.sources import HEADERS, source_label

log = logging.getLogger("parser")

EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b")

NON_PERSON = {
    # Articles / determiners
    "the","a","an","this","that","these","those","my","your","our","their",
    # Prepositions
    "of","from","to","in","on","at","by","for","with","without","about","into",
    "through","during","before","after","above","below","under","over","across",
    "behind","beyond","upon",
    # Conjunctions
    "and","or","but","nor","so","yet","than","because","while","whereas",
    # Question words
    "why","how","what","when","where","who","whom","whose","which","whether",
    # Comparatives / adjectives in titles
    "more","less","most","least","better","best","worse","worst","top","greatest",
    "amazing","incredible","essential","important","key","vital","crucial",
    "ultimate","complete","total","new","newest","latest","first","last","next",
    "biggest","smallest","cheapest","easiest","hardest",
    # Action / -ing verbs (article-title style)
    "building","creating","designing","exploring","finding","growing","scaling",
    "leading","making","running","starting","launching","raising","selling","buying",
    "managing","fixing","solving","tackling","facing","overcoming","achieving",
    "transforming","reinventing","disrupting","revolutionizing","modernizing",
    "thinking","writing","reading","working","living","being","becoming","doing",
    "going","coming","getting","giving","taking","keeping","holding","using",
    "succeeding","mastering","beating","winning","losing","helping","building",
    "understanding","knowing","learning","teaching","training","practicing",
    "navigating","embracing","celebrating","introducing","welcoming","meeting","greeting",
    # Title nouns
    "story","stories","journey","journeys","path","paths","road","way","ways",
    "guide","guides","tutorial","lesson","lessons","tip","tips","secret","secrets",
    "rule","rules","step","steps","method","methods","framework","frameworks",
    "system","systems","strategy","strategies","tactic","tactics","approach","approaches",
    "principle","principles","practice","practices","habit","habits",
    "business","businesses","company","companies","brand","brands","startup","startups",
    "founder","founders","entrepreneur","entrepreneurs","leader","leaders",
    "ceo","cto","cmo","coo","cfo","cio","vp","manager","director","executive",
    "owner","owners","employee","employees","worker","workers","team","teams",
    "client","clients","customer","customers","user","users","member","members",
    "expert","experts","professional","professionals","specialist","specialists",
    "consultant","consultants","coach","coaches","mentor","mentors","advisor","advisors",
    "investor","investors","partner","partners","stakeholder","stakeholders",
    "marketing","sales","finance","operations","technology","tech","engineering",
    "product","products","service","services","industry","industries","market","markets",
    "economy","commerce","trade","retail","wholesale",
    "ai","ml","data","analytics","insight","insights","metric","metrics","kpi",
    "growth","scale","expansion","launch","release","rollout",
    # Generic
    "small","big","large","global","local","national","international",
    "exclusive","conversations","conversation","interview","interviews","feature","featured",
    "spotlight","spotlights","profile","profiles","portrait","portraits",
    "meet","meets","welcome",
    "inspirational","inspiring","authentic","real","true","powerful",
    "amazing","awesome","brilliant","creative","creatives","innovative","innovator",
    "successful","success","fail","failure","mistake","mistakes",
    "art","artist","artists","artistry","craft","crafts","design","designer","designers",
    "creators","creating",
    "weekly","daily","monthly","yearly","annual","quarterly","past","future","present",
    "today","tomorrow","yesterday","now","then","later","sooner","longer","shorter",
    "morning","afternoon","evening","night","day","week","month","year",
    "early","late","fast","slow","quick","rapid",
    "one","two","three","four","five","six","seven","eight","nine","ten","hundred","thousand","million",
    "open","closed","public","private","secret","hidden","visible","invisible",
    "good","bad","great","poor","excellent","terrible",
    "front","side","middle","center","corner",
    "fake","false","authentic","genuine","original","copy","real",
    # Industries
    "fitness","health","wellness","therapy","mental","physical","spiritual",
    "luxury","premium","budget","affordable","high-end",
    "asset","assets","investment","investments","portfolio","portfolios",
    "menu","order","food","restaurant","dining","kitchen",
    "law","legal","attorney","lawyer","compliance","regulation",
    "operational","performance","productive","efficient",
    "architecture","architectural",
    "jewelry","fashion","beauty","cosmetic","makeup","skincare",
    "research","studies","study","analysis","review","reviews","report","reports",
    "just","only","even","still","yet","also","heart","gift","portrait",
    # Specific noisy categories I caught
    "introverted","power","behind","check","read","perspectives","thoughts","insights",
    "podcast","podcasts","introducing","place","places","shop","shops","tour","tours",
    "events","life",
}

JUNK_DOMAINS = {"sentry.io", "wixpress.com", "example.com", "domain.com", "test.com",
                "web.archive.org", "archive.org"}
JUNK_DOMAIN_SUBSTR = [
    "sentry.io", "wixpress.com", "godaddy.com", "shopify.com", "squarespace.com",
    "cloudflare", "amazonaws", "cloudfront", "google-analytics", "googletagmanager",
    "doubleclick", "akamai", "ingest.", "tracking.", "analytics.", "cdn.", "static.",
    "hubspot.com", "mailchimp.com", "constantcontact.com", "sendgrid.net",
    "mailgun.org", "postmark", "mandrillapp", "gravatar.com", "wp.com", "jetpack.com",
]
JUNK_LOCALS = {"noreply", "no-reply", "donotreply", "wordpress", "test", "example",
               "name", "yourname", "email", "youremail", "sentry"}

SOCIALS = ["instagram.com","facebook.com","tiktok.com","youtube.com","linkedin.com",
           "twitter.com","x.com","apple.com","spotify.com","amazon.com","soundcloud.com",
           "wiseinterviews.com","canvasinterviews.com","boldjourneymagazine.com",
           "voyageinterviews.com","etsy.com","pinterest.com","yelp.com","goodreads.com",
           "cash.app","venmo.com","paypal.com","patreon.com","threads.net",
           "wikipedia.org","goo.gl","bit.ly","linktr.ee","beacons.ai","stan.store",
           "later.com","mailchi.mp"]

HOST_BUILDERS = ["wixsite.com","squarespace.com","weebly.com","webnode.com",
                 "blogspot.com","wordpress.com","tumblr.com","medium.com",
                 "canvasinterviews.com","boldjourneymagazine.com","voyageinterviews.com",
                 "wiseinterviews.com","substack.com","carrd.co","linktr.ee"]


UA_LIST = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


# Sites known to block datacenter IPs — skip direct attempts, go straight to Wayback.
# Voyage/ShoutOut network is also blocked but detected dynamically via 403 response.
CLOUDFLARE_BLOCKED_HOSTS = ("canvasrebel.com", "boldjourney.com")
# Voyage + ShoutOut: all these block Railway/datacenter IPs — use Wayback directly
_WAYBACK_ONLY_PATTERNS = ("voyagela.com", "voyagemia.com", "voyagedallas.com",
                           "voyagehouston.com", "voyageraleigh.com", "voyagestl.com",
                           "voyagekc.com", "voyageaustin.com", "voyagechicago.com",
                           "voyageohio.com", "voyageminnesota.com", "voyageutah.com",
                           "voyagebaltimore.com", "voyagecharlotte.com", "voyagevirginia.com",
                           "voyagewisconsin.com", "voyagewashington.com", "voyagealabama.com",
                           "voyagemichigan.com", "voyagephoenix.com", "voyagedenver.com",
                           "voyagesf.com", "voyagerichmond.com", "voyageindy.com",
                           "voyagesd.com", "voyagememphis.com", "voyagephilly.com",
                           "voyagenashville.com", "voyageportland.com", "voyageseattle.com",
                           "voyageatl.com", "voyageny.com",
                           "shoutoutla.com", "shoutoutatl.com", "shoutoutdfw.com",
                           "shoutoutsocal.com", "shoutoutnorcal.com")


async def fetch(url: str, timeout: int = 15) -> str | None:
    """Try direct fetch with UA rotation, then fall back to Wayback Machine on 403/block."""
    from pipeline.netutil import proxy_client_kwargs
    blocked = any(h in url for h in CLOUDFLARE_BLOCKED_HOSTS + _WAYBACK_ONLY_PATTERNS)
    pkw = proxy_client_kwargs(url)

    # Blocked host WITH a proxy → fetch LIVE through the residential proxy
    # (preferred over stale Wayback). Falls through to Wayback if the proxy fails.
    if blocked and pkw:
        for ua in UA_LIST[:3]:
            try:
                h = {"User-Agent": ua, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}
                async with httpx.AsyncClient(headers=h, timeout=timeout,
                                             follow_redirects=True, **pkw) as cli:
                    r = await cli.get(url)
                    if r.status_code == 200 and len(r.text) > 200:
                        return r.text
            except Exception:
                continue

    # Blocked host (no proxy, or proxy attempt failed) → Wayback Machine
    if blocked:
        try:
            wb_url = f"https://web.archive.org/web/2026/{url}"
            async with httpx.AsyncClient(
                headers={"User-Agent": UA_LIST[0]},
                timeout=30, follow_redirects=True,
            ) as cli:
                r = await cli.get(wb_url)
                if r.status_code == 200 and len(r.text) > 200:
                    return r.text
        except Exception:
            pass
        return None

    # Normal path: try direct fetch with UA rotation
    got_403 = False
    for ua in UA_LIST:
        try:
            h = {"User-Agent": ua, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}
            async with httpx.AsyncClient(headers=h, timeout=timeout, follow_redirects=True) as cli:
                r = await cli.get(url)
                if r.status_code == 200 and "text/html" in r.headers.get("content-type", "").lower():
                    return r.text
                if r.status_code in (403, 429, 503):
                    got_403 = True
        except Exception:
            continue

    # Auto-fallback: if all direct attempts were blocked, try Wayback Machine
    if got_403:
        try:
            wb_url = f"https://web.archive.org/web/2026/{url}"
            async with httpx.AsyncClient(
                headers={"User-Agent": UA_LIST[0]},
                timeout=30, follow_redirects=True,
            ) as cli:
                r = await cli.get(wb_url)
                if r.status_code == 200 and len(r.text) > 200:
                    return r.text
        except Exception:
            pass
    return None


def _try_parse_segment(seg: str) -> str | None:
    """Lower-level: try to clean one candidate segment into a real name. Returns name or None."""
    name = seg.strip()
    # Strip possessive 's, credentials, suffixes
    name = re.sub(r"['']s\b.*", "", name)
    # Strip comma-separated role/credential anywhere after first two words
    # e.g. "Jane Smith, CEO" → "Jane Smith" | "John Doe, Ph.D." → "John Doe"
    name = re.sub(r",\s*\S.*$", "", name).strip()
    # Strip standalone credential abbreviations.
    # IMPORTANT: named credentials use IGNORECASE; the catch-all MUST be case-sensitive
    # ([A-Z]{2,5} only) so that mixed-case last names (Smith, Johnson, Malhotra …)
    # are never mistaken for abbreviations.
    name = re.sub(r"\s+(?:MD|PhD|DDS|JD|MBA|CPA|RN|LCSW|MFT|ATR-BC|NCC|LPC|LMFT|CPCC|PCC|MCC)\s*$",
                  "", name, flags=re.IGNORECASE).strip()
    # All-caps abbreviations only — no IGNORECASE flag so lowercase letters disqualify the word
    name = re.sub(r"\s+[A-Z]{2,6}(?:-[A-Z]{2,6})?\s*$", "", name).strip()
    # Strip honorifics
    name = re.sub(r"^(?:Dr\.?|Mr\.?|Mrs\.?|Ms\.?|Prof\.?|Rev\.?|Chef|Coach|DJ)\s+",
                  "", name, flags=re.IGNORECASE)
    # Strip trailing role / title words
    name = re.sub(r"\s+(?:Photographer|Designer|Founder|Co-Founder|CEO|COO|CFO|CTO|CMO|"
                  r"Owner|Author|Artist|Coach|Consultant|Entrepreneur|Director|President|"
                  r"Esthetician|Therapist|Novelist|Filmmaker|Chef|Doctor|Lawyer|Attorney|"
                  r"Realtor|Stylist|Influencer|Speaker|Trainer|Blogger|Podcaster|"
                  r"Freelancer|Developer|Engineer|Architect|Nurse|Professor)\s*$",
                  "", name, flags=re.IGNORECASE).strip()
    # Cut at " of/from/on/at/with/in/and "
    name = re.split(r"\s+(?:of|from|on|at|with|in|and|&)\s+", name, flags=re.IGNORECASE)[0].strip()
    # Strip any trailing punctuation
    name = name.rstrip(".,;:-—")

    # Normalize ALL-CAPS or all-lowercase
    if name == name.upper() or name == name.lower():
        name = " ".join(w.capitalize() for w in name.split())

    words = name.split()
    if not (2 <= len(words) <= 4) or any(c.isdigit() for c in name):
        return None
    if not all(re.match(r"^[A-ZÀ-Ý][\w'\-À-ÿ]+$|^[A-Z]\.?$", w, re.UNICODE) for w in words):
        return None
    if any(w.lower().rstrip(".") in NON_PERSON for w in words):
        return None
    if any(c in name for c in '"""'):
        return None
    for w in words:
        if len(w) == 1 and w.isupper(): continue
        if len(w) < 2: return None
    return name


def clean_name(title_text: str) -> str | None:
    """Strict name extraction. Tries multiple segments when title uses 'Topic: Name' style."""
    if not title_text: return None
    name = title_text
    # Reject guide / list / faq articles outright
    rejects = [r"\bguide to\b", r"\blist of\b", r"\btop \d+", r"\bbest of\b",
               r"\bfaqs?\b", r"\bspotlight\b", r"\bportraits? of\b", r"\binspiring stories\b",
               r"\bhow to\b", r"\bwhy you\b", r"\bwhat is\b", r"\bwhat are\b",
               r"\b(?:5|6|7|8|9|10)\s+(?:ways|tips|tricks|things|lessons|reasons)\b"]
    for pat in rejects:
        if re.search(pat, name, re.IGNORECASE):
            return None

    # Strip leading interview-style prefixes (case-insensitive)
    PREFIXES = [
        "Meet ", "Inspiring Conversations with ", "Conversations with ",
        "Life & Work with ", "Hidden Gems: Meet ", "Daily Inspiration: Meet ",
        "Exclusive Interview with ", "Interview with ", "Artist of the Day: ",
        "Featured Founder: ", "Founder Spotlight: ", "Spotlight on ",
        "Getting to Know ", "Get to Know ", "Catching Up with ",
        "In Conversation with ", "A Conversation with ", "We Sat Down with ",
        "Check Out ", "Introducing ", "Meet the Founder: ", "Founder Feature: ",
        "CEO Spotlight: ", "Rising Star: ", "Community Spotlight: ",
        "Entrepreneur Spotlight: ", "Business Spotlight: ", "Q&A with ",
        "An Interview with ", "Today we'd like to introduce you to ",
        "Today we're proud to present ", "Today we're thrilled to present ",
        "We had the pleasure of interviewing ",
    ]
    name_lower = name.lower()
    for prefix in PREFIXES:
        if name_lower.startswith(prefix.lower()):
            name = name[len(prefix):]
            break

    # Try EACH segment of the title (split on |, –, —, :)
    # First half is typical ("Sarah Smith | Founder of X"), but second half
    # works for "Topic of the Day: Sam Castillo" patterns.
    segments = re.split(r"\s*[|–—:]\s*", name)
    for seg in segments:
        seg = seg.strip()
        if not seg: continue
        result = _try_parse_segment(seg)
        if result:
            return result
    return None


_WEBSITE_JUNK_SUBSTR = [
    # Ad/analytics/CDN
    "google", "facebook", "doubleclick", "googletagmanager", "google-analytics",
    "googleapis", "gstatic", "cloudflare", "amazonaws", "cloudfront", "akamai",
    "cdn.", "static.", "assets.", "tracking.", "analytics.", "pixel.", "ingest.",
    # Payments/CRM/tools that aren't the founder's site
    "stripe.com", "paypal.com", "typeform.com", "jotform.com", "mailchimp.com",
    "hubspot.com", "calendly.com", "eventbrite.com", "gofundme.com", "patreon.com",
    "substack.com", "beehiiv.com", "convertkit.com", "activecampaign.com",
    "clickfunnels.com", "kajabi.com", "teachable.com", "thinkific.com",
    "shopify.com", "squarespace.com", "wix.com", "wixsite.com", "weebly.com",
    "godaddy.com", "namecheap.com", "wordpress.com", "blogspot.com",
    # Media / aggregator sites (they write about the founder, not the founder's site)
    "forbes.com", "inc.com", "entrepreneur.com", "techcrunch.com", "medium.com",
    "linkedin.com", "twitter.com", "x.com", "instagram.com", "facebook.com",
    "youtube.com", "tiktok.com", "pinterest.com", "yelp.com", "amazon.com",
    # Misc junk
    "bit.ly", "tinyurl.com", "goo.gl", "t.co", "linktr.ee", "beacons.ai",
    "stan.store", "carrd.co", "notion.so", "airtable.com",
]


def find_website(body, body_text: str, page_url: str) -> str | None:
    """Find the founder's personal/business website URL in the article.

    Priority:
    1. Explicitly labelled website fields ("Website:", "Personal Website:", etc.)
    2. First clean external link — filtered aggressively to exclude junk/tools/media
    """
    # 1. Labelled website field (highest precision)
    for label in ["Personal Website:", "Website:", "Website : ",
                  "Business Website:", "Company Website:", "Web:"]:
        m = re.search(rf"{label}\s*([^\s\n,<>\"']+)", body_text)
        if m:
            w = m.group(1).strip().rstrip(".,;)/")
            if "." in w and " " not in w and 8 < len(w) < 80:
                return w if w.startswith("http") else "https://" + w

    # 2. External links — filter aggressively
    page_domain = page_url.split("/")[2].lower() if page_url.startswith("http") else ""

    candidates = []
    for a in body.find_all("a", href=True):
        h = a["href"].strip()
        if not h.startswith("http"):
            continue
        h_clean = h.split("?")[0].split("#")[0].rstrip("/")

        # Unwrap Wayback Machine URLs: https://web.archive.org/web/TIMESTAMP/ORIGINAL
        # These appear when pages are fetched via archive.org (Cloudflare-blocked hosts).
        if "web.archive.org/web/" in h_clean:
            wb_m = re.match(r"https?://web\.archive\.org/web/\d+/(https?://.+)", h_clean)
            if wb_m:
                h_clean = wb_m.group(1).split("?")[0].split("#")[0].rstrip("/")
            else:
                continue  # Unrecognised archive URL — skip

        # Skip same-domain links (back-links to interview site)
        link_domain = h_clean.split("/")[2].lower() if h_clean.startswith("http") else ""
        if page_domain and page_domain in link_domain:
            continue
        # Skip social media
        if any(s in link_domain for s in SOCIALS):
            continue
        # Skip known junk/tool/media domains
        if any(j in link_domain for j in _WEBSITE_JUNK_SUBSTR):
            continue
        # Must look like a real domain (not a path-only or bare word)
        if "." not in link_domain or len(link_domain) < 4:
            continue
        candidates.append(h_clean)

    if candidates:
        return candidates[0]

    # 3. Plain-text https:// URLs in article body (not wrapped in <a> tags).
    # Many articles mention the founder's site inline: "visit janesmith.com" or
    # paste a raw https:// link. This catches them without needing Claude.
    if body_text:
        _URL_RE = re.compile(
            r'https?://([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,})(/[^\s<>"\',;()\[\]{}]*)?'
        )
        for m in _URL_RE.finditer(body_text):
            full = m.group(0).rstrip('.,;)/\'"')
            domain = m.group(1).lower()
            if page_domain and page_domain in domain:
                continue
            if any(j in domain for j in _WEBSITE_JUNK_SUBSTR):
                continue
            if any(s in domain for s in SOCIALS):
                continue
            if len(domain) < 5 or "." not in domain:
                continue
            return full

    return None


def find_role(text: str) -> str | None:
    for r_word in ["Founder","Co-Founder","CEO","Owner","Author","Creator","Director",
                   "Principal","President","Coach","Consultant","Entrepreneur"]:
        if re.search(rf"\b{r_word}\b", text):
            return r_word
    return None


def find_company(text: str) -> str | None:
    """Extract company name from article text. Conservative — better to return None
    than capture garbled chunks like 'Houston Rockets Even Though High Street...'"""
    for pat in [r"\b(?:founder|co-founder|cofounder|owner|ceo|president|principal)\s+(?:and\s+\w+\s+)?of\s+([A-Z][A-Za-z0-9&\.\-' ]{1,60})",
                r"\b(?:I\s+(?:founded|started|launched|created)|I\s+am\s+the\s+founder\s+of)\s+([A-Z][A-Za-z0-9&\.\-' ]{1,60})"]:
        m = re.search(pat, text, re.IGNORECASE)
        if not m: continue
        c = m.group(1).strip().rstrip(".,;:")
        # Cut at common sentence-continuation words
        c = re.split(r"\s+(?:where|which|that|when|so|because|but|after|before|while|though|"
                     r"whose|whom|since|until|even|still|then|now|today|yesterday|"
                     r"is|are|was|were|has|have|had|with|to|from|in|on|at|of|by|for|"
                     r"about|over|under|through|across|behind|beyond|inside|outside)\s+",
                     c, flags=re.IGNORECASE, maxsplit=1)[0].strip().rstrip(".,;:&-")
        # Sanity checks — reject if it looks like a sentence fragment
        words = c.split()
        if not (1 <= len(words) <= 6): continue
        # Reject if mostly lowercase (means we captured running prose, not a proper noun company)
        cap_count = sum(1 for w in words if w and w[0].isupper())
        if cap_count < max(1, len(words) // 2): continue
        if 2 <= len(c) <= 60:
            return c
    return None


def _extract_text_for_claude(soup: BeautifulSoup, title_text: str,
                             body, url: str) -> str:
    """Extract clean, minimal text for Claude — ~500 tokens vs 2000+ for raw HTML.
    Sends: URL + page title + meta description + first 2500 chars of article text.

    When BeautifulSoup can't find the <article> element (body=None), falls back to
    stripping nav/footer/script/style tags and using the remaining page text.
    This captures content in non-standard layouts (Voyage, BoldJourney, etc.)
    """
    parts = [f"URL: {url}"]
    if title_text:
        parts.append(f"PAGE TITLE: {title_text}")
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        parts.append(f"META DESCRIPTION: {meta['content']}")

    if body:
        body_text = body.get_text(" ", strip=True)[:2500]
        parts.append(f"ARTICLE TEXT:\n{body_text}")
    else:
        # No <article> tag found — try common content containers, then fall back to
        # stripping known chrome elements from the full page.
        content = (
            soup.find("main")
            or soup.find(id=re.compile(r"content|main|post|article", re.I))
            or soup.find(class_=re.compile(r"content|main|post|article|body", re.I))
        )
        if content:
            page_text = content.get_text(" ", strip=True)[:2500]
        else:
            # Fallback: skip nav/footer/header text — check all ancestors, not just parent
            skip = {"nav", "footer", "header", "script", "style", "aside", "noscript"}
            chunks = []
            total = 0
            for el in soup.find_all(string=True):
                if total >= 2500:
                    break
                # Walk up the ancestor chain to check if any ancestor is a skip tag
                in_skip = False
                for ancestor in el.parents:
                    if ancestor.name in skip:
                        in_skip = True
                        break
                if in_skip:
                    continue
                text = el.strip()
                if text:
                    chunks.append(text)
                    total += len(text)
            page_text = " ".join(chunks)[:2500]
        if page_text:
            parts.append(f"PAGE TEXT:\n{page_text}")

        # Also extract external link URLs explicitly (href attributes aren't in text nodes)
        # This helps Claude find the founder's website even when it's only in an <a href>
        page_domain = url.split("/")[2].lower().replace("www.", "") if url.startswith("http") else ""
        ext_links = []
        for a in soup.find_all("a", href=True):
            h = a.get("href", "").strip()
            if not h.startswith("http"):
                continue
            try:
                link_domain = h.split("/")[2].lower().replace("www.", "")
            except IndexError:
                continue
            if link_domain == page_domain:
                continue
            if any(s in link_domain for s in SOCIALS):
                continue
            if any(j in link_domain for j in JUNK_DOMAIN_SUBSTR):
                continue
            ext_links.append(h.split("?")[0].split("#")[0])
        if ext_links:
            parts.append("EXTERNAL LINKS ON PAGE:\n" + "\n".join(ext_links[:10]))

    return "\n\n".join(parts)


def _is_interview_worthy(title_text: str, body_text: str, url: str = "") -> bool:
    """Quick pre-screen: is this page likely an interview/profile about a real person?
    Returns False for clear non-interview content (listicles, news, product pages).
    Saves a Claude call on pages that would return {"skip": true}.

    NOTE: this only fires when regex already failed — so we're already on a harder case.
    Be permissive rather than strict to avoid blocking legitimate interviews with
    unconventional HTML structures.
    """
    title_l = (title_text or "").lower()

    # ── 1. Strong URL signals — very high precision ──────────────────────────
    slug = url.rstrip("/").split("/")[-1].lower()
    if any(s in slug for s in ("meet-", "-meets-", "interview-", "spotlight-",
                                "founder-story", "our-story")):
        return True
    # Voyage / ShoutOut / similar date-prefixed articles often match "meet" in slug
    if slug.count("-") >= 4 and any(s in url.lower() for s in ("/meet-", "/interview-")):
        return True

    # ── 2. Strong title signals ───────────────────────────────────────────────
    title_signals = [
        "meet ", "meet: ", "interview with", "q&a with", "in conversation with",
        "we chatted with", "i sat down with", "founder spotlight", "founder story",
        "catching up with", "exclusive with", "get to know",
    ]
    if any(s in title_l for s in title_signals):
        return True

    # ── 3. Hard-reject obvious non-interview patterns ────────────────────────
    hard_rejects = [
        "how to ", "top 10 ", "top 5 ", "best of ", "guide to",
        "breaking:", "just in:", "stock market", "earnings report",
        " ways to ", " tips to ", " steps to ", " reasons to ",
        " things to ", " ways you ", " tips for ", "must-know",
        "the best ", "a guide ", "everything you", "what you need",
    ]
    if any(r in title_l for r in hard_rejects):
        return False
    # Digit-prefixed list articles: "5 Ways...", "10 Things...", "7 Tips..."
    if re.match(r"^\d+\s+(?:ways|tips|steps|reasons|things|secrets|lessons|habits)\b", title_l):
        return False

    # ── 4. If body is missing/tiny, trust the title + URL (don't over-reject) ─
    # CSS selector can fail on non-standard layouts; don't permanently discard
    if not body_text or len(body_text) < 100:
        # Allow Claude if the title has a capitalized two-word name pattern
        # (a heuristic for "Meet [Name]" style titles stripped of prefix)
        words = title_text.split()[:5] if title_text else []
        has_name_pattern = sum(1 for w in words if w and w[0].isupper() and len(w) > 2) >= 2
        return has_name_pattern

    # ── 5. Body text keyword check ────────────────────────────────────────────
    combined = title_l + " " + body_text[:1500].lower()
    body_signals = [
        "founder", "co-founder", "ceo", "owner", "entrepreneur", "startup",
        "coach", "consultant", "launched", "founded", "tell us", "tell me",
        "i started", "i built", "i created", "i founded", "my business",
        "my company", "my practice", "my agency", "our company", "our business",
        "photographer", "designer", "therapist", "realtor", "attorney",
        "chef ", "artist ", "author ", "speaker ", "trainer ", "blogger ",
        "podcaster", "influencer", "freelancer",
    ]
    return any(s in combined for s in body_signals)


async def parse_article(url: str) -> dict | None:
    """Returns dict with name, website, role, company, niche, hook, article_emails, or None.

    Extraction layers:
      1. Authority Magazine: RSS-cached name + Clearbit domain resolution
      2. Regex: H1 name + website scrape (fast, free, no API)
      3. Claude Haiku fallback: fires when regex can't find name or website
         — sends clean extracted text (~500 tokens, not raw HTML)
         — only fires when article passes interview pre-screen
         — respects CLAUDE_MAX_PER_DAY daily cap

    Returns {"_failed": "claude"} when regex fails AND Claude was tried but also
    failed — so the orchestrator can mark the URL as claude_no_parse (never retry).
    """
    from pipeline.sources import AUTHORITY_CACHE, source_label as _src

    # --- Authority Magazine special path ---
    if url in AUTHORITY_CACHE:
        cached = AUTHORITY_CACHE[url]
        from pipeline.company_resolver import resolve_to_domain
        domain = await resolve_to_domain(cached["company"])
        if not domain:
            return None
        from pipeline.niche import classify
        return {
            "source_url": url,
            "source": "AuthorityMagazine",
            "name": cached["name"],
            "website": f"https://{domain}",
            "role": "Founder",
            "company": cached["company"],
            "niche": classify("Founder", cached["company"], None, domain),
            "hook": "",
            "article_emails": [],
        }

    html = await fetch(url)
    if not html:
        return None

    if "brainzmagazine.com" in url:
        return None

    soup = BeautifulSoup(html, "lxml")
    title = soup.find("h1")
    title_text = title.get_text(strip=True) if title else ""

    name = clean_name(title_text)
    body = soup.find("article") or soup.find(class_=re.compile("entry-content|post-content"))
    body_text = body.get_text(" ", strip=True) if body else ""
    website = find_website(body, body_text, url) if body else None

    # --- Claude fallback when regex couldn't extract name or website ---
    if (not name or not website) and html:
        # Pre-screen: skip Claude call if page clearly isn't an interview/profile
        if not _is_interview_worthy(title_text, body_text, url):
            return None  # not an interview — mark no_parse (cheap, no Claude)

        from pipeline.claude_parser import parse_with_claude, _get_client, CAP_REACHED

        clean_text = _extract_text_for_claude(soup, title_text, body, url)
        claude_result = await parse_with_claude(url, clean_text)

        # Cap reached or no API key — Claude never fired. Keep URL as no_parse
        # so it gets retried next batch (NOT claude_no_parse which is permanent).
        if claude_result is CAP_REACHED:
            return None

        if claude_result:
            article_emails = extract_emails(str(body)) if body else set()
            from pipeline.niche import classify
            niche = claude_result.get("niche") or classify(
                claude_result.get("role"), claude_result.get("company"),
                None, claude_result.get("website"),
            )
            return {
                "source_url": url,
                "source": source_label(url),
                "name": claude_result["name"],
                "website": claude_result["website"],
                "role": claude_result.get("role", "Founder"),
                "company": claude_result.get("company", ""),
                "niche": niche,
                "hook": claude_result.get("hook", ""),
                "article_emails": sorted(article_emails),
                "_parsed_by": "claude",
            }
        # Claude was configured, actually tried, and genuinely failed — mark
        # as claude_no_parse so we don't waste Haiku credits retrying it.
        return {"_failed": "claude"}

    # name + website found via regex — body is optional (used only for email extraction)
    if not name or not website:
        return None

    article_emails = extract_emails(str(body)) if body else set()
    role = find_role(body_text) if body_text else None
    company = find_company(body_text) if body_text else None

    from pipeline.niche import classify
    niche = classify(role, company, None, website)

    return {
        "source_url": url,
        "source": source_label(url),
        "name": name,
        "website": website,
        "role": role,
        "company": company,
        "niche": niche,
        "hook": "",          # hook only available from Claude path
        "article_emails": sorted(article_emails),
        "_parsed_by": "regex",
    }


def extract_emails(html: str) -> set[str]:
    found = set(EMAIL_RE.findall(html))
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if h.lower().startswith("mailto:"):
            e = h.split(":", 1)[1].split("?")[0].strip()
            if "@" in e:
                found.add(e)
    decoded = html.replace(" [at] ", "@").replace(" (at) ", "@").replace("[at]", "@").replace("(at)", "@")\
                  .replace(" [dot] ", ".").replace(" (dot) ", ".").replace("[dot]", ".").replace("(dot)", ".")
    found |= set(EMAIL_RE.findall(decoded))

    clean = set()
    for e in found:
        e = e.lower().strip(".,;:()[]\"'")
        if "@" not in e: continue
        local, domain = e.split("@", 1)
        if domain in JUNK_DOMAINS: continue
        if any(d in domain for d in JUNK_DOMAIN_SUBSTR): continue
        if local in JUNK_LOCALS: continue
        if re.match(r"^[0-9a-f]{20,}$", local): continue
        if any(domain.endswith(ext) for ext in [".png",".jpg",".jpeg",".gif",".svg",".webp",".pdf",".js",".css"]):
            continue
        if len(local) > 40 or len(domain) > 60: continue
        clean.add(e)
    return clean


async def find_emails(website: str, founder_name: str) -> list[str]:
    import asyncio
    from config import settings
    if not website: return []
    if not website.startswith("http"): website = "https://" + website
    if any(b in website for b in SOCIALS + HOST_BUILDERS): return []

    parsed = urllib.parse.urlparse(website)
    base = f"{parsed.scheme}://{parsed.netloc}"

    pages = [website]
    for path in ["/contact", "/contact-us", "/about", "/about-us", "/team"]:
        pages.append(base + path)

    # Fetch pages in parallel (same host, but few requests — OK for almost all sites)
    sem = asyncio.Semaphore(settings.EMAIL_FIND_CONCURRENCY)
    async def fetch_one(p):
        async with sem:
            return await fetch(p, timeout=10)
    htmls = await asyncio.gather(*[fetch_one(p) for p in pages[:5]], return_exceptions=True)

    all_emails: set[str] = set()
    for html in htmls:
        if isinstance(html, str):
            all_emails |= extract_emails(html)

    # Generate personal pattern emails (L3) — ONLY when guessing is enabled.
    # Guessed patterns (jane@domain, j.smith@domain …) are the #1 bounce source:
    # they look plausible and can even pass verification on catch-all domains,
    # then bounce in the real campaign. With ALLOW_EMAIL_GUESSING off we return
    # only addresses actually found on the page (real mailto:/text emails) and
    # let the finder APIs (Skrapp) supply anything else.
    if settings.ALLOW_EMAIL_GUESSING and founder_name:
        parts = founder_name.lower().replace(".", "").split()
        if len(parts) >= 2:
            first = re.sub(r"[^a-z]", "", parts[0])
            last = re.sub(r"[^a-z]", "", parts[-1])
            if first and last and len(first) > 1 and len(last) > 1:
                domain = parsed.netloc.replace("www.", "")
                if domain and "." in domain and len(domain) < 50:
                    if not any(b in domain for b in HOST_BUILDERS):
                        f1 = first[0]     # first initial
                        l1 = last[0]      # last initial
                        # 10 patterns — cover the most common corporate email formats
                        all_emails.add(f"{first}@{domain}")           # jane@
                        all_emails.add(f"{first}.{last}@{domain}")    # jane.smith@
                        all_emails.add(f"{first}{last}@{domain}")     # janesmith@
                        all_emails.add(f"{f1}{last}@{domain}")        # jsmith@
                        all_emails.add(f"{f1}.{last}@{domain}")       # j.smith@
                        all_emails.add(f"{last}@{domain}")            # smith@
                        all_emails.add(f"{last}.{first}@{domain}")    # smith.jane@
                        all_emails.add(f"{first}_{last}@{domain}")    # jane_smith@
                        all_emails.add(f"{first}.{l1}@{domain}")      # jane.s@
                        all_emails.add(f"{last}{f1}@{domain}")        # smithj@
                        all_emails.add(f"{first}-{last}@{domain}")    # jane-smith@
                        # Generic fallbacks (lower rank — tried only if personal fail)
                        all_emails.add(f"hello@{domain}")
                        all_emails.add(f"info@{domain}")
                        all_emails.add(f"hi@{domain}")

    all_emails = {e for e in all_emails if e.split("@")[0] not in JUNK_LOCALS}
    return sorted(all_emails)


FREE_PROVIDERS = {"gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com",
                  "aol.com","protonmail.com","proton.me","pm.me","mail.com",
                  "live.com","msn.com","yandex.com","yandex.ru","zoho.com",
                  "fastmail.com","tutanota.com","gmx.com","mac.com","me.com"}


def rank_emails(emails: list[str], founder_name: str) -> list[str]:
    """Sort emails so the highest-personal-fit comes first.
    Order:
      1. Founder firstname/lastname @ free provider (e.g. sarah@gmail.com)
      2. Free provider with any local part (e.g. random@gmail.com)
      3. Founder firstname/lastname @ business domain
      4. Other personal-looking @ business domain
      5. Generic @ business domain (info@, hello@, contact@)
    """
    if not emails: return []
    parts = founder_name.lower().split() if founder_name else []
    first = re.sub(r"[^a-z]", "", parts[0]) if parts else ""
    last = re.sub(r"[^a-z]", "", parts[-1]) if len(parts) > 1 else ""

    def score(e: str) -> int:
        local, _, dom = e.partition("@")
        is_free = dom in FREE_PROVIDERS
        local_l = local.lower()
        name_match = (first and first in local_l) or (last and last in local_l)
        is_generic = local_l in {"info","hello","hi","contact","support","team","admin","office",
                                 "sales","help","press","media","hr","jobs","careers"}
        # Lower is better (sorted ascending)
        if is_free and name_match: return 0
        if is_free: return 1
        if name_match: return 2
        if is_generic: return 9
        return 5

    return sorted(set(emails), key=score)
