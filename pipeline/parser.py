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

JUNK_DOMAINS = {"sentry.io", "wixpress.com", "example.com", "domain.com", "test.com"}
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


CLOUDFLARE_BLOCKED_HOSTS = ("canvasrebel.com", "boldjourney.com")


async def fetch(url: str, timeout: int = 15) -> str | None:
    """Try multiple UAs. For known Cloudflare-blocked hosts, go directly to Wayback (skip the doomed direct attempts)."""
    # Fast path: known-blocked → Wayback only
    if any(h in url for h in CLOUDFLARE_BLOCKED_HOSTS):
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
    for ua in UA_LIST:
        try:
            h = {"User-Agent": ua, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}
            async with httpx.AsyncClient(headers=h, timeout=timeout, follow_redirects=True) as cli:
                r = await cli.get(url)
                if r.status_code == 200 and "text/html" in r.headers.get("content-type", "").lower():
                    return r.text
        except Exception:
            continue
    return None


def clean_name(title_text: str) -> str | None:
    """Strict name extraction. Rejects article titles, business names, lists, etc."""
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

    # Strip leading interview-style prefixes
    for prefix in ["Meet ", "Inspiring Conversations with ", "Conversations with ",
                   "Life & Work with ", "Hidden Gems: Meet ", "Daily Inspiration: Meet ",
                   "Exclusive Interview with ", "Interview with "]:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break

    # Cut at separator | – — :
    name = re.split(r"\s*[|–—:]\s*", name)[0].strip()
    # Cut at " of/from/on/at/with/in "
    name = re.split(r"\s+(?:of|from|on|at|with|in)\s+", name, flags=re.IGNORECASE)[0].strip()
    # Strip possessive 's, credentials, suffixes
    name = re.sub(r"[‘’]s\b.*", "", name)
    name = re.sub(r",\s*(MD|PhD|DDS|JD|MBA|CPA|RN|LCSW|MFT|ATR-BC|[A-Z]{2,5}-?[A-Z]{2,5})\b.*",
                  "", name, flags=re.IGNORECASE)
    # Strip honorifics
    name = re.sub(r"^(?:Dr\.?|Mr\.?|Mrs\.?|Ms\.?|Prof\.?|Rev\.?|Chef|Coach|DJ)\s+",
                  "", name, flags=re.IGNORECASE)
    # Strip trailing role / title words
    name = re.sub(r"\s+(Photographer|Designer|Founder|CEO|Owner|Author|Artist|Coach|"
                  r"Consultant|Entrepreneur|Director|President|Esthetician|Therapist|"
                  r"novelist|Filmmaker|Chef|Doctor|Lawyer|Realtor|Stylist|Influencer)\s*$",
                  "", name, flags=re.IGNORECASE)

    # Normalize ALL-CAPS or all-lowercase names to Title Case
    if name == name.upper() or name == name.lower():
        name = " ".join(w.capitalize() for w in name.split())

    words = name.split()
    if not (2 <= len(words) <= 4) or any(c.isdigit() for c in name):
        return None
    # Each word: starts with capital, letters (incl unicode/accented) + optional hyphen/apostrophe
    if not all(re.match(r"^[A-ZÀ-Ý][\w'\-À-ÿ]+$|^[A-Z]\.?$", w, re.UNICODE) for w in words):
        return None
    # No name-word can match the title-words blocklist
    if any(w.lower().rstrip(".") in NON_PERSON for w in words):
        return None
    # Reject if all-caps after our normalization attempt failed (unusual)
    # Reject special punctuation in name
    if any(c in name for c in '"”“'):
        return None
    # Each word at least 2 chars OR a single uppercase letter (initial)
    for w in words:
        if len(w) == 1 and w.isupper(): continue  # initial OK
        if len(w) < 2: return None
    return name


def find_website(body, body_text: str, page_url: str) -> str | None:
    for label in ["Personal Website:", "Website:", "Website : ",
                  "Business Website:", "Company Website:", "Web:"]:
        m = re.search(rf"{label}\s*([^\s\n,]+)", body_text)
        if m:
            w = m.group(1).strip().rstrip(".,;")
            if "." in w and " " not in w and len(w) < 80:
                return w if w.startswith("http") else "https://" + w

    for a in body.find_all("a", href=True):
        h = a["href"].strip()
        if h.startswith("http") and not any(s in h.lower() for s in SOCIALS):
            if page_url.split("/")[2] in h: continue
            return h.split("?")[0].split("#")[0]
    return None


def find_role(text: str) -> str | None:
    for r_word in ["Founder","Co-Founder","CEO","Owner","Author","Creator","Director",
                   "Principal","President","Coach","Consultant","Entrepreneur"]:
        if re.search(rf"\b{r_word}\b", text):
            return r_word
    return None


def find_company(text: str) -> str | None:
    for pat in [r"\b(?:founder|co-founder|cofounder|owner|ceo|president|principal)\s+(?:and\s+\w+\s+)?of\s+([A-Z][A-Za-z0-9&\.\s\-']{1,50})",
                r"\b(?:I founded|I started|I launched|I created|I am the founder of)\s+([A-Z][A-Za-z0-9&\.\s\-']{1,50})"]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            c = m.group(1).strip().rstrip(".,;")
            c = re.split(r"\b(?:is|in|and|where|which|that|with|to|when|so|because)\b", c)[0].strip()
            if 2 <= len(c) <= 60:
                return c
    return None


async def parse_article(url: str) -> dict | None:
    """Returns dict with name, website, role, company, or None."""
    html = await fetch(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    title = soup.find("h1")
    title_text = title.get_text(strip=True) if title else ""

    name = clean_name(title_text)
    if not name:
        return None

    body = soup.find("article") or soup.find(class_=re.compile("entry-content|post-content"))
    if not body:
        return None

    body_text = body.get_text(" ", strip=True)
    website = find_website(body, body_text, url)
    if not website:
        return None

    return {
        "source_url": url,
        "source": source_label(url),
        "name": name,
        "website": website,
        "role": find_role(body_text),
        "company": find_company(body_text),
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

    # Generate pattern emails if nothing found
    if not all_emails and founder_name:
        parts = founder_name.lower().replace(".", "").split()
        if len(parts) >= 2:
            first = re.sub(r"[^a-z]", "", parts[0])
            last = re.sub(r"[^a-z]", "", parts[-1])
            if first and last and len(first) > 1 and len(last) > 1:
                domain = parsed.netloc.replace("www.", "")
                if domain and "." in domain and len(domain) < 50:
                    if not any(b in domain for b in HOST_BUILDERS):
                        all_emails.add(f"{first}@{domain}")
                        all_emails.add(f"{first}.{last}@{domain}")
                        all_emails.add(f"{first}{last}@{domain}")
                        all_emails.add(f"hello@{domain}")
                        all_emails.add(f"info@{domain}")

    all_emails = {e for e in all_emails if e.split("@")[0] not in JUNK_LOCALS}
    return sorted(all_emails)
