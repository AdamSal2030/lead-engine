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
    "small", "business", "gift", "guide", "community", "heart", "introverted",
    "spotlight", "conversations", "conversation", "interviews", "interview",
    "inspirational", "inspiring", "local", "stories", "story", "portrait",
    "portraits", "creative", "creatives", "entrepreneur", "entrepreneurs",
    "founder", "founders", "ceo", "owner", "leader", "leaders", "success",
    "feature", "featured", "meet", "life", "work", "behind", "check", "read",
    "exclusive", "amazing", "best", "top", "perspectives", "thoughts", "insights",
    "podcasts", "podcast", "weekly", "daily", "new", "introducing", "welcome",
    "where", "the", "building", "finding", "power",
    "open", "late", "early", "night", "day", "morning", "afternoon",
    "guide", "list", "review", "place", "places", "restaurant", "restaurants",
    "shop", "shops", "fitness", "tour", "tours", "events",
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


async def fetch(url: str, timeout: int = 15) -> str | None:
    try:
        async with httpx.AsyncClient(headers=HEADERS, timeout=timeout, follow_redirects=True) as cli:
            r = await cli.get(url)
            if r.status_code == 200 and "text/html" in r.headers.get("content-type", "").lower():
                return r.text
    except Exception:
        pass
    return None


def clean_name(title_text: str) -> str | None:
    name = title_text
    # Reject guide / list articles outright
    rejects = [r"\bguide to\b", r"\blist of\b", r"\btop \d+", r"\bbest of\b",
               r"\bfaqs?\b", r"\bspotlight\b", r"\bportraits? of\b", r"\binspiring stories\b"]
    for pat in rejects:
        if re.search(pat, name, re.IGNORECASE):
            return None
    for prefix in ["Meet ", "Inspiring Conversations with ", "Conversations with ",
                   "Life & Work with ", "Hidden Gems: Meet ", "Daily Inspiration: Meet "]:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    name = re.split(r"\s*[|–—:]\s*", name)[0].strip()
    name = re.split(r"\s+(?:of|from|on|at|with)\s+", name, flags=re.IGNORECASE)[0].strip()
    name = re.sub(r"[‘’]s\b.*", "", name)
    # Strip honorifics
    name = re.sub(r"^(?:Dr\.?|Mr\.?|Mrs\.?|Ms\.?|Prof\.?|Rev\.?|Chef|Coach)\s+", "", name, flags=re.IGNORECASE)
    # Strip trailing role words
    name = re.sub(r"\s+(Photographer|Designer|Founder|CEO|Owner|Author|Artist|Coach|"
                  r"Consultant|Entrepreneur|Director|President|Esthetician|Therapist|"
                  r"novelist|Filmmaker|Chef|Doctor|Lawyer|Realtor|Stylist|Influencer)$",
                  "", name, flags=re.IGNORECASE)
    words = name.split()
    if not (2 <= len(words) <= 4) or any(c.isdigit() for c in name):
        return None
    if not all(re.match(r"^[A-Za-z][A-Za-z'\-\.]*$", w) for w in words):
        return None
    if words[0].lower() in NON_PERSON:
        return None
    # Reject if any word is in NON_PERSON (e.g. "Open Late")
    if any(w.lower() in NON_PERSON for w in words):
        return None
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
    if not website: return []
    if not website.startswith("http"): website = "https://" + website
    if any(b in website for b in SOCIALS + HOST_BUILDERS): return []

    parsed = urllib.parse.urlparse(website)
    base = f"{parsed.scheme}://{parsed.netloc}"

    pages = [website]
    for path in ["/contact", "/contact-us", "/contact/", "/about", "/about-us",
                 "/about/", "/team", "/get-in-touch", "/connect"]:
        pages.append(base + path)

    all_emails: set[str] = set()
    for page in pages[:5]:
        html = await fetch(page, timeout=10)
        if html:
            all_emails |= extract_emails(html)
            if len(all_emails) >= 5: break

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
