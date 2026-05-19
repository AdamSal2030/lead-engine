from __future__ import annotations
"""Niche classifier for verified leads.

Priority order:
  1. Claude-provided niche hint (already a good label — just normalise it)
  2. Role keyword shortcut
  3. Keyword scan across role + company + website text
  4. Fallback → "Founder / Startup"

Niches drive CSV/Excel sheet segmentation in delivery.py.
"""

# Ordered list so the first match wins on ambiguous leads
NICHE_RULES: list[tuple[str, list[str]]] = [
    ("Marketing Agency", [
        "marketing agency", "digital marketing", "social media agency", "seo agency",
        "ppc agency", "advertising agency", "content agency", "growth agency",
        "performance marketing", "creative agency", "media buying", "ad agency",
        "inbound marketing", "outbound agency", "brand strategy", "content marketing",
        "email marketing", "influencer marketing", "affiliate marketing",
    ]),
    ("PR & Communications", [
        "public relations", "pr firm", "pr agency", "communications agency",
        "media relations", "brand communications", "press release", "reputation management",
        "crisis communications", "media strategy",
    ]),
    ("Coaching", [
        "life coach", "business coach", "executive coach", "leadership coach",
        "career coach", "health coach", "wellness coach", "mindset coach",
        "performance coach", "relationship coach", "dating coach",
        "coach", "coaching practice", "coaching business", "certified coach",
        "nlp practitioner", "life coaching", "business coaching",
    ]),
    ("Author / Speaker", [
        "author", "book author", "published author", "bestselling author",
        "keynote speaker", "public speaker", "motivational speaker", "ted speaker",
        "speaker", "thought leader", "speaker bureau", "podcast host",
        "podcast", "podcaster", "show host", "radio host", "writer",
        "ghostwriter", "content creator", "blogger",
    ]),
    ("Consulting", [
        "consultant", "consulting firm", "advisory", "advisor",
        "management consulting", "hr consulting", "strategy consulting",
        "operations consulting", "it consulting", "fractional",
        "fractional cmo", "fractional coo", "fractional cto", "interim",
        "business advisor", "startup advisor",
    ]),
    ("Real Estate", [
        "real estate", "realtor", "property", "realty", "mortgage broker",
        "real estate investor", "property management", "property developer",
        "airbnb", "short-term rental", "real estate agent", "home buying",
        "commercial real estate", "real estate investing",
    ]),
    ("E-commerce", [
        "ecommerce", "e-commerce", "online store", "shopify store", "amazon seller",
        "consumer goods", "product brand", "dropshipping", "print on demand",
        "physical product", "dtc", "direct to consumer", "fba",
    ]),
    ("SaaS / Tech", [
        "saas", "software as a service", "b2b software", "developer tools",
        "cybersecurity", "fintech", "edtech", "healthtech", "proptech",
        "api platform", "no-code", "low-code", "tech startup", "app developer",
        "software startup", "ai startup", "machine learning", "data analytics",
    ]),
    ("Fitness & Wellness", [
        "fitness", "gym", "yoga", "pilates", "personal trainer", "nutrition coach",
        "wellness", "mindfulness", "meditation", "naturopath", "holistic health",
        "functional medicine", "CrossFit", "strength coach", "sports coach",
        "nutritionist", "dietitian", "weight loss", "health and wellness",
    ]),
    ("Healthcare", [
        "doctor", "physician", "therapist", "psychologist", "mental health",
        "dentist", "chiropractor", "nurse practitioner", "medical practice",
        "telehealth", "healthcare provider", "clinical", "licensed therapist",
        "lcsw", "counselor", "occupational therapist", "physical therapist",
        "psychiatrist", "skin care", "aesthetician", "esthetician",
    ]),
    ("Legal & Finance", [
        "lawyer", "attorney", "law firm", "legal services", "accountant", "cpa",
        "financial advisor", "wealth management", "insurance", "tax services",
        "bookkeeping", "fractional cfo", "financial planner", "investment advisor",
        "estate planning", "business attorney", "tax advisor",
    ]),
    ("Creative Services", [
        "photographer", "videographer", "graphic designer", "web designer",
        "creative director", "art director", "illustrator", "animator",
        "filmmaker", "branding studio", "design studio", "motion graphics",
        "interior designer", "interior design", "architect", "ux designer",
        "ui designer", "logo designer", "web design agency",
    ]),
    ("Education & Training", [
        "education", "tutoring", "online course", "e-learning", "corporate training",
        "curriculum", "edtech", "bootcamp", "workshop", "online academy",
        "course creator", "learning platform", "teacher", "instructor",
        "online educator", "digital course", "membership site",
    ]),
    ("Food & Hospitality", [
        "restaurant", "food brand", "chef", "catering", "bakery", "cafe",
        "beverage company", "hospitality", "food startup", "meal prep",
        "food blogger", "personal chef", "food photographer", "bartender",
        "mixologist", "winery", "brewery",
    ]),
    ("Recruiting & HR", [
        "recruiting", "recruitment agency", "staffing", "headhunter",
        "human resources", "hr tech", "talent acquisition", "executive search",
        "hr consultant", "people operations", "employer brand",
    ]),
    ("Events & Entertainment", [
        "event planning", "wedding planner", "event management", "entertainment",
        "music", "dj", "production company", "venue", "event coordinator",
        "corporate events", "social media influencer", "content creator",
        "brand ambassador", "model", "actor", "actress",
    ]),
]

# Fast-path: exact role → niche (checked before keyword scan)
ROLE_TO_NICHE: dict[str, str] = {
    "Coach": "Coaching",
    "Life Coach": "Coaching",
    "Business Coach": "Coaching",
    "Executive Coach": "Coaching",
    "Consultant": "Consulting",
    "Realtor": "Real Estate",
    "Attorney": "Legal & Finance",
    "Lawyer": "Legal & Finance",
    "CPA": "Legal & Finance",
    "Photographer": "Creative Services",
    "Videographer": "Creative Services",
    "Designer": "Creative Services",
    "Graphic Designer": "Creative Services",
    "Interior Designer": "Creative Services",
    "Therapist": "Healthcare",
    "Psychologist": "Healthcare",
    "Counselor": "Healthcare",
    "Doctor": "Healthcare",
    "Physician": "Healthcare",
    "Chef": "Food & Hospitality",
    "Wedding Planner": "Events & Entertainment",
    "Event Planner": "Events & Entertainment",
    "Recruiter": "Recruiting & HR",
    "Author": "Author / Speaker",
    "Speaker": "Author / Speaker",
    "Podcaster": "Author / Speaker",
    "Writer": "Author / Speaker",
    "Blogger": "Author / Speaker",
}


def classify(
    role: str | None,
    company: str | None,
    niche_hint: str | None,
    website: str | None = None,
) -> str:
    """Return the best-matching niche label for this lead."""
    # 1. Claude gave us a niche hint — check if it maps to a known category
    if niche_hint:
        h = niche_hint.lower()
        for niche, keywords in NICHE_RULES:
            if any(kw in h for kw in keywords):
                return niche
        # Hint is a short custom label — use it directly (title-cased)
        if 3 < len(niche_hint) < 50:
            return niche_hint.strip().title()

    # 2. Role shortcut
    if role:
        mapped = ROLE_TO_NICHE.get(role.strip())
        if mapped:
            return mapped

    # 3. Keyword scan across all available text
    text = f"{role or ''} {company or ''} {website or ''}".lower()
    for niche, keywords in NICHE_RULES:
        if any(kw in text for kw in keywords):
            return niche

    return "Founder / Startup"
