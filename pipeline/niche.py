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
        "inbound marketing", "outbound agency",
    ]),
    ("PR & Communications", [
        "public relations", "pr firm", "pr agency", "communications agency",
        "media relations", "brand communications", "press release",
    ]),
    ("Coaching", [
        "life coach", "business coach", "executive coach", "leadership coach",
        "career coach", "health coach", "wellness coach", "mindset coach",
        "performance coach", "relationship coach", "dating coach",
        "coach", "coaching practice", "coaching business",
    ]),
    ("Consulting", [
        "consultant", "consulting firm", "advisory", "advisor",
        "management consulting", "hr consulting", "strategy consulting",
        "operations consulting", "it consulting", "fractional",
    ]),
    ("Real Estate", [
        "real estate", "realtor", "property", "realty", "mortgage broker",
        "real estate investor", "property management", "property developer",
        "airbnb", "short-term rental",
    ]),
    ("E-commerce", [
        "ecommerce", "e-commerce", "online store", "shopify store", "amazon seller",
        "consumer goods", "product brand", "dropshipping", "print on demand",
        "physical product",
    ]),
    ("SaaS / Tech", [
        "saas", "software as a service", "b2b software", "developer tools",
        "cybersecurity", "fintech", "edtech", "healthtech", "proptech",
        "api platform", "no-code", "low-code", "tech startup",
    ]),
    ("Fitness & Wellness", [
        "fitness", "gym", "yoga", "pilates", "personal trainer", "nutrition coach",
        "wellness", "mindfulness", "meditation", "naturopath", "holistic health",
        "functional medicine",
    ]),
    ("Healthcare", [
        "doctor", "physician", "therapist", "psychologist", "mental health",
        "dentist", "chiropractor", "nurse practitioner", "medical practice",
        "telehealth", "healthcare provider", "clinical",
    ]),
    ("Legal & Finance", [
        "lawyer", "attorney", "law firm", "legal services", "accountant", "cpa",
        "financial advisor", "wealth management", "insurance", "tax services",
        "bookkeeping", "fractional cfo",
    ]),
    ("Creative Services", [
        "photographer", "videographer", "graphic designer", "web designer",
        "creative director", "art director", "illustrator", "animator",
        "filmmaker", "branding studio", "design studio", "motion graphics",
    ]),
    ("Education & Training", [
        "education", "tutoring", "online course", "e-learning", "corporate training",
        "curriculum", "edtech", "bootcamp", "workshop", "online academy",
    ]),
    ("Food & Hospitality", [
        "restaurant", "food brand", "chef", "catering", "bakery", "cafe",
        "beverage company", "hospitality", "food startup", "meal prep",
    ]),
    ("Recruiting & HR", [
        "recruiting", "recruitment agency", "staffing", "headhunter",
        "human resources", "hr tech", "talent acquisition", "executive search",
    ]),
    ("Events & Entertainment", [
        "event planning", "wedding planner", "event management", "entertainment",
        "music", "dj", "production company", "venue",
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
    "Therapist": "Healthcare",
    "Psychologist": "Healthcare",
    "Doctor": "Healthcare",
    "Physician": "Healthcare",
    "Chef": "Food & Hospitality",
    "Wedding Planner": "Events & Entertainment",
    "Recruiter": "Recruiting & HR",
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
