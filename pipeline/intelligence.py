from __future__ import annotations
"""Self-improvement intelligence engine.

Every INTELLIGENCE_RUN_HOURS (default 24h) the engine:
  1. Pulls per-niche and per-source KPIs from the DB
     (leads delivered, bounces, replies, rates)
  2. Sends those metrics to Claude Sonnet for strategic analysis
  3. Claude returns: which niches to grow, which to cut, which new ones to test,
     and adjusted per-source quality weights
  4. Source weights are written to the SourceWeight table
  5. The orchestrator reads these weights on every batch and processes
     high-quality sources first — so future batches skew toward what works

Result over time:
  - High-reply niches get more of the URL queue
  - Bouncy / silent niches get pushed to the back
  - New niches are tested when Claude spots a promising adjacent category
  - A plain-English narrative report is stored for the dashboard
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import select, func, update as sql_update, insert as sql_insert
from db import SessionLocal, VerifiedLead, SourceWeight, IntelligenceReport

log = logging.getLogger("intelligence")

_cycle_count = 0
_last_run: datetime | None = None
_lock = asyncio.Lock()


# ── 1. Metrics ──────────────────────────────────────────────────────────────

async def compute_metrics() -> dict:
    """Aggregate per-niche and per-source KPIs from the DB."""
    async with SessionLocal() as s:
        leads = (await s.execute(select(VerifiedLead))).scalars().all()

    niche_stats: dict[str, dict] = defaultdict(lambda: {
        "total": 0, "bounced": 0, "replied": 0,
    })
    source_stats: dict[str, dict] = defaultdict(lambda: {
        "total": 0, "bounced": 0, "replied": 0,
    })

    for l in leads:
        niche = (l.niche or "Founder / Startup").strip() or "Founder / Startup"
        src = l.source or "Unknown"

        niche_stats[niche]["total"] += 1
        source_stats[src]["total"] += 1

        if l.bounced:
            niche_stats[niche]["bounced"] += 1
            source_stats[src]["bounced"] += 1
        if l.responded:
            niche_stats[niche]["replied"] += 1
            source_stats[src]["replied"] += 1

    def _rates(d: dict) -> dict:
        t = d["total"] or 1
        b = d["bounced"]
        r = d["replied"]
        return {
            **d,
            "bounce_rate": round(100 * b / t, 1),
            "reply_rate": round(100 * r / t, 1),
            "net_score": round(100 * r / t - 100 * b / t, 1),
        }

    return {
        "total_leads": len(leads),
        "total_bounced": sum(1 for l in leads if l.bounced),
        "total_replied": sum(1 for l in leads if l.responded),
        "niche": {k: _rates(v) for k, v in niche_stats.items()},
        "source": {k: _rates(v) for k, v in source_stats.items()},
    }


def _format_metrics_for_claude(metrics: dict) -> str:
    """Format metrics as a readable table for Claude's context."""
    lines = [
        f"Total leads in DB: {metrics['total_leads']}",
        f"Total bounced:     {metrics['total_bounced']} "
        f"({round(100*metrics['total_bounced']/(metrics['total_leads'] or 1),1)}%)",
        f"Total replied:     {metrics['total_replied']} "
        f"({round(100*metrics['total_replied']/(metrics['total_leads'] or 1),1)}%)",
        "",
        "NICHE PERFORMANCE (sorted by net score = reply% - bounce%):",
        f"{'Niche':<28} {'Leads':>6} {'Bounce%':>8} {'Reply%':>7} {'Net%':>6}",
        "-" * 62,
    ]

    for niche, d in sorted(
        metrics["niche"].items(), key=lambda x: -x[1]["net_score"]
    ):
        lines.append(
            f"{niche:<28} {d['total']:>6} {d['bounce_rate']:>7}% "
            f"{d['reply_rate']:>6}% {d['net_score']:>+6}%"
        )

    lines += [
        "",
        "SOURCE PERFORMANCE (sorted by total leads):",
        f"{'Source':<28} {'Leads':>6} {'Bounce%':>8} {'Reply%':>7} {'Net%':>6}",
        "-" * 62,
    ]

    for src, d in sorted(
        metrics["source"].items(), key=lambda x: -x[1]["total"]
    ):
        if d["total"] < 3:
            continue  # skip tiny sources — not enough data
        lines.append(
            f"{src:<28} {d['total']:>6} {d['bounce_rate']:>7}% "
            f"{d['reply_rate']:>6}% {d['net_score']:>+6}%"
        )

    return "\n".join(lines)


# ── 2. Claude analysis ───────────────────────────────────────────────────────

INTELLIGENCE_PROMPT = """\
You are the strategy engine for an automated B2B lead generation system.
The system finds business owners / professionals through interview articles,
verifies their emails, and sends cold outreach via Instantly.

Below are the current performance metrics. Your job is to recommend how to improve results.

{metrics}

Based on this data, provide strategic recommendations. Consider:
- Net score (reply% - bounce%) is the key metric. High net score = good niche.
- Niches with zero bounces/replies likely have insufficient data (< 20 leads) — flag these as "needs data".
- Look for adjacent niches that might perform similarly to our best-performing ones.
- Sources with high net scores should be prioritised; poor ones deprioritised.

Respond ONLY with valid JSON in this exact format:
{{
  "expand": ["niche1", "niche2"],
  "reduce": ["niche3"],
  "test_new": ["newNicheA", "newNicheB"],
  "source_weights": {{
    "SourceName": 1.5,
    "OtherSource": 0.7
  }},
  "narrative": "3–5 sentence plain-English summary of what the data shows and why you're making these recommendations.",
  "top_insight": "Single most actionable finding in one sentence."
}}

Rules:
- expand: niches with the best net scores (positive and sufficient data). Max 4.
- reduce: niches with net score < -5% OR chronic high bounce with no replies. Max 3.
- test_new: niches NOT currently in the data that are likely adjacent to top performers. Max 3. Use specific labels like "Wedding Planners", "HR Consultants", "Fractional CFOs".
- source_weights: only include sources you want to change from the default (1.0). Range 0.3–2.0.
- narrative: interpret trends, not just restate numbers. Mention if data is too thin to be conclusive.
- top_insight: the single change that would have the biggest impact.
"""


async def analyze_with_claude(metrics: dict) -> dict | None:
    """Send metrics to the configured LLM and get strategic recommendations.

    Uses settings.LLM_PROVIDER (Claude Sonnet by default; ollama/openai when set).
    """
    from config import settings
    from pipeline.llm import active_provider, chat_json
    # Need either an Anthropic key (claude) or an open-model config (ollama/openai)
    if active_provider() == "claude" and not settings.ANTHROPIC_API_KEY:
        log.info("No ANTHROPIC_API_KEY — skipping intelligence analysis")
        return None

    formatted = _format_metrics_for_claude(metrics)
    prompt = INTELLIGENCE_PROMPT.format(metrics=formatted)

    try:
        # Sonnet-class reasoning — runs once per day, so a larger token budget.
        raw = await chat_json(
            "", prompt,
            claude_model="claude-sonnet-4-6",
            max_tokens=800,
        )
        if not raw:
            return None
        raw = raw.strip()
        if "```" in raw:
            parts = raw.split("```")
            raw = parts[1][4:] if parts[1].startswith("json") else parts[1]
        result = json.loads(raw.strip())
        log.info(f"Intelligence analysis complete. Top insight: {result.get('top_insight', '')}")
        return result
    except Exception as e:
        log.exception(f"Claude intelligence analysis failed: {e}")
        return None


# ── 3. Weight update ─────────────────────────────────────────────────────────

async def update_source_weights(weights: dict[str, float]) -> None:
    """Write updated source weights to DB."""
    if not weights:
        return
    async with SessionLocal() as s:
        for source, weight in weights.items():
            weight = max(0.3, min(2.0, float(weight)))  # clamp
            existing = (await s.execute(
                select(SourceWeight).where(SourceWeight.source == source)
            )).scalar_one_or_none()
            if existing:
                await s.execute(
                    sql_update(SourceWeight)
                    .where(SourceWeight.source == source)
                    .values(weight=weight, updated_at=datetime.utcnow())
                )
            else:
                s.add(SourceWeight(source=source, weight=weight, updated_at=datetime.utcnow()))
        await s.commit()
    log.info(f"Updated weights for {len(weights)} sources")


async def get_source_weights() -> dict[str, float]:
    """Return current source weights for the orchestrator. Defaults to 1.0."""
    async with SessionLocal() as s:
        rows = (await s.execute(select(SourceWeight))).scalars().all()
    return {r.source: r.weight for r in rows}


# ── 4. Store report ──────────────────────────────────────────────────────────

async def _store_report(
    cycle: int,
    metrics: dict,
    analysis: dict | None,
) -> None:
    expand = reduce = test = source_w = narrative = insight = ""
    if analysis:
        expand = json.dumps(analysis.get("expand", []))
        reduce = json.dumps(analysis.get("reduce", []))
        test = json.dumps(analysis.get("test_new", []))
        source_w = json.dumps(analysis.get("source_weights", {}))
        narrative = analysis.get("narrative", "")
        insight = analysis.get("top_insight", "")

    async with SessionLocal() as s:
        s.add(IntelligenceReport(
            cycle=cycle,
            niche_metrics=json.dumps(metrics.get("niche", {})),
            source_metrics=json.dumps(metrics.get("source", {})),
            expand_niches=expand,
            reduce_niches=reduce,
            test_niches=test,
            source_weights=source_w,
            narrative=narrative,
            top_insight=insight,
        ))
        await s.commit()


# ── 5. Full cycle ─────────────────────────────────────────────────────────────

async def run_cycle() -> dict:
    """Run one full intelligence cycle. Safe to call from API or scheduler."""
    global _cycle_count, _last_run

    async with _lock:
        _cycle_count += 1
        _last_run = datetime.utcnow()
        cycle = _cycle_count

        log.info(f"=== Intelligence cycle #{cycle} starting ===")

        # 1. Metrics
        metrics = await compute_metrics()
        log.info(
            f"  Metrics: {metrics['total_leads']} leads, "
            f"{metrics['total_bounced']} bounced, "
            f"{metrics['total_replied']} replied"
        )

        # 2. Claude analysis (requires ANTHROPIC_API_KEY + enough data)
        analysis = None
        if metrics["total_leads"] >= 10:
            analysis = await analyze_with_claude(metrics)
        else:
            log.info("  Skipping Claude analysis — fewer than 10 leads in DB (not enough signal yet)")

        # 3. Apply weight updates
        if analysis and analysis.get("source_weights"):
            await update_source_weights(analysis["source_weights"])

        # 4. Store report
        await _store_report(cycle, metrics, analysis)

        log.info(f"=== Intelligence cycle #{cycle} complete ===")

        return {
            "cycle": cycle,
            "total_leads": metrics["total_leads"],
            "top_insight": analysis.get("top_insight") if analysis else "Insufficient data",
            "expand": analysis.get("expand", []) if analysis else [],
            "reduce": analysis.get("reduce", []) if analysis else [],
            "test_new": analysis.get("test_new", []) if analysis else [],
            "source_weights_updated": len(analysis.get("source_weights", {})) if analysis else 0,
        }


async def get_last_report() -> dict | None:
    """Return the most recent intelligence report for the dashboard."""
    async with SessionLocal() as s:
        row = (await s.execute(
            select(IntelligenceReport).order_by(IntelligenceReport.id.desc()).limit(1)
        )).scalar_one_or_none()

    if not row:
        return None

    def _j(v):
        try:
            return json.loads(v) if v else []
        except Exception:
            return v

    return {
        "cycle": row.cycle,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "top_insight": row.top_insight,
        "narrative": row.narrative,
        "expand_niches": _j(row.expand_niches),
        "reduce_niches": _j(row.reduce_niches),
        "test_niches": _j(row.test_niches),
        "source_weights": _j(row.source_weights),
        "niche_metrics": _j(row.niche_metrics),
        "source_metrics": _j(row.source_metrics),
    }


async def intelligence_loop(run_every_hours: int = 24):
    """Background task: run intelligence cycle on a schedule."""
    from config import settings
    log.info(f"Intelligence loop started — will run every {run_every_hours}h")
    # First run after 2 hours (give the first batch time to complete)
    await asyncio.sleep(2 * 3600)
    while True:
        try:
            await run_cycle()
        except Exception:
            log.exception("Intelligence cycle error")
        await asyncio.sleep(run_every_hours * 3600)
