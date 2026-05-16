"""HTML dashboard rendered server-side. No JS framework — vanilla auto-refresh."""
from __future__ import annotations
import os
from datetime import datetime, timezone
from sqlalchemy import select, desc, func
from db import SessionLocal, VerifiedLead, Batch
from config import settings


def human_time(dt: datetime | None) -> str:
    if not dt: return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    s = int(delta.total_seconds())
    if s < 60: return f"{s}s ago"
    if s < 3600: return f"{s // 60}m ago"
    if s < 86400: return f"{s // 3600}h {(s % 3600) // 60}m ago"
    return f"{s // 86400}d ago"


def status_badge(status: str) -> str:
    colors = {
        "running": "#f59e0b",      # amber
        "completed": "#10b981",    # green
        "failed": "#ef4444",       # red
        "interrupted": "#9ca3af",  # gray
    }
    color = colors.get(status, "#6b7280")
    return f'<span class="badge" style="background:{color}">{status}</span>'


# Wikipedia Commons public photos
SYDNEY_PICS = [
    "https://upload.wikimedia.org/wikipedia/commons/thumb/0/04/Sydney_Sweeney_by_Gage_Skidmore.jpg/512px-Sydney_Sweeney_by_Gage_Skidmore.jpg",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/2/2c/Sydney_Sweeney_at_2018_Miss_Bala_Premiere.jpg/512px-Sydney_Sweeney_at_2018_Miss_Bala_Premiere.jpg",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/d/db/Sydney_Sweeney_2024.jpg/512px-Sydney_Sweeney_2024.jpg",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/b/b2/Sydney_Sweeney_in_2024.jpg/512px-Sydney_Sweeney_in_2024.jpg",
]


async def render_dashboard(loop_state: dict, perpetual_paused: bool, current_batch: dict | None, is_running_now: bool) -> str:
    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one()
        recent_batches = (await s.execute(
            select(Batch).order_by(desc(Batch.id)).limit(15)
        )).scalars().all()
        # Latest 8 leads
        recent_leads = (await s.execute(
            select(VerifiedLead).order_by(desc(VerifiedLead.id)).limit(8)
        )).scalars().all()

    # Current batch progress
    cb_html = ""
    if current_batch:
        target = current_batch.get("target", 0) or 1
        verified_now = current_batch.get("verified", 0) or 0
        pct = min(100, int(verified_now / target * 100))
        scraped = current_batch.get("scraped", 0)
        raw = current_batch.get("raw_with_emails", 0)
        cb_html = f"""
        <div class="current-batch">
          <div class="cb-header">
            <strong>Batch #{current_batch.get('batch_id')}</strong> running now
            <span class="muted">target: {target} leads</span>
          </div>
          <div class="progress-bar"><div class="progress-fill" style="width:{pct}%"></div></div>
          <div class="cb-stats">
            <span><b>{verified_now}</b> verified</span>
            <span>·</span>
            <span><b>{raw}</b> raw with emails</span>
            <span>·</span>
            <span><b>{scraped}</b> URLs scraped</span>
          </div>
        </div>"""

    # Batches table
    rows = []
    base = settings.PUBLIC_BASE_URL.rstrip("/") if settings.PUBLIC_BASE_URL else ""
    for b in recent_batches:
        csv_link = ""
        if b.csv_path and b.delivered_count and b.delivered_count > 0:
            fname = os.path.basename(b.csv_path)
            csv_link = f'<a href="{base}/download/{fname}" class="dl-link">⬇ CSV</a>'
        duration = ""
        if b.started_at and b.finished_at:
            d = (b.finished_at - b.started_at).total_seconds()
            duration = f"{int(d // 60)}m {int(d % 60)}s"
        rows.append(f"""
          <tr>
            <td>#{b.id}</td>
            <td>{status_badge(b.status)}</td>
            <td><b>{b.delivered_count or 0}</b> <span class="muted">/ {b.target}</span></td>
            <td>{duration or '—'}</td>
            <td>{human_time(b.started_at)}</td>
            <td>{b.trigger}</td>
            <td>{csv_link}</td>
          </tr>
        """)
    batches_html = "\n".join(rows) if rows else '<tr><td colspan="7" class="muted">No batches yet</td></tr>'

    # Recent leads preview
    leads_rows = []
    for l in recent_leads:
        leads_rows.append(f"""
          <tr>
            <td>{(l.first_name or '') + ' ' + (l.last_name or '')}</td>
            <td><a href="mailto:{l.email}">{l.email}</a></td>
            <td class="muted">{(l.company or '')[:35]}</td>
            <td><a href="{l.website or '#'}" target="_blank" class="muted">{(l.website or '')[:35]}</a></td>
            <td class="muted">{l.source or ''}</td>
            <td class="muted">{human_time(l.created_at)}</td>
          </tr>
        """)
    leads_table = "\n".join(leads_rows) if leads_rows else '<tr><td colspan="6" class="muted">No leads yet</td></tr>'

    # Loop status pill
    if perpetual_paused:
        loop_pill = '<span class="pill pill-warn">⏸ Paused</span>'
    elif is_running_now:
        loop_pill = '<span class="pill pill-live">● Live — scraping now</span>'
    else:
        loop_pill = '<span class="pill pill-idle">Idle (between batches)</span>'

    # Sydney panel — picks one image based on minute so it rotates on each auto-refresh
    sydney_idx = (datetime.utcnow().minute // 5) % len(SYDNEY_PICS)
    sydney_url = SYDNEY_PICS[sydney_idx]

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="20">
<title>Lead Engine — Live</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, "SF Pro Text", Segoe UI, sans-serif;
    background: #f6f7fa; color: #1b2440; margin: 0; padding: 24px;
    max-width: 1200px; margin-left: auto; margin-right: auto;
  }}
  h1 {{ margin: 0; font-size: 28px; }}
  h2 {{ margin: 32px 0 12px; font-size: 18px; color: #4b5563; font-weight: 600; }}
  .header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 4px; }}
  .muted {{ color: #6b7280; font-size: 13px; }}
  .pill {{ display: inline-block; padding: 4px 12px; border-radius: 999px; font-size: 12px; font-weight: 600; }}
  .pill-live {{ background: #d1fae5; color: #047857; }}
  .pill-idle {{ background: #e5e7eb; color: #4b5563; }}
  .pill-warn {{ background: #fef3c7; color: #92400e; }}
  .stats {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 14px; margin-top: 20px;
  }}
  .stat-card {{
    background: white; padding: 18px; border-radius: 10px;
    border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,0.04);
  }}
  .stat-num {{ font-size: 36px; font-weight: 700; line-height: 1.2; }}
  .stat-label {{ font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }}
  .current-batch {{
    background: white; padding: 18px; border-radius: 10px;
    border: 1px solid #e5e7eb; margin-top: 20px;
  }}
  .cb-header {{ display: flex; gap: 12px; align-items: center; margin-bottom: 10px; }}
  .cb-stats {{ display: flex; gap: 10px; margin-top: 8px; font-size: 14px; }}
  .progress-bar {{ background: #f3f4f6; border-radius: 999px; height: 10px; overflow: hidden; }}
  .progress-fill {{ background: linear-gradient(90deg, #6366f1, #8b5cf6); height: 100%; transition: width 0.3s; }}
  table {{
    width: 100%; border-collapse: collapse; background: white;
    border: 1px solid #e5e7eb; border-radius: 10px; overflow: hidden;
  }}
  th, td {{
    text-align: left; padding: 10px 14px; border-bottom: 1px solid #f3f4f6; font-size: 14px;
  }}
  th {{ background: #f9fafb; font-weight: 600; color: #4b5563; font-size: 12px; text-transform: uppercase; }}
  tr:last-child td {{ border-bottom: none; }}
  .badge {{ display: inline-block; padding: 2px 10px; border-radius: 999px; color: white; font-size: 11px; font-weight: 600; text-transform: uppercase; }}
  .dl-link {{ color: #6366f1; text-decoration: none; font-weight: 600; font-size: 13px; }}
  .dl-link:hover {{ text-decoration: underline; }}
  a {{ color: #4b5563; }}
  .auto-refresh {{ font-size: 11px; color: #9ca3af; margin-top: 24px; text-align: center; }}
  .sydney-panel {{
    margin-top: 24px;
    background: white;
    border: 1px solid #e5e7eb;
    border-radius: 10px;
    padding: 14px;
    display: flex;
    gap: 14px;
    align-items: center;
  }}
  .sydney-panel img {{
    width: 120px; height: 120px; border-radius: 10px; object-fit: cover;
    border: 2px solid #f3f4f6;
  }}
  .sydney-quote {{ font-style: italic; color: #4b5563; font-size: 14px; line-height: 1.5; }}
  .sydney-name {{ font-weight: 600; margin-top: 8px; font-size: 13px; color: #1b2440; }}
</style>
</head>
<body>

<div class="header">
  <h1>Lead Engine</h1>
  {loop_pill}
</div>
<div class="muted">Tier-A verified US founder leads, scraped continuously.</div>

<div class="stats">
  <div class="stat-card">
    <div class="stat-num">{total:,}</div>
    <div class="stat-label">Total Tier-A leads</div>
  </div>
  <div class="stat-card">
    <div class="stat-num">{loop_state.get('completed_batches', 0)}</div>
    <div class="stat-label">Batches completed</div>
  </div>
  <div class="stat-card">
    <div class="stat-num">{loop_state.get('current_target', settings.BATCH_SIZE):,}</div>
    <div class="stat-label">Current batch target</div>
  </div>
  <div class="stat-card">
    <div class="stat-num">{settings.BATCH_SIZE_MAX:,}</div>
    <div class="stat-label">Max target (ramp ceiling)</div>
  </div>
</div>

{cb_html}

<h2>Recent batches</h2>
<table>
  <thead><tr>
    <th>Batch</th><th>Status</th><th>Leads</th><th>Duration</th><th>Started</th><th>Trigger</th><th></th>
  </tr></thead>
  <tbody>
    {batches_html}
  </tbody>
</table>

<h2>Latest 8 leads</h2>
<table>
  <thead><tr>
    <th>Name</th><th>Email</th><th>Business</th><th>Website</th><th>Source</th><th>Added</th>
  </tr></thead>
  <tbody>
    {leads_table}
  </tbody>
</table>

<div class="sydney-panel">
  <img src="{sydney_url}" alt="Motivation" loading="lazy" onerror="this.style.display='none'">
  <div>
    <div class="sydney-quote">"The engine never sleeps. Neither should your pipeline."</div>
    <div class="sydney-name">— Sydney, probably</div>
  </div>
</div>

<div class="auto-refresh">Auto-refreshes every 20 seconds · Last updated: {datetime.utcnow().strftime("%H:%M:%S")} UTC</div>

</body>
</html>"""
