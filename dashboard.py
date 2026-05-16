from __future__ import annotations
"""HTML dashboard — DNA Neon / Matrix theme."""
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
        "running":     "#00ffa8",  # neon green
        "completed":   "#00ffff",  # neon cyan
        "failed":      "#ff3a6e",  # neon red/pink
        "interrupted": "#8c8c8c",  # dim gray
    }
    color = colors.get(status, "#8c8c8c")
    return f'<span class="badge" style="background:{color};color:#000">{status}</span>'


async def render_dashboard(loop_state: dict, perpetual_paused: bool, current_batch: dict | None, is_running_now: bool) -> str:
    from pipeline.verifier import CALLS_MADE as REOON_CALLS
    from pipeline.finder import get_state as skrapp_state, _load_counter as _sk_load
    await _sk_load()
    sk = skrapp_state()
    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one()
        recent_batches = (await s.execute(select(Batch).order_by(desc(Batch.id)).limit(15))).scalars().all()
        recent_leads = (await s.execute(select(VerifiedLead).order_by(desc(VerifiedLead.id)).limit(8))).scalars().all()

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
            <span class="dot"></span> <strong>Batch #{current_batch.get('batch_id')}</strong>
            <span class="muted">running · target {target:,}</span>
          </div>
          <div class="progress-bar"><div class="progress-fill" style="width:{pct}%"></div></div>
          <div class="cb-stats">
            <span class="metric"><b>{verified_now}</b>&nbsp;verified</span>
            <span class="metric"><b>{raw}</b>&nbsp;raw</span>
            <span class="metric"><b>{scraped:,}</b>&nbsp;scraped</span>
          </div>
        </div>"""

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
          </tr>""")
    batches_html = "\n".join(rows) if rows else '<tr><td colspan="7" class="muted">No batches yet</td></tr>'

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
          </tr>""")
    leads_table = "\n".join(leads_rows) if leads_rows else '<tr><td colspan="6" class="muted">No leads yet</td></tr>'

    if perpetual_paused:
        loop_pill = '<span class="pill pill-warn">⏸ PAUSED</span>'
    elif is_running_now:
        loop_pill = '<span class="pill pill-live">● LIVE</span>'
    else:
        loop_pill = '<span class="pill pill-idle">IDLE</span>'

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="20">
<title>Lead Engine · DNA</title>
<style>
  * {{ box-sizing: border-box; }}
  html, body {{ margin: 0; padding: 0; }}
  body {{
    font-family: "SF Mono", "Menlo", "Monaco", "Consolas", monospace;
    background: #000;
    color: #d8ffe8;
    padding: 24px;
    min-height: 100vh;
    position: relative;
    overflow-x: hidden;
  }}

  /* Matrix code-rain background */
  #matrix {{
    position: fixed;
    top: 0; left: 0; width: 100vw; height: 100vh;
    z-index: 0;
    opacity: 0.18;
    pointer-events: none;
  }}

  /* Foreground container */
  .wrap {{ position: relative; z-index: 1; max-width: 1200px; margin: 0 auto; }}

  /* Headings + accents */
  h1 {{
    margin: 0;
    font-size: 30px;
    font-weight: 700;
    color: #00ffa8;
    text-shadow: 0 0 12px rgba(0,255,168,0.55), 0 0 32px rgba(0,255,168,0.25);
    letter-spacing: 2px;
    text-transform: uppercase;
  }}
  h2 {{
    margin: 32px 0 12px;
    font-size: 14px;
    color: #00ffff;
    font-weight: 700;
    letter-spacing: 4px;
    text-transform: uppercase;
    text-shadow: 0 0 8px rgba(0,255,255,0.4);
  }}
  .header {{ display: flex; align-items: center; gap: 14px; margin-bottom: 6px; }}
  .muted {{ color: #65a08a; font-size: 12px; }}
  .subtitle {{ color: #65a08a; font-size: 12px; letter-spacing: 2px; text-transform: uppercase; margin-bottom: 4px; }}

  /* Pill */
  .pill {{
    display: inline-block; padding: 5px 14px; border-radius: 0;
    font-size: 11px; font-weight: 700; letter-spacing: 2px;
    border: 1px solid currentColor;
    background: transparent;
  }}
  .pill-live  {{ color: #00ffa8; box-shadow: 0 0 12px rgba(0,255,168,0.5) inset, 0 0 8px rgba(0,255,168,0.3); animation: pulseLive 2s infinite; }}
  .pill-idle  {{ color: #8c8c8c; }}
  .pill-warn  {{ color: #ffe66b; }}
  @keyframes pulseLive {{ 0%, 100% {{ opacity:1 }} 50% {{ opacity:0.6 }} }}

  /* Big number cards */
  .stats {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 14px; margin-top: 22px;
  }}
  .stat-card {{
    background: rgba(0, 25, 15, 0.7);
    backdrop-filter: blur(4px);
    border: 1px solid rgba(0,255,168,0.4);
    padding: 18px;
    box-shadow: 0 0 20px rgba(0,255,168,0.08);
    position: relative;
  }}
  .stat-card::before {{
    content: "";
    position: absolute; top: 0; left: 0; width: 3px; height: 100%;
    background: linear-gradient(180deg, #00ffa8, #00ffff);
    box-shadow: 0 0 10px #00ffa8;
  }}
  .stat-num {{
    font-size: 38px; font-weight: 700; line-height: 1.1;
    color: #00ffa8;
    text-shadow: 0 0 12px rgba(0,255,168,0.6);
  }}
  .stat-label {{
    font-size: 10px; color: #65a08a;
    text-transform: uppercase; letter-spacing: 2px; margin-top: 6px;
  }}

  /* Download all button */
  .dl-all-btn {{
    display: inline-block; margin-top: 12px; padding: 12px 22px;
    background: transparent;
    border: 1px solid #00ffa8;
    color: #00ffa8;
    text-decoration: none; font-weight: 700;
    font-size: 13px; letter-spacing: 2px; text-transform: uppercase;
    box-shadow: 0 0 14px rgba(0,255,168,0.25), 0 0 0 rgba(0,255,168,0.5) inset;
    transition: all 0.2s;
  }}
  .dl-all-btn:hover {{
    background: rgba(0,255,168,0.1);
    box-shadow: 0 0 28px rgba(0,255,168,0.6), 0 0 0 rgba(0,255,168,0.5) inset;
  }}

  /* Current batch panel */
  .current-batch {{
    background: rgba(0,25,15,0.7);
    border: 1px solid rgba(0,255,255,0.4);
    padding: 18px;
    margin-top: 22px;
    box-shadow: 0 0 20px rgba(0,255,255,0.08);
  }}
  .cb-header {{ display: flex; gap: 12px; align-items: center; margin-bottom: 12px; }}
  .cb-stats {{ display: flex; gap: 22px; margin-top: 10px; font-size: 13px; }}
  .metric b {{ color: #00ffa8; font-size: 16px; text-shadow: 0 0 8px rgba(0,255,168,0.5); }}
  .progress-bar {{ background: rgba(255,255,255,0.05); border: 1px solid #00444a; height: 10px; }}
  .progress-fill {{
    background: linear-gradient(90deg, #00ffa8, #00ffff);
    height: 100%; transition: width 0.5s;
    box-shadow: 0 0 12px rgba(0,255,168,0.6);
  }}
  .dot {{
    display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    background: #00ffa8; box-shadow: 0 0 8px #00ffa8;
    animation: pulseLive 1.2s infinite;
  }}

  /* Tables */
  table {{
    width: 100%; border-collapse: collapse;
    background: rgba(0,15,10,0.7);
    border: 1px solid rgba(0,255,168,0.25);
  }}
  th, td {{
    text-align: left; padding: 10px 14px;
    border-bottom: 1px solid rgba(0,255,168,0.1);
    font-size: 13px;
    color: #d8ffe8;
  }}
  th {{
    background: rgba(0,30,20,0.8);
    color: #00ffff; font-weight: 700;
    font-size: 10px; text-transform: uppercase; letter-spacing: 2px;
  }}
  tr:last-child td {{ border-bottom: none; }}
  td a {{ color: #00ffa8; text-decoration: none; }}
  td a:hover {{ text-shadow: 0 0 6px rgba(0,255,168,0.6); }}
  .badge {{
    display: inline-block; padding: 2px 10px;
    font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px;
  }}
  .dl-link {{ color: #00ffff; font-weight: 700; font-size: 12px; }}

  .auto-refresh {{
    font-size: 10px; color: #4a7060;
    margin-top: 24px; text-align: center;
    letter-spacing: 2px; text-transform: uppercase;
  }}
</style>
</head>
<body>

<canvas id="matrix"></canvas>

<div class="wrap">

<div class="subtitle">// DNA · LEAD ENGINE · V1</div>
<div class="header">
  <h1>Lead Engine</h1>
  {loop_pill}
</div>
<div class="muted">// tier-a verified us founder leads — scraped continuously</div>

<div class="stats">
  <div class="stat-card">
    <div class="stat-num">{total:,}</div>
    <div class="stat-label">Tier-A Leads</div>
  </div>
  <div class="stat-card">
    <div class="stat-num">{loop_state.get('completed_batches', 0)}</div>
    <div class="stat-label">Batches Done</div>
  </div>
  <div class="stat-card">
    <div class="stat-num">{loop_state.get('current_target', settings.BATCH_SIZE):,}</div>
    <div class="stat-label">Current Target</div>
  </div>
  <div class="stat-card">
    <div class="stat-num">{REOON_CALLS:,}</div>
    <div class="stat-label">Reoon Verifier Calls</div>
  </div>
</div>

<div style="margin-top:14px; font-size:12px; color:#65a08a;">
  <span style="color:#00ffa8">SKRAPP:</span>
  {sk['calls']} calls · {sk['hits']} hits ({(100*sk['hits']/sk['calls']) if sk['calls'] else 0:.0f}% success)
  · {'⚠ QUOTA EXHAUSTED — running free-only' if sk['quota_exhausted'] else ('✓ enabled' if sk['enabled'] else '○ disabled')}
</div>

<div><a href="{base}/download-all.csv" class="dl-all-btn">⬇ Download all {total:,} leads</a></div>

{cb_html}

<h2>// recent batches</h2>
<table>
  <thead><tr>
    <th>Batch</th><th>Status</th><th>Leads</th><th>Duration</th><th>Started</th><th>Trigger</th><th></th>
  </tr></thead>
  <tbody>
    {batches_html}
  </tbody>
</table>

<h2>// latest leads</h2>
<table>
  <thead><tr>
    <th>Name</th><th>Email</th><th>Business</th><th>Website</th><th>Source</th><th>Added</th>
  </tr></thead>
  <tbody>
    {leads_table}
  </tbody>
</table>

<div class="auto-refresh">// auto-refresh 20s · {datetime.utcnow().strftime("%H:%M:%S")} UTC</div>

</div>

<script>
// Matrix-style code rain (Python keywords + DNA bases)
(function() {{
  const canvas = document.getElementById("matrix");
  const ctx = canvas.getContext("2d");
  function resize() {{ canvas.width = window.innerWidth; canvas.height = window.innerHeight; }}
  resize();
  window.addEventListener("resize", resize);

  const chars = "def class async await for in if elif else return yield import lambda True False None self while try except print 01ATCG{{}}()[]:=.,#".split("");
  const fontSize = 14;
  let columns = Math.floor(canvas.width / fontSize);
  let drops = new Array(columns).fill(1);

  window.addEventListener("resize", () => {{
    columns = Math.floor(canvas.width / fontSize);
    drops = new Array(columns).fill(1);
  }});

  function draw() {{
    ctx.fillStyle = "rgba(0, 0, 0, 0.08)";
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = "#00ffa8";
    ctx.font = fontSize + "px 'SF Mono', monospace";
    for (let i = 0; i < drops.length; i++) {{
      const txt = chars[Math.floor(Math.random() * chars.length)];
      ctx.fillText(txt, i * fontSize, drops[i] * fontSize);
      if (drops[i] * fontSize > canvas.height && Math.random() > 0.975) drops[i] = 0;
      drops[i]++;
    }}
  }}
  setInterval(draw, 60);
}})();
</script>

</body>
</html>"""
