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
    from pipeline.mv_verifier import get_state as mv_state, _load_counter as _mv_load
    await _sk_load()
    await _mv_load()
    sk = skrapp_state()
    mv = mv_state()
    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one()
        responded = (await s.execute(
            select(func.count()).select_from(VerifiedLead).where(VerifiedLead.responded == True)
        )).scalar_one() or 0
        recent_batches = (await s.execute(select(Batch).order_by(desc(Batch.id)).limit(15))).scalars().all()
        recent_leads = (await s.execute(select(VerifiedLead).order_by(desc(VerifiedLead.id)).limit(8))).scalars().all()

    # Get reply insights if there's data
    insights_html = ""
    if responded > 0:
        try:
            from pipeline.unibox import get_reply_insights
            ins = await get_reply_insights()
            top_sources = "".join(f'<li>{s}: <b style="color:#00ffa8">{n}</b></li>' for s, n in (ins.get("top_sources") or [])[:5])
            top_kw = "".join(f'<span class="kw-pill">{k} ({n})</span>' for k, n in (ins.get("top_niche_keywords") or [])[:12])
            top_tlds = "".join(f'<li>{t}: <b style="color:#00ffa8">{n}</b></li>' for t, n in (ins.get("top_tlds") or [])[:5])
            free = ins.get("domain_split", {}).get("free_pct", 0)
            insights_html = f"""
        <h2>// reply insights — who responds to your outreach</h2>
        <div class="insights-grid">
          <div class="insights-card">
            <div class="ins-label">Total Responders</div>
            <div class="ins-big">{ins.get("total_responders", 0)}</div>
            <div class="muted">{free}% on free providers (gmail/yahoo/etc) · rest on business domains</div>
          </div>
          <div class="insights-card">
            <div class="ins-label">Top Reply Sources</div>
            <ul class="ins-list">{top_sources}</ul>
          </div>
          <div class="insights-card">
            <div class="ins-label">Top Business TLDs</div>
            <ul class="ins-list">{top_tlds}</ul>
          </div>
        </div>
        <div class="kw-cloud-label">Niche keywords from responding companies</div>
        <div class="kw-cloud">{top_kw}</div>
"""
        except Exception:
            pass

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
  .dl-all-btn, .ctrl-btn {{
    display: inline-block; margin-top: 12px; padding: 12px 22px;
    background: transparent;
    border: 1px solid #00ffa8;
    color: #00ffa8;
    text-decoration: none; font-weight: 700;
    font-size: 13px; letter-spacing: 2px; text-transform: uppercase;
    box-shadow: 0 0 14px rgba(0,255,168,0.25);
    transition: all 0.2s;
    cursor: pointer;
    font-family: inherit;
  }}
  .dl-all-btn:hover, .ctrl-btn:hover {{
    background: rgba(0,255,168,0.1);
    box-shadow: 0 0 28px rgba(0,255,168,0.6);
  }}
  .ctrl-pause {{ border-color: #ffe66b; color: #ffe66b; box-shadow: 0 0 14px rgba(255,230,107,0.25); }}
  .ctrl-pause:hover {{ background: rgba(255,230,107,0.1); box-shadow: 0 0 28px rgba(255,230,107,0.6); }}
  .ctrl-resume {{ border-color: #00ffa8; color: #00ffa8; }}
  .control-bar {{ margin-top: 16px; display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }}
  .range-box {{
    margin-top: 20px; padding: 18px;
    background: rgba(0,25,15,0.7);
    border: 1px solid rgba(0,255,255,0.4);
    box-shadow: 0 0 20px rgba(0,255,255,0.08);
  }}
  .range-title {{ font-size: 13px; color: #00ffff; font-weight: 700; letter-spacing: 2px; text-transform: uppercase; margin-bottom: 8px; text-shadow: 0 0 8px rgba(0,255,255,0.4); }}
  .range-form {{ display: flex; gap: 14px; flex-wrap: wrap; align-items: center; }}
  .range-form label {{ font-size: 12px; color: #65a08a; text-transform: uppercase; letter-spacing: 1px; }}
  .range-input {{
    width: 120px; padding: 9px 12px; margin-left: 4px;
    background: #000; border: 1px solid #00ffa8; color: #00ffa8;
    font-family: inherit; font-size: 14px; font-weight: 700;
  }}
  .range-input:focus {{ outline: none; box-shadow: 0 0 12px rgba(0,255,168,0.5); }}
  .insights-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(220px,1fr));
    gap: 14px; margin-top: 12px;
  }}
  .insights-card {{
    background: rgba(0,25,15,0.7);
    border: 1px solid rgba(0,255,168,0.3);
    padding: 16px;
  }}
  .ins-label {{ font-size: 10px; color: #65a08a; text-transform: uppercase; letter-spacing: 2px; }}
  .ins-big {{ font-size: 34px; font-weight: 700; color: #00ffa8; margin-top: 4px; text-shadow: 0 0 12px rgba(0,255,168,0.5); }}
  .ins-list {{ list-style:none; padding:0; margin:8px 0 0; font-size: 13px; color:#d8ffe8; }}
  .ins-list li {{ padding: 3px 0; }}
  .kw-cloud-label {{ font-size:12px; color:#65a08a; text-transform:uppercase; letter-spacing:2px; margin-top: 20px; }}
  .kw-cloud {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }}
  .kw-pill {{
    display: inline-block; padding: 4px 12px;
    border: 1px solid #00ffff; color: #00ffff;
    font-size: 12px; border-radius: 0;
    background: rgba(0,255,255,0.05);
  }}
  .paused-banner {{
    background: rgba(255,230,107,0.1);
    border: 1px solid #ffe66b; color: #ffe66b;
    padding: 14px 20px; margin-top: 20px;
    text-align: center; font-weight: 700; letter-spacing: 3px;
    text-transform: uppercase; font-size: 14px;
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
    <div class="stat-num">{mv['calls']:,}</div>
    <div class="stat-label">MV Verifier Calls (primary)</div>
  </div>
</div>

<div style="margin-top:14px; font-size:12px; color:#65a08a;">
  <span style="color:#00ffa8">MILLIONVERIFIER:</span>
  {mv['calls']:,} calls · {mv['hits']:,} hits ({mv['hit_rate']:.0f}% Tier-A rate)
  · {'⚠ QUOTA EXHAUSTED' if mv['quota_exhausted'] else ('✓ enabled' if mv['enabled'] else '○ disabled')}
  &nbsp;|&nbsp;
  <span style="color:#00ffa8">REOON</span> (fallback): {REOON_CALLS:,} calls
  &nbsp;|&nbsp;
  <span style="color:#00ffa8">SKRAPP</span>: {sk['calls']:,}/{sk['hits']:,} ({(100*sk['hits']/sk['calls']) if sk['calls'] else 0:.0f}%)
  · {'⚠' if sk['quota_exhausted'] else ('✓' if sk['enabled'] else '○')}
</div>

<div class="control-bar">
  <a href="{base}/download-all.csv" class="dl-all-btn">⬇ Download all {total:,} leads</a>
  {'<form method="post" action="/control/resume" style="display:inline"><button class="ctrl-btn ctrl-resume" type="submit">▶ RESUME ENGINE</button></form>' if perpetual_paused else '<form method="post" action="/control/pause" style="display:inline"><button class="ctrl-btn ctrl-pause" type="submit">⏸ PAUSE ENGINE</button></form>'}
</div>
{'<div class="paused-banner">⏸ ENGINE PAUSED — no credits being burned · unibox + analysis still running</div>' if perpetual_paused else ''}

<div class="range-box">
  <div class="range-title">⬇ Download by range — grab the next batch without repeats</div>
  <div class="muted" style="margin-bottom:10px">
    Numbering is over your <b style="color:#00ffa8">{total:,}</b> clean leads (bounced &amp; catch-all already excluded), oldest first.
    Already pulled the first 8,000? Enter <b>8001</b> → <b>10000</b> for the next 2,000.
  </div>
  <form method="get" action="{base}/download-range" class="range-form">
    <label>From&nbsp;<input type="number" name="start" min="1" value="1" class="range-input"></label>
    <label>To&nbsp;<input type="number" name="end" min="1" value="2000" class="range-input"></label>
    <button type="submit" class="ctrl-btn">⬇ Download range</button>
  </form>
</div>

{cb_html}

{insights_html}

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
