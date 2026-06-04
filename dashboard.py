from __future__ import annotations
"""HTML dashboard — Lead Cleaning & Delivery Hub (DNA Neon theme).

The portal pivoted from scraping to CSV import: upload a Skrapp Lead Search /
Instantly SuperSearch export, it verifies + dedupes + niche-tags, and you
download clean numbered batches for Instantly.
"""
import os
from datetime import datetime, timezone
from sqlalchemy import select, desc, func
from db import SessionLocal, VerifiedLead
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


_CSS = """
* { box-sizing: border-box; }
body { background:#05080a; color:#c8ffe6; font-family:'SF Mono',Menlo,Consolas,monospace;
       margin:0; padding:28px 32px 80px; }
a { color:#00ffff; text-decoration:none; }
h1 { font-size:30px; letter-spacing:3px; margin:0; color:#00ffa8; text-shadow:0 0 14px #00ffa855; }
h2 { font-size:14px; letter-spacing:2px; color:#5ad; margin:34px 0 14px; text-transform:uppercase; }
.sub { color:#5f8; opacity:.6; font-size:12px; letter-spacing:2px; margin-top:6px; }
.muted { color:#6b8b7e; }
.stat-row { display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-top:24px; }
.stat { border:1px solid #0c3; border-radius:10px; padding:18px 20px; background:#06140e;
        box-shadow:inset 0 0 24px #00ffa814; }
.stat .big { font-size:38px; font-weight:700; color:#00ffa8; text-shadow:0 0 12px #00ffa844; }
.stat .lbl { font-size:11px; letter-spacing:2px; color:#6b8b7e; margin-top:6px; text-transform:uppercase; }
.verline { margin-top:14px; font-size:12px; color:#7fb; opacity:.8; }
.hero { margin-top:30px; border:1.5px dashed #0c6; border-radius:14px; background:#06140e;
        padding:26px 28px; }
.hero h3 { margin:0 0 4px; color:#00ffff; letter-spacing:2px; font-size:16px; }
.drop { margin-top:18px; border:2px dashed #0a5; border-radius:12px; padding:30px; text-align:center;
        background:#04100b; transition:.15s; cursor:pointer; }
.drop.drag { background:#0a2a1c; border-color:#00ffa8; }
.drop input { display:none; }
.drop .big { font-size:16px; color:#00ffa8; }
.row { display:flex; align-items:center; gap:14px; flex-wrap:wrap; margin-top:16px; }
.btn { background:#00ffa8; color:#022; border:none; border-radius:8px; padding:11px 22px;
       font-weight:700; letter-spacing:1px; cursor:pointer; font-family:inherit; font-size:13px; }
.btn:hover { box-shadow:0 0 16px #00ffa877; }
.btn.ghost { background:transparent; color:#00ffa8; border:1px solid #0a6; }
.btn:disabled { opacity:.45; cursor:not-allowed; box-shadow:none; }
.chk { color:#9fd; font-size:12px; display:flex; align-items:center; gap:8px; }
.result { margin-top:18px; font-size:13px; display:none; }
.result.show { display:block; }
.result .grid { display:grid; grid-template-columns:repeat(3,1fr); gap:10px; margin-top:10px; }
.result .cell { border:1px solid #0a4; border-radius:8px; padding:10px 12px; background:#04100b; }
.result .cell b { color:#00ffa8; font-size:20px; display:block; }
.result .cell span { font-size:10px; letter-spacing:1px; color:#6b8b7e; text-transform:uppercase; }
.panel { margin-top:18px; border:1px solid #0a3a28; border-radius:12px; padding:20px 22px; background:#06120d; }
.dl-row { display:flex; align-items:flex-end; gap:14px; flex-wrap:wrap; }
.fld { display:flex; flex-direction:column; gap:6px; }
.fld label { font-size:10px; letter-spacing:1px; color:#6b8b7e; text-transform:uppercase; }
.fld input { background:#04100b; border:1px solid #0a5; color:#00ffa8; border-radius:8px;
             padding:10px 12px; width:130px; font-family:inherit; font-size:14px; }
.nb { margin-top:8px; }
.nb .bar { display:flex; align-items:center; gap:10px; margin:7px 0; font-size:12px; }
.nb .barfill { height:14px; background:linear-gradient(90deg,#00ffa8,#00ffff); border-radius:4px; min-width:2px; }
.nb .nm { width:170px; color:#9fd; }
.nb .ct { color:#00ffa8; width:60px; }
table { width:100%; border-collapse:collapse; margin-top:10px; font-size:12px; }
th { text-align:left; color:#6b8b7e; font-weight:400; letter-spacing:1px; padding:8px 10px;
     border-bottom:1px solid #0a3a28; text-transform:uppercase; font-size:10px; }
td { padding:9px 10px; border-bottom:1px solid #08231a; }
.pill { padding:3px 10px; border-radius:20px; font-size:10px; letter-spacing:1px; }
.pill-import { background:#063; color:#00ffa8; }
.ins-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; }
.ins-card { border:1px solid #0a3a28; border-radius:10px; padding:16px; background:#06120d; }
.ins-card .il { font-size:10px; letter-spacing:1px; color:#6b8b7e; text-transform:uppercase; }
.ins-card .ib { font-size:30px; color:#00ffa8; }
.ins-card ul { list-style:none; padding:0; margin:8px 0 0; font-size:12px; }
.kw { display:inline-block; background:#062a1d; color:#7fe; border-radius:14px; padding:3px 9px; margin:3px; font-size:11px; }
.spin { display:inline-block; width:14px; height:14px; border:2px solid #0a5; border-top-color:#00ffa8;
        border-radius:50%; animation:sp .7s linear infinite; vertical-align:middle; }
@keyframes sp { to { transform:rotate(360deg); } }
"""

_JS = """
const drop = document.getElementById('drop');
const fileInput = document.getElementById('file');
const btn = document.getElementById('uploadBtn');
const res = document.getElementById('result');
let chosen = null;

drop.addEventListener('click', () => fileInput.click());
drop.addEventListener('dragover', e => { e.preventDefault(); drop.classList.add('drag'); });
drop.addEventListener('dragleave', () => drop.classList.remove('drag'));
drop.addEventListener('drop', e => {
  e.preventDefault(); drop.classList.remove('drag');
  if (e.dataTransfer.files.length) { chosen = e.dataTransfer.files[0]; showChosen(); }
});
fileInput.addEventListener('change', () => { if (fileInput.files.length) { chosen = fileInput.files[0]; showChosen(); } });
function showChosen() { document.getElementById('dropmsg').textContent = '📄 ' + chosen.name; btn.disabled = false; }

btn.addEventListener('click', async () => {
  if (!chosen) return;
  btn.disabled = true;
  const orig = btn.innerHTML;
  btn.innerHTML = '<span class="spin"></span> verifying…';
  const fd = new FormData();
  fd.append('file', chosen);
  const verify = document.getElementById('verify').checked;
  try {
    const r = await fetch('/upload-csv?verify=' + verify, { method:'POST', body:fd, credentials:'same-origin' });
    const d = await r.json();
    renderResult(d);
  } catch (e) {
    res.className = 'result show';
    res.innerHTML = '<span style="color:#ff3a6e">Upload failed: ' + e + '</span>';
  }
  btn.innerHTML = orig; btn.disabled = false;
});

function renderResult(d) {
  res.className = 'result show';
  if (!d.ok && d.error) {
    res.innerHTML = '<span style="color:#ff3a6e">' + d.error + '</span>'; return;
  }
  if (d.error) {
    res.innerHTML = '<span style="color:#ff3a6e">' + d.error + '</span>'; return;
  }
  res.innerHTML =
    '<div style="color:#00ffa8">✓ Imported <b>' + d.filename + '</b> — ' + d.added +
    ' new clean leads added. Portal now holds ' + (d.portal_total||'—') + '.</div>' +
    '<div class="grid">' +
      cell(d.total_rows, 'rows in file') +
      cell(d.added, 'added (new + verified)') +
      cell(d.duplicates, 'already in portal') +
      cell(d.verified_ok, 'MV confirmed ok') +
      cell(d.catch_all_dropped, 'catch-all dropped') +
      cell(d.dropped_unverifiable + d.bad_format + d.no_email, 'bad / undeliverable') +
    '</div>';
}
function cell(v, l) { return '<div class="cell"><b>' + (v||0) + '</b><span>' + l + '</span></div>'; }
"""


async def render_dashboard(loop_state: dict, perpetual_paused: bool,
                           current_batch: dict | None, is_running_now: bool) -> str:
    from pipeline.mv_verifier import get_state as mv_state, _load_counter as _mv_load
    from pipeline.delivery import _deliverable_filter
    await _mv_load()
    mv = mv_state()

    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one()
        clean = (await s.execute(
            select(func.count()).select_from(VerifiedLead).where(*_deliverable_filter()))).scalar_one()
        bounced = (await s.execute(
            select(func.count()).select_from(VerifiedLead).where(VerifiedLead.bounced == True))).scalar_one() or 0
        responded = (await s.execute(
            select(func.count()).select_from(VerifiedLead).where(VerifiedLead.responded == True))).scalar_one() or 0
        # Niche breakdown (top 10) over clean leads
        niche_rows = (await s.execute(
            select(VerifiedLead.niche, func.count(VerifiedLead.id))
            .where(*_deliverable_filter())
            .group_by(VerifiedLead.niche).order_by(desc(func.count(VerifiedLead.id))).limit(10)
        )).all()
        recent = (await s.execute(
            select(VerifiedLead).order_by(desc(VerifiedLead.id)).limit(10))).scalars().all()

    # Niche bars
    nmax = max([c for _, c in niche_rows], default=1) or 1
    nb = ""
    for niche, c in niche_rows:
        w = int(100 * c / nmax)
        nb += (f'<div class="bar"><span class="nm">{(niche or "—")[:24]}</span>'
               f'<div class="barfill" style="width:{w}%"></div><span class="ct">{c:,}</span></div>')
    if not nb:
        nb = '<div class="muted">No leads yet — upload a CSV to begin.</div>'

    # Recent leads
    rl = ""
    for l in recent:
        nm = ((l.first_name or "") + " " + (l.last_name or "")).strip() or (l.name or "—")
        rl += (f'<tr><td>{nm[:28]}</td><td><a href="mailto:{l.email}">{l.email}</a></td>'
               f'<td class="muted">{(l.company or "")[:28]}</td>'
               f'<td class="muted">{(l.niche or "")[:20]}</td>'
               f'<td><span class="pill pill-import">{(l.source or "")[:18]}</span></td>'
               f'<td class="muted">{human_time(l.created_at)}</td></tr>')
    if not rl:
        rl = '<tr><td colspan="6" class="muted">No leads yet</td></tr>'

    # Reply insights (only if data)
    insights = ""
    if responded > 0:
        try:
            from pipeline.unibox import get_reply_insights
            ins = await get_reply_insights()
            top_src = "".join(f"<li>{x}: <b>{n}</b></li>" for x, n in (ins.get("top_sources") or [])[:5])
            top_kw = "".join(f'<span class="kw">{k} ({n})</span>' for k, n in (ins.get("top_niche_keywords") or [])[:12])
            free = ins.get("domain_split", {}).get("free_pct", 0)
            insights = f"""
      <h2>// reply insights — who responds</h2>
      <div class="ins-grid">
        <div class="ins-card"><div class="il">Total Responders</div><div class="ib">{ins.get("total_responders",0)}</div>
          <div class="muted" style="font-size:11px">{free}% free providers · rest business domains</div></div>
        <div class="ins-card"><div class="il">Top Reply Sources</div><ul>{top_src}</ul></div>
        <div class="ins-card"><div class="il">Niche keywords (responders)</div><div style="margin-top:8px">{top_kw}</div></div>
      </div>"""
        except Exception:
            pass

    verline = (f'MillionVerifier: {mv.get("calls",0):,} checks · {mv.get("hits",0):,} deliverable'
               f' · {"✓ active" if mv.get("enabled") else "⚠ no key"}'
               f'{" · ⚠ credits exhausted" if mv.get("quota_exhausted") else ""}')

    body = f"""
    <div style="display:flex;align-items:center;gap:16px">
      <h1>LEAD HUB</h1>
      <span class="pill pill-import">CSV IMPORT · VERIFIED · DEDUPED</span>
    </div>
    <div class="sub">// upload skrapp / supersearch exports → cleaned, deduped, bounce-tracked → download for instantly</div>

    <div class="stat-row">
      <div class="stat"><div class="big">{clean:,}</div><div class="lbl">Clean leads (downloadable)</div></div>
      <div class="stat"><div class="big">{total:,}</div><div class="lbl">Total in portal</div></div>
      <div class="stat"><div class="big">{bounced:,}</div><div class="lbl">Bounced · auto-excluded</div></div>
      <div class="stat"><div class="big">{responded:,}</div><div class="lbl">Replied</div></div>
    </div>
    <div class="verline">{verline}</div>

    <div class="hero">
      <h3>⬆ UPLOAD A LEAD CSV</h3>
      <div class="muted" style="font-size:12px">From Skrapp Lead Search or Instantly SuperSearch → export CSV → drop it here. We dedupe, MillionVerifier-check every email, drop bad/catch-all, and niche-tag the rest.</div>
      <div class="drop" id="drop">
        <input type="file" id="file" accept=".csv,.tsv,.txt">
        <div class="big" id="dropmsg">Drop CSV here, or click to choose</div>
        <div class="muted" style="font-size:11px;margin-top:6px">.csv · up to 50MB</div>
      </div>
      <div class="row">
        <label class="chk"><input type="checkbox" id="verify" checked> Re-verify with MillionVerifier (recommended)</label>
        <button class="btn" id="uploadBtn" disabled>Import & Clean</button>
      </div>
      <div class="result" id="result"></div>
    </div>

    <h2>// download clean leads for instantly</h2>
    <div class="panel">
      <form class="dl-row" method="get" action="/download-range">
        <div class="fld"><label>From #</label><input type="number" name="start" value="1" min="1"></div>
        <div class="fld"><label>To #</label><input type="number" name="end" value="2000" min="1"></div>
        <button class="btn" type="submit">⬇ Download range</button>
        <a class="btn ghost" href="/download-all.csv">⬇ Download all {clean:,}</a>
      </form>
      <div class="muted" style="font-size:11px;margin-top:10px">Numbered over your {clean:,} clean leads (bounced & catch-all excluded), oldest first. Pulled 1–2000 already? Enter 2001 → 4000 next — no repeats.</div>
    </div>

    <h2>// niche breakdown</h2>
    <div class="panel nb">{nb}</div>

    {insights}

    <h2>// recently imported</h2>
    <div class="panel">
      <table>
        <tr><th>Name</th><th>Email</th><th>Company</th><th>Niche</th><th>Source</th><th>Added</th></tr>
        {rl}
      </table>
    </div>
    """

    return ("<!doctype html><html lang=en><head><meta charset=utf-8>"
            "<title>Lead Hub · DNA</title><style>" + _CSS + "</style></head><body>"
            + body + "<script>" + _JS + "</script></body></html>")
