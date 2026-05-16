# Lead Engine

Continuously scrapes US founder interviews from public sources, finds emails on their websites, verifies them with Reoon (Tier A / SMTP-confirmed only), dedupes against history, and emails you a CSV.

## What it does

- Pulls fresh URLs from ~20 US founder-interview sources every run
- Skips any URL already seen (no re-scraping ever)
- Verifies emails through Reoon **power mode** — only accepts `status=safe` (no catch-all)
- Stores everything in SQLite (single file in `./data/`)
- Emails the CSV to `DELIVERY_EMAIL` when a batch completes
- Provides HTTP `/run`, `/status`, `/download` endpoints

## Local dev

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill in REOON_API_KEY and SMTP creds
uvicorn app:app --reload
```

Trigger a batch:
```bash
curl -X POST localhost:8000/run?target=500
```

Check status:
```bash
curl localhost:8000/status
```

## Deploy to Railway

1. Push this repo to GitHub.
2. In Railway: **New Project → Deploy from GitHub repo**.
3. In the service's **Variables** tab, add:
   - `REOON_API_KEY` — your Reoon key
   - `SMTP_HOST=smtp.gmail.com`, `SMTP_PORT=587`
   - `SMTP_USER` — one of your Google Workspace emails (e.g. `rick@digitalnetworkingagency.com`)
   - `SMTP_PASS` — a Google **App Password** (Account → Security → 2FA → App passwords)
   - `DELIVERY_EMAIL=sam@digitalnetworkingagency.com`
   - `PUBLIC_BASE_URL` — Railway gives you a domain like `https://lead-engine.up.railway.app` — paste it here so download links work
   - `API_TOKEN` — optional, a random string to protect `/run`, `/download`, `/leads/recent`
4. Add a **Volume** mounted at `/app/data` so SQLite + CSVs persist across deploys.
5. The cron runs automatically: Monday 06:00 UTC by default (configurable via `CRON_DAY`, `CRON_HOUR`).
6. To trigger an extra batch any time:
   ```bash
   curl -X POST -H "Authorization: Bearer $API_TOKEN" https://your-app.up.railway.app/run?target=2000
   ```

## How batches behave

- Each run targets `DEFAULT_TARGET` Tier-A leads (default 2,000).
- Run stops early when target hit. Pending URLs stay in the pool for next run.
- Verified leads are deduped by email across ALL history — no duplicates.
- When sources run dry, the run completes with whatever was found (you'll see `"No unseen URLs"` in the batch notes).

## Endpoints

- `GET /` — service info + current batch
- `GET /health` — for Railway's healthcheck
- `GET /status` — total leads ever, current batch progress, last 10 batches
- `POST /run?target=N` — start a batch (auth required if `API_TOKEN` set)
- `GET /download/{filename}` — fetch a batch CSV (returned in /status)
- `GET /leads/recent?limit=50` — peek at the latest leads added

## Adding more sources

Each source is a small async function in `pipeline/sources.py` that returns a list of URLs. Add new ones (Brainz Magazine, Disrupt Magazine, Authority Magazine RSS, podcast guests) by:

1. Add a new function `async def brainz_urls() -> list[str]: ...`
2. Call it from `collect_all_urls()`.
3. Add a label match in `source_label()`.

The parser in `pipeline/parser.py` works for most "founder interview" pages out of the box — it pulls the H1 as the name, looks for `Website:` labels and external non-social links, and finds emails via standard `/contact`, `/about` page sweeps + pattern generation. If a new source has a unique page structure, add specialized parsing inside `parse_article()`.

## Reoon credit budget

A Tier-A lead burns ~3-6 Reoon power-mode calls on average (it tries several email candidates until one verifies). For 2,000 Tier-A leads per batch, plan for ~8,000-12,000 verifications. Check your Reoon plan accordingly.

If you run out, the system gracefully fails: it logs and continues — you'll just get fewer Tier-A in that batch.
