from __future__ import annotations
"""CSV write + email delivery via SMTP (any provider)."""
import csv
import os
import smtplib
import logging
from email.message import EmailMessage
from datetime import datetime
from sqlalchemy import select
from db import SessionLocal, VerifiedLead, Batch
from config import settings

log = logging.getLogger("delivery")


async def deliver_batch(batch_id: int) -> str:
    """Write CSV for batch, attempt email delivery. Return CSV path."""
    async with SessionLocal() as s:
        result = await s.execute(
            select(VerifiedLead).where(VerifiedLead.batch_id == batch_id)
        )
        leads = result.scalars().all()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M")
    fname = f"leads_batch_{batch_id}_{timestamp}.csv"
    path = os.path.join(settings.DATA_DIR, fname)

    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Tier","First Name","Last Name","Email","Role","Business",
                    "Website","Source","Source URL","Reoon Status","Score","Catch-All"])
        for l in leads:
            w.writerow([
                l.tier, l.first_name or "", l.last_name or "",
                l.email, l.role or "", l.company or "",
                l.website or "", l.source or "", l.source_url or "",
                l.reoon_status or "", l.reoon_score or "",
                "Yes" if l.is_catch_all else "No",
            ])
    log.info(f"CSV written: {path} ({len(leads)} leads)")

    # Try email delivery
    try:
        if settings.SMTP_HOST and settings.SMTP_USER and settings.SMTP_PASS:
            send_email(
                to=settings.DELIVERY_EMAIL,
                subject=f"[Lead Engine] Batch #{batch_id} ready — {len(leads)} verified leads",
                body=build_email_body(batch_id, leads, path),
                attachments=[path] if len(leads) > 0 else [],
            )
            log.info(f"Email sent to {settings.DELIVERY_EMAIL}")
    except Exception as e:
        log.exception(f"Email delivery failed: {e}")

    return path


def build_email_body(batch_id: int, leads, path: str) -> str:
    tier_a = sum(1 for l in leads if l.tier == "A")
    src_counts: dict[str, int] = {}
    for l in leads:
        src_counts[l.source or "Other"] = src_counts.get(l.source or "Other", 0) + 1
    src_lines = "\n".join(f"  • {s}: {c}" for s, c in sorted(src_counts.items(), key=lambda x: -x[1]))

    download_link = ""
    if settings.PUBLIC_BASE_URL:
        download_link = f"\nDownload (also): {settings.PUBLIC_BASE_URL}/download/{os.path.basename(path)}\n"

    return f"""Hey,

Batch #{batch_id} is ready.

Total Tier-A verified leads: {tier_a}
{download_link}
Source breakdown:
{src_lines}

Hit Tier A first — these passed Reoon power-mode (SMTP-level) checks. Use the Source URL column to personalize ("saw your CanvasRebel feature on…").

— Lead Engine
"""


def send_email(to: str, subject: str, body: str, attachments: list[str] = None):
    msg = EmailMessage()
    msg["From"] = settings.SMTP_USER
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    for ap in (attachments or []):
        if not os.path.exists(ap):
            continue
        with open(ap, "rb") as f:
            data = f.read()
        msg.add_attachment(
            data, maintype="text", subtype="csv",
            filename=os.path.basename(ap),
        )

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as smtp:
        smtp.starttls()
        smtp.login(settings.SMTP_USER, settings.SMTP_PASS)
        smtp.send_message(msg)
