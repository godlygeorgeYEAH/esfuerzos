"""
notify_pipeline.py - Proactive WhatsApp match notifications for Reune VE.

When a family reports a missing person via WhatsApp, the synchronous search at
report time only catches what is already in the DB. Scrapers keep ingesting and
the 4h text/face cross-match jobs keep finding new matches. This pipeline closes
the loop: it scans matches found LATER, and if one involves a report tied to a
WhatsApp reporter (via bot_subscribers), it messages that family.

Flow (run_match_notifier):
  1. Fetch matches with notify_sent=false and combined_score >= threshold.
  2. Look up bot_subscribers for the report ids on both sides of each match.
  3. For a match where one side has a subscriber, fetch the OTHER report's
     name/location/source and send the family a preliminary, non-confirming
     WhatsApp message.
  4. Mark matches.notify_sent=true ONLY after a successful send, so a network
     failure retries next run instead of silently dropping the notification.

Safety (project constraints):
  - Never confirms a match ("posible coincidencia ... en verificación").
  - Never states a person is deceased.
  - Conservative score gate to avoid giving false hope.

Registered on the APScheduler in main.py (every ~10 min, max_instances=1).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from config import get_settings
# El envío WhatsApp vive ahora en el bot prox (un solo proceso). main.py añade
# migration_prox a sys.path antes de importar este módulo, así que app.* resuelve.
from app.services.waha import send_message as _waha_send_raw

logger = logging.getLogger(__name__)
settings = get_settings()

# Etiquetas legibles por fuente (antes vivían en waha_intake.py, ya retirado).
SOURCE_LABELS: dict[str, str] = {
    "sos_laguaira": "SOS La Guaira",
    "venezreporta": "Venezuela Reporta",
    "sos_venezuela": "SOS Venezuela",
    "terremotove": "TerremotoVE",
    "pacientes_terremoto": "Pacientes en Hospitales",
    "localizados_venezuela": "Localizados Venezuela",
    "venezuela_te_busca": "Venezuela Te Busca",
    "reconexion": "Reconexión",
    "google_drive_hospital": "Directorio Hospitalario",
    "red_solidaria_venezuela": "Red Solidaria Venezuela",
    "hospitales_ve": "Hospitales VE",
    "redayuda_ve": "Red Ayuda VE",
    "tuayudave": "Tu Ayuda VE",
    "waha_whatsapp": "Reúne VE (WhatsApp)",
}


def _source_label(source: str) -> str:
    return SOURCE_LABELS.get(source, source or "fuente externa")


async def _waha_send(phone: str, text: str) -> bool:
    """Envía un texto por WAHA vía el servicio de prox. True si tuvo éxito."""
    return bool(await _waha_send_raw(phone=phone, message=text, session=settings.waha_session))

# GP rule: notify a family ONLY after a human has verified the match
# (matches.status='confirmed'). No auto-notify on raw algorithmic score — a false
# positive sent as fact to a grieving family is the worst-case failure. The score
# threshold instead governs which matches surface in the human review queue
# (see /admin/matches). Confirmed → notify the subscribed family.
_COOLDOWN_HOURS = 24
_BATCH = 100


def _hdr(key: str, prefer: str = "") -> dict:
    h = {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    return h


def _message(reported_name: str | None, other: dict) -> str:
    who = reported_name or "tu familiar"
    loc = other.get("last_seen_location") or "ubicación por confirmar"
    src = _source_label(other.get("source", ""))
    matched_name = other.get("full_name") or "una persona"
    return (
        f"🔔 Reúne VE: encontramos una *posible coincidencia* para {who}.\n"
        f"Coincide con: *{matched_name}* — {loc} (via {src}).\n\n"
        "Esto es preliminar y está *en verificación*, no es una confirmación. "
        "Nuestro equipo revisará y te contactará. "
        "Si ya reuniste a tu familiar, respóndenos para cerrar el caso."
    )


def _within_cooldown(last_notified_at: str | None, now: datetime) -> bool:
    if not last_notified_at:
        return False
    try:
        prev = datetime.fromisoformat(last_notified_at.replace("Z", "+00:00"))
        return (now - prev).total_seconds() < _COOLDOWN_HOURS * 3600
    except Exception:
        return False


async def run_match_notifier(app: Any) -> dict:
    sb = app.state.supabase_url.rstrip("/")
    key = app.state.supabase_service_key
    now = datetime.now(timezone.utc)
    sent = 0
    errors = 0
    checked = 0
    notified_this_run: set[str] = set()  # report_ids messaged this run (no double-fire)

    try:
        async with httpx.AsyncClient(timeout=20) as cl:
            # 1. HUMAN-CONFIRMED, not-yet-notified matches only.
            mr = await cl.get(
                f"{sb}/rest/v1/matches",
                headers=_hdr(key),
                params={
                    "select": "id,missing_id,found_id,combined_score,notify_sent,status",
                    "notify_sent": "eq.false",
                    "status": "eq.confirmed",
                    "order": "combined_score.desc",
                    "limit": str(_BATCH),
                },
            )
            if mr.status_code != 200:
                logger.warning("notifier: matches fetch %d: %s", mr.status_code, mr.text[:120])
                return {"checked": 0, "sent": 0, "errors": 1}
            matches = mr.json() or []
            checked = len(matches)
            if not matches:
                return {"checked": 0, "sent": 0, "errors": 0}

            # 2. Subscribers for all involved report ids
            ids = set()
            for m in matches:
                for k in ("missing_id", "found_id"):
                    if m.get(k):
                        ids.add(m[k])
            id_list = ",".join(f'"{i}"' for i in ids)
            sr = await cl.get(
                f"{sb}/rest/v1/bot_subscribers",
                headers=_hdr(key),
                params={"select": "report_id,phone,full_name,last_notified_at",
                        "report_id": f"in.({id_list})"},
            )
            subs = {s["report_id"]: s for s in (sr.json() if sr.status_code == 200 else [])}
            if not subs:
                return {"checked": checked, "sent": 0, "errors": 0}

            for m in matches:
                miss, found = m.get("missing_id"), m.get("found_id")
                # Notify EACH subscribed side (both families may be reachable).
                sides = []
                if miss in subs:
                    sides.append((subs[miss], found))
                if found in subs:
                    sides.append((subs[found], miss))

                all_handled = True
                for sub, other_id in sides:
                    rid = sub["report_id"]
                    if rid in notified_this_run or _within_cooldown(sub.get("last_notified_at"), now):
                        continue  # respect per-family cooldown; match stays unnotified
                    if not other_id:
                        continue

                    # Fetch the matched (other) report; skip if missing/empty (no false-hope blanks)
                    rr = await cl.get(
                        f"{sb}/rest/v1/reports",
                        headers=_hdr(key),
                        params={"id": f"eq.{other_id}",
                                "select": "full_name,last_seen_location,source"},
                    )
                    rows = rr.json() if rr.status_code == 200 else []
                    if not rows:
                        all_handled = False
                        continue
                    other = rows[0]
                    if not other.get("full_name") and not other.get("last_seen_location"):
                        all_handled = False
                        continue

                    ok = await _waha_send(sub["phone"], _message(sub.get("full_name"), other))
                    if not ok:
                        errors += 1
                        all_handled = False
                        continue

                    pr = await cl.patch(
                        f"{sb}/rest/v1/bot_subscribers",
                        headers=_hdr(key, "return=minimal"),
                        params={"report_id": f"eq.{rid}"},
                        json={"last_notified_at": now.isoformat()},
                    )
                    if pr.status_code not in (200, 204):
                        logger.warning("notifier: subscriber PATCH %d", pr.status_code)
                    notified_this_run.add(rid)
                    sent += 1

                # Mark the match consumed only if every subscribed side was handled.
                if sides and all_handled:
                    mp = await cl.patch(
                        f"{sb}/rest/v1/matches",
                        headers=_hdr(key, "return=minimal"),
                        params={"id": f"eq.{m['id']}"},
                        json={"notify_sent": True},
                    )
                    if mp.status_code not in (200, 204):
                        logger.warning("notifier: match PATCH %d (will retry next run)", mp.status_code)
    except Exception as exc:
        errors += 1
        logger.error("run_match_notifier error: %s", exc)

    result = {"checked": checked, "sent": sent, "errors": errors}
    if sent:
        logger.info("notifier: %s", result)
    return result
