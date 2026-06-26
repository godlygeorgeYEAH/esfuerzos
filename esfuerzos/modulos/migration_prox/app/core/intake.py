"""
Intake — Crea Report y Photo desde el contexto de conversación.

Llamado por el Orchestrator al avanzar a reporte_guardado.
Lee context["intake_person_raw"] y context["pending_photos"],
crea los registros en DB y limpia ambas claves.
"""
import hashlib
import json
import logging
import re
from typing import Optional

from sqlalchemy.orm import Session

from app.models.reporte import Report, Photo

logger = logging.getLogger(__name__)


def hash_phone(phone: str) -> str:
    return hashlib.sha256(phone.encode()).hexdigest()


def parse_person_data(raw: str) -> dict:
    """
    Formato esperado: "Nombre completo, género, edad, última ubicación"
    Tolerante a partes faltantes o separadores extra.
    """
    parts = [p.strip() for p in raw.split(",")]
    result: dict = {
        "full_name": None,
        "gender": None,
        "age": None,
        "last_seen_location": None,
    }

    if len(parts) >= 1 and parts[0]:
        result["full_name"] = parts[0]
    if len(parts) >= 2 and parts[1]:
        result["gender"] = parts[1].lower()
    if len(parts) >= 3:
        m = re.search(r"\d+", parts[2])
        if m:
            result["age"] = int(m.group())
    if len(parts) >= 4:
        result["last_seen_location"] = ", ".join(p for p in parts[3:] if p) or None

    return result


def commit_report(
    db: Session,
    conversation,
    client_phone: str,
    notes: Optional[str] = None,
) -> Optional[Report]:
    """
    Crea Report + Photo(s) a partir del contexto de la conversación.
    Limpia intake_person_raw y pending_photos del contexto tras el commit.
    Devuelve el Report creado, o None si no hay datos suficientes.
    """
    try:
        ctx = json.loads(conversation.context or "{}")
    except Exception:
        ctx = {}

    person_raw: str = ctx.get("intake_person_raw", "")
    pending_photos: list = ctx.get("pending_photos", [])

    if not person_raw and not pending_photos:
        logger.warning(
            "intake: contexto vacío para conv=%d — report no creado", conversation.id
        )
        return None

    parsed = parse_person_data(person_raw) if person_raw else {}

    report = Report(
        kind="missing",
        full_name=parsed.get("full_name"),
        gender=parsed.get("gender"),
        age=parsed.get("age"),
        last_seen_location=parsed.get("last_seen_location"),
        distinguishing_marks=notes or None,
        reporter_wa_hash=hash_phone(client_phone) if client_phone else None,
        consent=True,
        source="whatsapp",
    )
    db.add(report)
    db.flush()

    for photo_data in pending_photos:
        db.add(Photo(
            report_id=report.id,
            media_url=photo_data.get("media_url", ""),
            local_path=photo_data.get("local_path"),
        ))

    ctx.pop("pending_photos", None)
    ctx.pop("intake_person_raw", None)
    conversation.context = json.dumps(ctx)

    db.commit()
    logger.info(
        "intake: Report #%d creado — %r (%d fotos)",
        report.id, parsed.get("full_name", "sin nombre"), len(pending_photos),
    )
    return report
