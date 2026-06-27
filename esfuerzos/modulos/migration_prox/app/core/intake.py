"""
Intake — Persiste reporte en Supabase reunion_reports.

Llamado por el Orchestrator al avanzar a reporte_guardado o rescatista_guardado.
Lee context["intake_person_raw"] y context["pending_photos"],
escribe en Supabase y limpia ambas claves del contexto.
"""
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_sb_client = None


def hash_phone(phone: str) -> str:
    return hashlib.sha256(phone.encode()).hexdigest()[:32]


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


def _get_supabase():
    global _sb_client
    if _sb_client is None:
        from app.config import get_settings
        from supabase import create_client
        s = get_settings()
        if not s.supabase_url or not s.supabase_service_role_key:
            return None
        _sb_client = create_client(s.supabase_url, s.supabase_service_role_key)
    return _sb_client


def _upload_photo(photo_bytes: bytes, report_id: str, kind: str, index: int) -> Optional[str]:
    sb = _get_supabase()
    if not sb:
        return None
    bucket = "reunion-photos"
    path = f"{kind}/{report_id}_{index}.jpg"
    try:
        sb.storage.from_(bucket).upload(path, photo_bytes, {"content-type": "image/jpeg"})
        return sb.storage.from_(bucket).get_public_url(path)
    except Exception as exc:
        logger.error("intake: upload foto falló — report=%s idx=%d: %s", report_id, index, exc)
        return None


@dataclass
class ReportResult:
    id: str


def commit_report(
    db: Session,
    conversation,
    client_phone: str,
    notes: Optional[str] = None,
    kind: str = "missing",
) -> Optional[ReportResult]:
    """
    Inserta en Supabase reunion_reports y sube fotos al bucket reunion-photos.
    Limpia intake_person_raw y pending_photos del contexto tras el commit.
    Devuelve ReportResult con el UUID de Supabase, o None si falla.
    """
    try:
        ctx = json.loads(conversation.context or "{}")
    except Exception:
        ctx = {}

    person_raw: str = ctx.get("intake_person_raw", "")
    pending_photos: list = ctx.get("pending_photos", [])

    if not person_raw and not pending_photos:
        logger.warning("intake: contexto vacío para conv=%d — report no creado", conversation.id)
        return None

    parsed = parse_person_data(person_raw) if person_raw else {}

    sb = _get_supabase()
    if not sb:
        logger.warning("intake: Supabase no configurado — report descartado (conv=%d)", conversation.id)
        _clean_context(ctx, conversation, db)
        return None

    row: dict = {
        "kind": kind,
        "reporter_wa_hash": hash_phone(client_phone) if client_phone else None,
        "name": parsed.get("full_name"),
        "age": str(parsed["age"]) if parsed.get("age") is not None else None,
        "location": parsed.get("last_seen_location"),
        "raw_data": {
            "raw_text": person_raw,
            "notes": notes,
            "gender": parsed.get("gender"),
            "photo_count": len(pending_photos),
            "source": "prox_waha",
        },
    }

    if kind == "missing":
        row["marks"] = notes
    else:
        row["found_state"] = "unknown"

    try:
        result = sb.table("reunion_reports").insert(row).execute()
        report_id: str = (result.data or [{}])[0].get("id", "unknown")
    except Exception as exc:
        logger.error("intake: insert Supabase falló (conv=%d): %s", conversation.id, exc)
        return None

    # Subir fotos al bucket; primera foto → photo_url del reporte
    first_photo_url: Optional[str] = None
    for i, photo_data in enumerate(pending_photos):
        local_path = photo_data.get("local_path")
        if not local_path:
            continue
        try:
            with open(local_path, "rb") as f:
                photo_bytes = f.read()
            url = _upload_photo(photo_bytes, report_id, kind, i)
            if url and i == 0:
                first_photo_url = url
        except Exception as exc:
            logger.warning("intake: no se pudo leer foto local %s: %s", local_path, exc)

    if first_photo_url:
        try:
            sb.table("reunion_reports").update({"photo_url": first_photo_url}).eq("id", report_id).execute()
        except Exception as exc:
            logger.warning("intake: no se pudo actualizar photo_url para %s: %s", report_id, exc)

    _clean_context(ctx, conversation, db)

    logger.info(
        "intake: Report %s creado en Supabase — %r kind=%s (%d fotos)",
        report_id[:8], parsed.get("full_name", "sin nombre"), kind, len(pending_photos),
    )
    return ReportResult(id=report_id)


def _clean_context(ctx: dict, conversation, db: Session) -> None:
    ctx.pop("pending_photos", None)
    ctx.pop("intake_person_raw", None)
    conversation.context = json.dumps(ctx)
    db.commit()
