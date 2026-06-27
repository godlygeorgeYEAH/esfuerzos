import logging
from typing import Optional

logger = logging.getLogger(__name__)

_sb_client = None


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


def upsert_hospital(
    wa_chat_id: str,
    nombre: Optional[str],
    ubicacion_texto: Optional[str],
    lat: Optional[float],
    lng: Optional[float],
) -> Optional[str]:
    """
    Crea o actualiza el registro del hospital. Devuelve el UUID o None si falla.
    """
    sb = _get_supabase()
    if not sb:
        logger.warning("hospital_service: Supabase no configurado — upsert omitido")
        return None

    row = {
        "wa_chat_id": wa_chat_id,
        "nombre": nombre,
        "ubicacion_texto": ubicacion_texto,
        "lat": lat,
        "lng": lng,
    }

    try:
        result = sb.table("hospitales").upsert(row, on_conflict="wa_chat_id").execute()
        hospital_id: str = (result.data or [{}])[0].get("id")
        logger.info("hospital_service: hospital upsert — id=%s nombre=%r", hospital_id, nombre)
        return hospital_id
    except Exception as exc:
        logger.error("hospital_service: upsert falló wa_chat_id=%s: %s", wa_chat_id, exc)
        return None


def add_lista(hospital_wa_chat_id: str, media_url: str, photo_url: Optional[str]) -> bool:
    """
    Agrega una foto de lista de ingresos al hospital. Devuelve True si tuvo éxito.
    """
    sb = _get_supabase()
    if not sb:
        return False

    try:
        hospital = sb.table("hospitales").select("id").eq("wa_chat_id", hospital_wa_chat_id).single().execute()
        hospital_id = (hospital.data or {}).get("id")
        if not hospital_id:
            logger.warning("hospital_service: hospital no encontrado para wa_chat_id=%s", hospital_wa_chat_id)
            return False

        sb.table("hospital_listas").insert({
            "hospital_id": hospital_id,
            "media_url": media_url,
            "photo_url": photo_url,
        }).execute()
        logger.info("hospital_service: lista agregada — hospital_id=%s", hospital_id)
        return True
    except Exception as exc:
        logger.error("hospital_service: add_lista falló: %s", exc)
        return False
