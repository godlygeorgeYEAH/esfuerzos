import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_NODE_TO_USER_TYPE: dict[str, str] = {
    "guia_familiar": "familiar",
    "guia_rescatista": "rescatista",
    "guia_hospital": "hospital",
}

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


def upsert_cliente(db: Session, wa_chat_id: str, phone: str) -> None:
    """Registra la primera visita o actualiza last_seen_at en Supabase."""
    sb = _get_supabase()
    if not sb:
        logger.warning("cliente_service: Supabase no configurado — upsert omitido")
        return

    now = datetime.now(timezone.utc).isoformat()
    try:
        sb.table("clientes").upsert(
            {"wa_chat_id": wa_chat_id, "phone": phone, "last_seen_at": now},
            on_conflict="wa_chat_id",
        ).execute()
    except Exception as exc:
        logger.error("cliente_service: upsert falló wa_chat_id=%s: %s", wa_chat_id, exc)


def set_user_type(db: Session, wa_chat_id: str, next_node_key: str) -> None:
    """Persiste el tipo de usuario al salir del nodo bienvenida."""
    user_type = _NODE_TO_USER_TYPE.get(next_node_key)
    if not user_type:
        return

    sb = _get_supabase()
    if not sb:
        return

    try:
        sb.table("clientes").update({"user_type": user_type}).eq("wa_chat_id", wa_chat_id).execute()
        logger.info("cliente_service: %s → user_type=%s", wa_chat_id, user_type)
    except Exception as exc:
        logger.error("cliente_service: set_user_type falló wa_chat_id=%s: %s", wa_chat_id, exc)
