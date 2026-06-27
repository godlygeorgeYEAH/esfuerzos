import logging
from datetime import datetime
from sqlalchemy.orm import Session

from app.models.cliente import Cliente

logger = logging.getLogger(__name__)

_NODE_TO_USER_TYPE: dict[str, str] = {
    "guia_familiar": "familiar",
    "guia_rescatista": "rescatista",
    "guia_hospital": "hospital",
}


def upsert_cliente(db: Session, wa_chat_id: str, phone: str) -> Cliente:
    """Registra la primera visita o actualiza last_seen_at. Idempotente."""
    cliente = db.query(Cliente).filter(Cliente.wa_chat_id == wa_chat_id).first()
    if cliente:
        cliente.last_seen_at = datetime.utcnow()
        db.commit()
        return cliente

    cliente = Cliente(wa_chat_id=wa_chat_id, phone=phone)
    db.add(cliente)
    db.commit()
    db.refresh(cliente)
    logger.info("Cliente registrado: wa_chat_id=%s phone=%s", wa_chat_id, phone)
    return cliente


def set_user_type(db: Session, wa_chat_id: str, next_node_key: str) -> None:
    """Persiste el tipo de usuario al salir del nodo bienvenida."""
    user_type = _NODE_TO_USER_TYPE.get(next_node_key)
    if not user_type:
        return

    cliente = db.query(Cliente).filter(Cliente.wa_chat_id == wa_chat_id).first()
    if not cliente:
        return

    if cliente.user_type != user_type:
        cliente.user_type = user_type
        db.commit()
        logger.info("Cliente %s → user_type=%s", wa_chat_id, user_type)
