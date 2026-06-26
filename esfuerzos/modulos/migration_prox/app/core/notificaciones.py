from typing import Optional
from sqlalchemy.orm import Session

from app.models.notificacion import Notificacion, TipoNotificacion


def crear_notificacion(
    db: Session,
    negocio_id: int,
    tipo: TipoNotificacion,
    titulo: str,
    ruta_destino: str,
    detalle: Optional[str] = None,
    referencia_id: Optional[int] = None,
) -> Notificacion:
    """
    Crea un registro de notificación para el negocio.
    No hace commit — el caller gestiona la transacción.
    """
    notif = Notificacion(
        negocio_id=negocio_id,
        tipo=tipo,
        titulo=titulo,
        detalle=detalle,
        ruta_destino=ruta_destino,
        referencia_id=referencia_id,
    )
    db.add(notif)
    return notif
