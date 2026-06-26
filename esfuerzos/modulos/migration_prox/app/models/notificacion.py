from datetime import datetime
from enum import Enum as PyEnum
from sqlalchemy import String, DateTime, ForeignKey, Integer, Enum, func
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


class TipoNotificacion(str, PyEnum):
    ORDEN_CREADA          = "orden_creada"
    COMPROBANTE_RECIBIDO  = "comprobante_recibido"
    ORDEN_ENTREGADA       = "orden_entregada"
    CONVERSACION_ESCALADA = "conversacion_escalada"
    CONDUCTOR_ACEPTO      = "conductor_acepto"
    CONDUCTOR_RECHAZO     = "conductor_rechazo"
    CONDUCTOR_SIN_RESPUESTA = "conductor_sin_respuesta"


class Notificacion(Base):
    __tablename__ = "notificaciones"

    id:            Mapped[int]           = mapped_column(primary_key=True)
    negocio_id:    Mapped[int]           = mapped_column(ForeignKey("negocios.id"), index=True)
    tipo:          Mapped[TipoNotificacion] = mapped_column(
        Enum(TipoNotificacion, values_callable=lambda obj: [e.value for e in obj])
    )
    titulo:        Mapped[str]           = mapped_column(String(200))
    detalle:       Mapped[str | None]    = mapped_column(String(500), nullable=True)
    ruta_destino:  Mapped[str]           = mapped_column(String(300))
    referencia_id: Mapped[int | None]    = mapped_column(Integer, nullable=True)
    created_at:    Mapped[datetime]      = mapped_column(DateTime(timezone=True), server_default=func.now())
