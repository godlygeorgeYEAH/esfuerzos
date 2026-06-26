"""
Modelos mínimos de Orden y Pago requeridos por orchestrator._handle_comprobante().

El bot SOLO usa estos campos:
  - Orden.id, Orden.total, Orden.negocio_id
  - Pago.id, Pago.orden_id, Pago.metodo, Pago.monto, Pago.comprobante_url, Pago.estado

Si el nuevo repo ya tiene estos modelos (con nombres distintos o campos adicionales),
NO usar este archivo — en cambio, actualizar los imports en orchestrator.py para
apuntar a los modelos del nuevo repo.

Si el nuevo repo no tiene sistema de órdenes, ver PROBLEMÁTICAS.md §2 para
las opciones de desacoplamiento.
"""
from datetime import datetime
from enum import Enum as PyEnum
from sqlalchemy import String, Numeric, DateTime, ForeignKey, Integer, Enum, func, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


class EstadoOrden(str, PyEnum):
    PENDIENTE      = "pendiente"
    CONFIRMADA     = "confirmada"
    EN_PREPARACION = "en_preparacion"
    EN_CAMINO      = "en_camino"
    ENTREGADA      = "entregada"
    LISTA          = "lista"
    RETIRADA       = "retirada"
    RECHAZADO      = "rechazado"
    CANCELADA      = "cancelada"


class EstadoPago(str, PyEnum):
    PENDIENTE  = "pendiente"
    CONFIRMADO = "confirmado"
    RECHAZADO  = "rechazado"


class Orden(Base):
    __tablename__ = "ordenes"

    id: Mapped[int] = mapped_column(primary_key=True)
    negocio_id: Mapped[int] = mapped_column(ForeignKey("negocios.id"), index=True)
    total: Mapped[float] = mapped_column(Numeric(10, 2))
    estado: Mapped[EstadoOrden] = mapped_column(
        Enum(EstadoOrden, name="estadoordentype",
             values_callable=lambda obj: [e.value for e in obj]),
        default=EstadoOrden.PENDIENTE,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relación con Cliente (nullable — ver PROBLEMÁTICAS.md §3)
    cliente_id: Mapped[int | None] = mapped_column(
        ForeignKey("clientes.id"), nullable=True, index=True
    )
    cliente: Mapped["Cliente | None"] = relationship(back_populates="ordenes")

    # Relación con Pago
    pago: Mapped["Pago | None"] = relationship(
        back_populates="orden", uselist=False, cascade="all, delete-orphan"
    )


class Pago(Base):
    __tablename__ = "pagos"

    id: Mapped[int] = mapped_column(primary_key=True)
    orden_id: Mapped[int] = mapped_column(ForeignKey("ordenes.id"), unique=True)
    metodo: Mapped[str] = mapped_column(String(50))
    monto: Mapped[float] = mapped_column(Numeric(10, 2))
    comprobante_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    estado: Mapped[EstadoPago] = mapped_column(
        Enum(EstadoPago, values_callable=lambda obj: [e.value for e in obj]),
        default=EstadoPago.PENDIENTE,
    )
    notas_rechazo: Mapped[str | None] = mapped_column(Text, nullable=True)
    intentos_rechazo: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    orden: Mapped["Orden"] = relationship(back_populates="pago")
