"""
Modelos Cliente y ClienteUbicacion.

La relación Cliente.ordenes requiere que el modelo Orden esté importado.
Si el nuevo repo no porta el sistema de órdenes, eliminar esa relación
y su back_populates correspondiente en Orden.
"""
from datetime import datetime
from sqlalchemy import String, DateTime, ForeignKey, Numeric, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


class Cliente(Base):
    __tablename__ = "clientes"
    __table_args__ = (
        UniqueConstraint("negocio_id", "telefono", name="uq_cliente_negocio_telefono"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    negocio_id: Mapped[int] = mapped_column(ForeignKey("negocios.id"), index=True)
    telefono: Mapped[str] = mapped_column(String(20))
    nombre: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    ubicaciones: Mapped[list["ClienteUbicacion"]] = relationship(
        back_populates="cliente", cascade="all, delete-orphan"
    )

    # Relación con Orden — ELIMINAR si no se porta el sistema de órdenes
    # (ver PROBLEMÁTICAS.md §3)
    ordenes: Mapped[list["Orden"]] = relationship(back_populates="cliente")


class ClienteUbicacion(Base):
    __tablename__ = "clientes_ubicaciones"

    id: Mapped[int] = mapped_column(primary_key=True)
    cliente_id: Mapped[int] = mapped_column(
        ForeignKey("clientes.id", ondelete="CASCADE"), index=True
    )
    lat: Mapped[float] = mapped_column(Numeric(10, 7))
    lng: Mapped[float] = mapped_column(Numeric(10, 7))
    referencia: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    cliente: Mapped["Cliente"] = relationship(back_populates="ubicaciones")
