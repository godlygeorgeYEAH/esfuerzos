from datetime import datetime
from enum import Enum as PyEnum
from sqlalchemy import String, Boolean, DateTime, ForeignKey, Enum, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


class EstadoConductor(str, PyEnum):
    PENDIENTE = "pendiente"
    ACEPTADA = "aceptada"
    RECHAZADA = "rechazada"


class Conductor(Base):
    __tablename__ = "conductores"
    __table_args__ = (
        UniqueConstraint("negocio_id", "telefono", name="uq_conductor_negocio_telefono"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    negocio_id: Mapped[int] = mapped_column(ForeignKey("negocios.id"), index=True)
    nombre: Mapped[str] = mapped_column(String(200))
    telefono: Mapped[str] = mapped_column(String(20))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relaciones
    negocio: Mapped["Negocio"] = relationship(back_populates="conductores")
    ordenes: Mapped[list["Orden"]] = relationship(back_populates="conductor")
