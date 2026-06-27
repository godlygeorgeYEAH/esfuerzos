from datetime import datetime
from sqlalchemy import DateTime, Float, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


class Hospital(Base):
    __tablename__ = "hospitales"

    id: Mapped[int] = mapped_column(primary_key=True)
    wa_chat_id: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    nombre: Mapped[str | None] = mapped_column(String(200), nullable=True)
    ubicacion_texto: Mapped[str | None] = mapped_column(String(500), nullable=True)
    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    lng: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    listas: Mapped[list["HospitalLista"]] = relationship(back_populates="hospital")

    def __repr__(self) -> str:
        return f"<Hospital(id={self.id}, nombre={self.nombre!r})>"


class HospitalLista(Base):
    __tablename__ = "hospital_listas"

    id: Mapped[int] = mapped_column(primary_key=True)
    hospital_id: Mapped[int] = mapped_column(ForeignKey("hospitales.id"), nullable=False, index=True)
    media_url: Mapped[str] = mapped_column(String(500), nullable=False)
    local_path: Mapped[str | None] = mapped_column(String(300), nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    hospital: Mapped["Hospital"] = relationship(back_populates="listas")
