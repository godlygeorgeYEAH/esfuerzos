from datetime import datetime
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


class Report(Base):
    """Reporte de persona desaparecida o encontrada recibido por WhatsApp."""
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(primary_key=True)
    kind: Mapped[str] = mapped_column(String(10), index=True)          # 'missing' | 'found'

    full_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_seen_location: Mapped[str | None] = mapped_column(Text, nullable=True)
    distinguishing_marks: Mapped[str | None] = mapped_column(Text, nullable=True)
    clothing: Mapped[str | None] = mapped_column(Text, nullable=True)
    person_state: Mapped[str] = mapped_column(String(20), default="unknown")  # alive|injured|deceased|unknown

    reporter_wa_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    consent: Mapped[bool] = mapped_column(Boolean, default=False)
    source: Mapped[str] = mapped_column(String(50), default="whatsapp")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"<Report(id={self.id}, kind={self.kind}, name={self.full_name!r})>"


class Photo(Base):
    """Foto adjunta a un reporte. Una o más por reporte."""
    __tablename__ = "photos"

    id: Mapped[int] = mapped_column(primary_key=True)
    report_id: Mapped[int] = mapped_column(
        ForeignKey("reports.id", ondelete="CASCADE"), nullable=False, index=True
    )
    media_url: Mapped[str] = mapped_column(Text, nullable=False)
    quality_ok: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return f"<Photo(id={self.id}, report_id={self.report_id})>"
