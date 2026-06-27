from datetime import datetime
from sqlalchemy import Boolean, DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


class Cliente(Base):
    """Persona que ha interactuado con el bot al menos una vez."""
    __tablename__ = "clientes"

    id: Mapped[int] = mapped_column(primary_key=True)
    wa_chat_id: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    phone: Mapped[str] = mapped_column(String(30), index=True, nullable=False)
    user_type: Mapped[str | None] = mapped_column(String(20), nullable=True)  # familiar | rescatista | hospital
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<Cliente(id={self.id}, phone={self.phone!r}, type={self.user_type!r})>"
