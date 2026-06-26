"""
Modelo de Operacion — entidad raíz del sistema (tenant base).

Representa una coordinación de emergencia, operativo u organización
que despliega el bot Reúne. En modo WAHA_FREE_TIER=True solo existe
una Operacion activa ("default").

Campos REQUERIDOS por el bot:

  Leídos por waha_resolver.py:
    - id                (PK int)
    - is_active         (bool) — solo operaciones activas reciben mensajes
    - waha_session      (str | None) — nombre de sesión WAHA; usado en modo
                          multi-tenant para enrutar mensajes a la operación

  Leídos por flow_engine.py → _generate_response():
    - nombre            (str) — {business_name} y {bot_name} en templates
    - slug              (str) — construye el link de la webapp ({webapp_link})
"""
from datetime import datetime
from sqlalchemy import String, Boolean, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


class Operacion(Base):
    """Coordinación de emergencia — entidad raíz del sistema multi-tenant."""
    __tablename__ = "operaciones"

    id: Mapped[int] = mapped_column(primary_key=True)
    nombre: Mapped[str] = mapped_column(String(200))
    slug: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    waha_session: Mapped[str | None] = mapped_column(String(100), unique=True, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
