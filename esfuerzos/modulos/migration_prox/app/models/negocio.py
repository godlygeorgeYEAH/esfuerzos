"""
Modelo mínimo de Negocio (tenant base del sistema bot).

Este es un STUB de referencia. El desarrollador del nuevo repo debe:
1. Verificar que su entidad equivalente (Negocio, Tenant, Business, etc.)
   tiene todos los campos que el bot lee (ver comentarios abajo).
2. Ajustar los imports en flow_engine.py y waha_resolver.py según
   el nombre que su entidad tenga.
3. Si los nombres de tabla/campos difieren, actualizar las referencias.

Campos REQUERIDOS por el bot (el nombre del campo puede cambiar, pero la
semántica debe ser la misma):

  Leídos por waha_resolver.py:
    - id                (PK int)
    - is_active         (bool) — solo negocios activos reciben mensajes
    - waha_session      (str | None) — nombre de sesión WAHA; usado en modo
                          multi-tenant para enrutar mensajes al negocio correcto

  Leídos por flow_engine.py → _generate_response():
    - nombre            (str) — {business_name} y {bot_name} en templates
    - slug              (str) — construye el link de la webapp ({webapp_link})
    - metodos_pago      (str JSON) — ej: '["efectivo","zelle","pago_movil"]'
    - datos_pago        (str JSON) — ej: '{"zelle":"correo@x.com"}'
    - delivery_enabled  (bool) — activa/desactiva modalidad delivery
    - retiro_enabled    (bool) — activa/desactiva modalidad retiro en local
    - negocio_lat       (float | None) — GPS del local
    - negocio_lng       (float | None) — GPS del local
    - direccion         (str | None) — dirección textual del local
"""
from datetime import datetime
from sqlalchemy import String, Boolean, DateTime, Text, Numeric, func
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


class Negocio(Base):
    """
    Entidad tenant. Ajustar campos según el modelo real del nuevo repo.
    """
    __tablename__ = "negocios"

    id: Mapped[int] = mapped_column(primary_key=True)
    nombre: Mapped[str] = mapped_column(String(200))
    slug: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    waha_session: Mapped[str | None] = mapped_column(String(100), unique=True, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Métodos de pago (JSON arrays/objects)
    metodos_pago: Mapped[str | None] = mapped_column(Text, nullable=True)
    datos_pago: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Modalidades de entrega
    delivery_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    retiro_enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    # Ubicación física del negocio
    negocio_lat: Mapped[float | None] = mapped_column(Numeric(10, 7), nullable=True)
    negocio_lng: Mapped[float | None] = mapped_column(Numeric(10, 7), nullable=True)
    direccion: Mapped[str | None] = mapped_column(String(500), nullable=True)
