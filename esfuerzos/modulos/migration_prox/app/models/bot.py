"""
Modelos de base de datos del sistema conversacional.

Todos los modelos filtran por negocio_id para garantizar aislamiento multi-tenant.
"""
from sqlalchemy import (
    Column, Integer, String, Boolean, Float, Text,
    DateTime, ForeignKey
)
from sqlalchemy.sql import func
from app.database import Base


# ---------------------------------------------------------------------------
# Configuración del bot por negocio
# ---------------------------------------------------------------------------

class BotConfig(Base):
    """
    Configuración del bot de WhatsApp para un negocio.
    Relación 1:1 con Negocio.
    """
    __tablename__ = "bot_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    negocio_id = Column(Integer, ForeignKey("negocios.id", ondelete="CASCADE"), unique=True, nullable=False, index=True)

    # Mensajes automáticos
    welcome_message = Column(String, nullable=True)
    away_message = Column(String, nullable=True)

    # Horario de operación (varchar(5) en DB: "HH:MM")
    working_hours_start = Column(String(5), nullable=True)
    working_hours_end = Column(String(5), nullable=True)
    working_days = Column(String, nullable=True)        # JSON: ["monday", "tuesday", ...]

    # Info del negocio
    payment_methods = Column(String, nullable=True)     # JSON: ["efectivo", "transferencia", ...]
    datos_pago = Column(Text, nullable=True)            # JSON: {"efectivo": "...", "zelle": "correo@..."}
    area_cobertura = Column(String, nullable=True)      # Texto libre: "Caracas, Chacao"
    delivery_enabled = Column(Boolean, default=True, nullable=False)
    retiro_enabled = Column(Boolean, default=True, nullable=False)

    # Detección de intención por LLM (per-negocio)
    enable_intent_detection = Column(Boolean, default=True, nullable=False)

    # Notificaciones
    notificaciones_ventana_horas = Column(Integer, default=6, nullable=False)

    # Estado del bot
    is_bot_active = Column(Boolean, default=True, nullable=False)

    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<BotConfig(negocio_id={self.negocio_id}, active={self.is_bot_active})>"


# ---------------------------------------------------------------------------
# Clientes bloqueados
# ---------------------------------------------------------------------------

class BlockedClient(Base):
    """Teléfonos de clientes bloqueados por negocio."""
    __tablename__ = "blocked_clients"

    id = Column(Integer, primary_key=True, autoincrement=True)
    negocio_id = Column(Integer, ForeignKey("negocios.id", ondelete="CASCADE"), nullable=False, index=True)
    client_phone = Column(String, nullable=False, index=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    def __repr__(self):
        return f"<BlockedClient(negocio_id={self.negocio_id}, phone={self.client_phone})>"


# ---------------------------------------------------------------------------
# Conversaciones y mensajes
# ---------------------------------------------------------------------------

class Conversacion(Base):
    """
    Hilo conversacional entre un cliente y el bot de un negocio.
    Almacena el estado actual en el flujo y el contexto acumulado (JSON).
    """
    __tablename__ = "conversaciones"

    id = Column(Integer, primary_key=True, autoincrement=True)
    negocio_id = Column(Integer, ForeignKey("negocios.id", ondelete="CASCADE"), nullable=False, index=True)

    # Datos del cliente
    client_phone = Column(String, nullable=False, index=True)
    waha_chat_id = Column(String, nullable=True, index=True)  # chatId WAHA: 584...@c.us o LID@lid
    client_name = Column(String, nullable=True)

    # Estado de la conversación
    status = Column(String, default="active", nullable=False)  # active | converted | abandoned | blocked
    current_node_key = Column(String, nullable=True)

    # Contexto JSON acumulado (intenciones, sentimientos, artículos de interés, etc.)
    context = Column(String, nullable=True)

    # Timestamps
    started_at = Column(DateTime, server_default=func.now(), nullable=False)
    last_message_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    ended_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<Conversacion(id={self.id}, negocio_id={self.negocio_id}, phone={self.client_phone}, status={self.status})>"


class MensajeConversacion(Base):
    """Mensaje individual dentro de una conversación."""
    __tablename__ = "mensajes_conversacion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    conversacion_id = Column(Integer, ForeignKey("conversaciones.id", ondelete="CASCADE"), nullable=False, index=True)

    sender_type = Column(String, nullable=False)    # client | bot | manual
    message_text = Column(String, nullable=True)
    media_url = Column(String, nullable=True)

    # Metadatos de IA
    node_key = Column(String, nullable=True)        # Nodo que generó la respuesta (bot)
    ai_generated = Column(Boolean, default=False, nullable=False)
    ai_confidence = Column(Float, nullable=True)    # 0.00 - 1.00

    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)

    def __repr__(self):
        return f"<MensajeConversacion(id={self.id}, conv={self.conversacion_id}, sender={self.sender_type})>"


class EventoConversacion(Base):
    """
    Evento estructurado del pipeline de conversación para analytics.

    Tipos:
    - intent_detected: Resultado del Intent Detector
    - decision_made: Decisión del Decision Engine
    - response_generated: Método y resultado del Response Generator
    """
    __tablename__ = "eventos_conversacion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    conversacion_id = Column(Integer, ForeignKey("conversaciones.id", ondelete="CASCADE"), nullable=False, index=True)
    negocio_id = Column(Integer, nullable=False, index=True)    # Denormalizado para queries de analytics
    event_type = Column(String, nullable=False, index=True)
    data = Column(Text, nullable=True)                          # JSON con campos del evento
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)

    def __repr__(self):
        return f"<EventoConversacion(id={self.id}, conv={self.conversacion_id}, type={self.event_type})>"


# ---------------------------------------------------------------------------
# Sistema de flujos conversacionales
# ---------------------------------------------------------------------------

class FlowTemplate(Base):
    """
    Plantilla de flujo conversacional.
    El sistema tiene un template por defecto (is_system_default=True)
    compartido por todos los negocios inicialmente.
    """
    __tablename__ = "flow_templates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    is_system_default = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    def __repr__(self):
        return f"<FlowTemplate(id={self.id}, name={self.name}, system_default={self.is_system_default})>"


class FlowNode(Base):
    """
    Nodo individual de un flujo conversacional.
    Cada nodo representa un paso con template, respuestas esperadas y mapa de navegación.
    """
    __tablename__ = "flow_nodes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    flow_template_id = Column(Integer, ForeignKey("flow_templates.id", ondelete="CASCADE"), nullable=False, index=True)

    node_type = Column(String, nullable=False)      # greeting, menu, pricing, etc.
    node_key = Column(String, nullable=False)        # Identificador único: "greeting", "service_list"
    order_position = Column(Integer, nullable=False)

    parameters = Column(String, nullable=True)                  # JSON flexible
    message_template = Column(String, nullable=True)            # Template con {variables}
    expected_responses = Column(String, nullable=True)          # JSON: ["si", "no", "menu"]
    next_node_map = Column(String, nullable=True)               # JSON: {"si": "confirmation"}
    conditions = Column(String, nullable=True)                  # JSON: condiciones de activación

    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    def __repr__(self):
        return f"<FlowNode(id={self.id}, key={self.node_key}, type={self.node_type})>"


class NegocioFlow(Base):
    """
    Asocia un negocio con su FlowTemplate activo.
    Permite customización futura por negocio.
    """
    __tablename__ = "negocio_flows"

    id = Column(Integer, primary_key=True, autoincrement=True)
    negocio_id = Column(Integer, ForeignKey("negocios.id", ondelete="CASCADE"), unique=True, nullable=False, index=True)
    flow_template_id = Column(Integer, ForeignKey("flow_templates.id"), nullable=False, index=True)
    is_active = Column(Boolean, default=True, nullable=False)
    custom_parameters = Column(String, nullable=True)   # JSON: overrides por nodo

    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<NegocioFlow(negocio_id={self.negocio_id}, flow_id={self.flow_template_id})>"


# ---------------------------------------------------------------------------
# Preguntas frecuentes administrables por el operador
# ---------------------------------------------------------------------------

class PreguntaFrecuente(Base):
    """
    Preguntas frecuentes del negocio. El bot consulta esta tabla antes de
    llamar al LLM — si hay match por similitud, responde con el texto fijo
    del operador (costo cero de API, respuesta determinista).
    """
    __tablename__ = "preguntas_frecuentes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    negocio_id = Column(Integer, ForeignKey("negocios.id", ondelete="CASCADE"), nullable=False, index=True)
    pregunta = Column(Text, nullable=False)
    respuesta = Column(Text, nullable=False)
    activa = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    def __repr__(self):
        return f"<PreguntaFrecuente(id={self.id}, negocio_id={self.negocio_id}, activa={self.activa})>"
