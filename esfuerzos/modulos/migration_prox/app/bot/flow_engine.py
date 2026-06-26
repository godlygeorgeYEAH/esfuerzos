"""
Motor de flujo conversacional del bot.

Gestiona el estado de la conversación, verifica configuración del bot,
horarios, bloqueos y generación de respuestas por template.

El Orchestrator usa estos métodos como infraestructura base.
"""
import json
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session

from app.models.bot import (
    Conversacion, MensajeConversacion, FlowNode, OperacionFlow,
    BotConfig, BlockedClient,
)
from app.models.negocio import Operacion
from app.bot.template_renderer import render_template, add_context_variables


class FlowEngine:
    """
    Motor de infraestructura del flujo conversacional.

    Responsable de:
    - Verificaciones (cliente bloqueado, bot activo, horario)
    - Obtener/crear conversaciones
    - Guardar mensajes
    - Navegar entre nodos
    - Generar respuestas a partir de templates
    """

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Verificaciones
    # ------------------------------------------------------------------

    def _is_client_blocked(self, negocio_id: int, client_phone: str) -> bool:
        blocked = self.db.query(BlockedClient).filter(
            BlockedClient.negocio_id == negocio_id,
            BlockedClient.client_phone == client_phone,
        ).first()
        return blocked is not None

    def _get_bot_config(self, negocio_id: int) -> Optional[BotConfig]:
        return self.db.query(BotConfig).filter(BotConfig.negocio_id == negocio_id).first()

    def _is_within_working_hours(self, bot_config: BotConfig) -> bool:
        now = datetime.now()
        current_time = now.time()
        current_day = now.strftime('%A').lower()

        if bot_config.working_days:
            try:
                working_days = (
                    json.loads(bot_config.working_days)
                    if isinstance(bot_config.working_days, str)
                    else bot_config.working_days
                )
                if current_day not in working_days:
                    return False
            except Exception:
                pass

        if bot_config.working_hours_start and bot_config.working_hours_end:
            try:
                start_time = (
                    datetime.strptime(bot_config.working_hours_start, "%H:%M").time()
                    if isinstance(bot_config.working_hours_start, str)
                    else bot_config.working_hours_start
                )
                end_time = (
                    datetime.strptime(bot_config.working_hours_end, "%H:%M").time()
                    if isinstance(bot_config.working_hours_end, str)
                    else bot_config.working_hours_end
                )
                if start_time <= end_time:
                    if not (start_time <= current_time <= end_time):
                        return False
                else:
                    if not (current_time >= start_time or current_time <= end_time):
                        return False
            except Exception:
                pass

        return True

    # ------------------------------------------------------------------
    # Conversaciones
    # ------------------------------------------------------------------

    def _get_or_create_conversation(
        self, negocio_id: int, client_phone: str, waha_chat_id: Optional[str] = None
    ) -> Conversacion:
        conversation = self.db.query(Conversacion).filter(
            Conversacion.negocio_id == negocio_id,
            Conversacion.client_phone == client_phone,
            Conversacion.status == "active",
        ).order_by(Conversacion.last_message_at.desc()).first()

        if conversation:
            if waha_chat_id and not conversation.waha_chat_id:
                conversation.waha_chat_id = waha_chat_id
                self.db.commit()
            return conversation

        conversation = Conversacion(
            negocio_id=negocio_id,
            client_phone=client_phone,
            waha_chat_id=waha_chat_id,
            status="active",
            current_node_key=None,
            context="{}",
            last_message_at=datetime.utcnow(),
        )
        self.db.add(conversation)
        self.db.commit()
        self.db.refresh(conversation)
        return conversation

    # ------------------------------------------------------------------
    # Nodos
    # ------------------------------------------------------------------

    def _get_current_node(self, conversation: Conversacion) -> Optional[FlowNode]:
        if not conversation.current_node_key:
            return None

        negocio_flow = self.db.query(OperacionFlow).filter(
            OperacionFlow.negocio_id == conversation.negocio_id,
            OperacionFlow.is_active == True,
        ).first()

        if not negocio_flow:
            return None

        return self.db.query(FlowNode).filter(
            FlowNode.flow_template_id == negocio_flow.flow_template_id,
            FlowNode.node_key == conversation.current_node_key,
        ).first()

    def _get_node_by_key(self, negocio_id: int, node_key: str) -> Optional[FlowNode]:
        negocio_flow = self.db.query(OperacionFlow).filter(
            OperacionFlow.negocio_id == negocio_id,
            OperacionFlow.is_active == True,
        ).first()

        if not negocio_flow:
            from app.bot.flow_seeder import seed_default_flow
            template = seed_default_flow(self.db)
            negocio_flow = OperacionFlow(
                negocio_id=negocio_id,
                flow_template_id=template.id,
                is_active=True,
            )
            self.db.add(negocio_flow)
            self.db.commit()
            self.db.refresh(negocio_flow)

        return self.db.query(FlowNode).filter(
            FlowNode.flow_template_id == negocio_flow.flow_template_id,
            FlowNode.node_key == node_key,
        ).first()

    # ------------------------------------------------------------------
    # Respuestas por template
    # ------------------------------------------------------------------

    def _generate_response(self, node: FlowNode, negocio_id: int, conversation: Conversacion) -> str:
        if not node.message_template:
            return "..."

        negocio = self.db.query(Operacion).filter(Operacion.id == negocio_id).first()
        bot_config = self._get_bot_config(negocio_id)
        context = self._get_context(conversation)

        variables = {
            'bot_name': negocio.nombre if negocio else "Asistente",
            'business_name': negocio.nombre if negocio else "Nuestro servicio",
            'welcome_message': bot_config.welcome_message if bot_config and bot_config.welcome_message else "",
        }

        variables = add_context_variables(variables, context)
        return render_template(node.message_template, variables)

    def _handle_fallback(self, conversation: Conversacion, message_text: str) -> str:
        fallback_node = self._get_node_by_key(conversation.negocio_id, "fallback")
        if fallback_node and fallback_node.message_template:
            return self._generate_response(fallback_node, conversation.negocio_id, conversation)
        return "Disculpa, no entendí tu mensaje. ¿Podrías reformularlo?"

    # ------------------------------------------------------------------
    # Mensajes
    # ------------------------------------------------------------------

    def _save_message(
        self,
        negocio_id: int,
        client_phone: str,
        sender_type: str,
        content: str,
        conversacion_id: Optional[int] = None,
        save_conversation: bool = True,
    ):
        if not save_conversation and not conversacion_id:
            return

        if not conversacion_id and save_conversation:
            conversation = self._get_or_create_conversation(negocio_id, client_phone)
            conversacion_id = conversation.id

        message = MensajeConversacion(
            conversacion_id=conversacion_id,
            sender_type=sender_type,
            message_text=content,
        )
        self.db.add(message)
        self.db.commit()

    # ------------------------------------------------------------------
    # Contexto
    # ------------------------------------------------------------------

    def _get_context(self, conversation: Conversacion) -> Dict[str, Any]:
        if not conversation.context:
            return {}
        try:
            if isinstance(conversation.context, str):
                return json.loads(conversation.context)
            return conversation.context
        except Exception:
            return {}
