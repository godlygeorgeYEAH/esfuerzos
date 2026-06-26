"""
Motor de flujo conversacional del bot (adaptado para Foob).

Gestiona el estado de la conversación, verifica configuración del bot,
horarios, bloqueos y generación de respuestas por template.

El Orchestrator usa estos métodos como infraestructura base.
"""
import json
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session

from app.models.bot import (
    Conversacion, MensajeConversacion, FlowNode, NegocioFlow,
    BotConfig, BlockedClient,
)
from app.models.negocio import Negocio
from app.models.menu import Articulo
from app.bot.template_renderer import (
    render_template, render_articulo_list, render_working_hours,
    render_payment_methods, add_context_variables,
)


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
        """Verifica si el número de teléfono está bloqueado para el negocio."""
        blocked = self.db.query(BlockedClient).filter(
            BlockedClient.negocio_id == negocio_id,
            BlockedClient.client_phone == client_phone,
        ).first()
        return blocked is not None

    def _get_bot_config(self, negocio_id: int) -> Optional[BotConfig]:
        """Obtiene la configuración del bot para el negocio."""
        return self.db.query(BotConfig).filter(BotConfig.negocio_id == negocio_id).first()

    def _is_within_working_hours(self, bot_config: BotConfig) -> bool:
        """Verifica si el momento actual está dentro del horario de operación."""
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
                    # Horario que cruza medianoche
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
        """Obtiene la conversacion activa o crea una nueva.

        Busca por client_phone (E.164 normalizado, sin '+').
        """
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
        """Obtiene el nodo actual del flujo para la conversación."""
        if not conversation.current_node_key:
            return None

        negocio_flow = self.db.query(NegocioFlow).filter(
            NegocioFlow.negocio_id == conversation.negocio_id,
            NegocioFlow.is_active == True,
        ).first()

        if not negocio_flow:
            return None

        return self.db.query(FlowNode).filter(
            FlowNode.flow_template_id == negocio_flow.flow_template_id,
            FlowNode.node_key == conversation.current_node_key,
        ).first()

    def _get_node_by_key(self, negocio_id: int, node_key: str) -> Optional[FlowNode]:
        """
        Obtiene un nodo por su key.

        Si el negocio no tiene NegocioFlow activo, lo crea automáticamente
        apuntando al FlowTemplate del sistema por defecto.
        """
        negocio_flow = self.db.query(NegocioFlow).filter(
            NegocioFlow.negocio_id == negocio_id,
            NegocioFlow.is_active == True,
        ).first()

        if not negocio_flow:
            from app.bot.flow_seeder import seed_default_flow
            template = seed_default_flow(self.db)
            negocio_flow = NegocioFlow(
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
        """Genera la respuesta de un nodo usando su message_template."""
        if not node.message_template:
            return "..."

        from app.config import get_settings
        settings = get_settings()

        negocio = self.db.query(Negocio).filter(Negocio.id == negocio_id).first()
        bot_config = self._get_bot_config(negocio_id)
        context = self._get_context(conversation)

        variables = {
            'bot_name': negocio.nombre if negocio else "Asistente",
            'business_name': negocio.nombre if negocio else "Nuestro negocio",
            'welcome_message': bot_config.welcome_message if bot_config and bot_config.welcome_message else "",
        }

        # ------------------------------------------------------------------
        # Nodos Phase 2
        # ------------------------------------------------------------------

        if node.node_key == "ver_menu":
            slug = negocio.slug if negocio and hasattr(negocio, 'slug') else str(negocio_id)
            variables['webapp_link'] = f"{settings.webapp_base_url}/menu/{slug}"

        elif node.node_key == "pedido_recibido":
            variables['orden_numero'] = context.get('orden_numero', '—')
            variables['subtotal'] = context.get('subtotal', '—')
            variables['total'] = context.get('total', '—')
            variables['items_list'] = context.get('items_list', '')
            if context.get('modalidad_entrega') == 'delivery':
                variables['delivery_line'] = f"🛵 Delivery: ${context.get('tarifa_delivery', '0.00')}\n"
            else:
                variables['delivery_line'] = ''

        elif node.node_key == "orden_confirmada":
            variables['orden_numero'] = context.get('orden_numero', '—')
            if context.get('modalidad_entrega') == 'retiro':
                variables['aviso_siguiente_paso'] = 'Te avisaremos cuando esté lista para retirar.'
            else:
                variables['aviso_siguiente_paso'] = 'Te avisaremos cuando esté en camino.'

        elif node.node_key == "orden_en_camino":
            variables['orden_numero'] = context.get('orden_numero', '—')
            referencia = context.get('cliente_referencia')
            variables['cliente_referencia_msg'] = (
                f"📍 *Referencia de dirección:* {referencia}\n\n" if referencia else ""
            )

        elif node.node_key == "orden_lista_retiro":
            variables['orden_numero'] = context.get('orden_numero', '—')
            negocio = self.db.get(Negocio, negocio_id)
            if negocio and negocio.negocio_lat and negocio.negocio_lng:
                maps_link = f"https://maps.google.com/?q={negocio.negocio_lat},{negocio.negocio_lng}"
                if negocio.direccion:
                    variables['direccion_negocio'] = f"{negocio.direccion}\n{maps_link}"
                else:
                    variables['direccion_negocio'] = maps_link
            elif negocio and negocio.direccion:
                variables['direccion_negocio'] = negocio.direccion
            else:
                variables['direccion_negocio'] = "consultar con el negocio"

        elif node.node_key == "info_negocio":
            if bot_config:
                try:
                    working_days = (
                        json.loads(bot_config.working_days)
                        if isinstance(bot_config.working_days, str)
                        else (bot_config.working_days or [])
                    )
                    variables['working_hours'] = render_working_hours(
                        bot_config.working_hours_start,
                        bot_config.working_hours_end,
                        working_days,
                    )
                except Exception:
                    variables['working_hours'] = "Consultar horarios"

                if negocio and negocio.negocio_lat and negocio.negocio_lng:
                    maps_link = f"https://maps.google.com/?q={negocio.negocio_lat},{negocio.negocio_lng}"
                    if negocio.direccion:
                        variables['area_cobertura'] = f"{negocio.direccion}\n{maps_link}"
                    else:
                        variables['area_cobertura'] = maps_link
                elif negocio and negocio.direccion:
                    variables['area_cobertura'] = negocio.direccion
                else:
                    variables['area_cobertura'] = "Consultar con el negocio"

                metodos_src = negocio.metodos_pago if negocio and negocio.metodos_pago else None
                try:
                    methods = (
                        json.loads(metodos_src)
                        if isinstance(metodos_src, str)
                        else (metodos_src or [])
                    )
                    variables['payment_methods'] = render_payment_methods(methods)
                except Exception:
                    variables['payment_methods'] = "Consultar"

        # ------------------------------------------------------------------
        # Nodos legacy (por compatibilidad con nodos custom del negocio)
        # ------------------------------------------------------------------

        elif node.node_key == "service_list":
            articulos = self.db.query(Articulo).filter(
                Articulo.negocio_id == negocio_id,
                Articulo.is_active == True,
            ).order_by(Articulo.id).all()
            variables['articulos_list'] = render_articulo_list(articulos)
            variables['services_list'] = variables['articulos_list']

        elif node.node_key == "availability":
            if bot_config:
                try:
                    working_days = (
                        json.loads(bot_config.working_days)
                        if isinstance(bot_config.working_days, str)
                        else (bot_config.working_days or [])
                    )
                    variables['working_hours'] = render_working_hours(
                        bot_config.working_hours_start,
                        bot_config.working_hours_end,
                        working_days,
                    )
                except Exception:
                    variables['working_hours'] = "Consultar disponibilidad"

        elif node.node_key == "location":
            if bot_config:
                variables['area_cobertura'] = bot_config.area_cobertura or "Consultar cobertura"
                modalidades = []
                delivery = bool(negocio.delivery_enabled) if negocio else False
                retiro = bool(negocio.retiro_enabled) if negocio else False
                if delivery:
                    modalidades.append("Delivery")
                if retiro:
                    modalidades.append("Retiro en local")
                variables['modalidades'] = " / ".join(modalidades) if modalidades else "Consultar"
                try:
                    metodos_src = negocio.metodos_pago if negocio and negocio.metodos_pago else None
                    methods = (
                        json.loads(metodos_src)
                        if isinstance(metodos_src, str)
                        else (metodos_src or [])
                    )
                    variables['payment_methods'] = render_payment_methods(methods)
                except Exception:
                    variables['payment_methods'] = "Consultar"

        variables = add_context_variables(variables, context)
        return render_template(node.message_template, variables)

    def _handle_fallback(self, conversation: Conversacion, message_text: str) -> str:
        """Respuesta de fallback cuando no se entiende el mensaje."""
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
        """Guarda un mensaje en la base de datos."""
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
    # Helpers de renderizado
    # ------------------------------------------------------------------

    def _build_payment_method_messages(self, negocio_id: int) -> list:
        """
        Retorna una lista de strings, uno por método de pago activo del negocio.
        Efectivo siempre va último como recordatorio de billete.
        Formato por método: "{icon} *{Método}*\\n{datos}"
        """
        _ICONS = {
            'efectivo': '💵', 'cash': '💵',
            'transferencia': '💳', 'bank_transfer': '💳',
            'pago_movil': '📱', 'pago movil': '📱', 'mobile_payment': '📱',
            'zelle': '🇺🇸', 'paypal': '💸',
            'binance': '₿', 'crypto': '₿',
        }
        _LABELS = {
            'efectivo': 'Efectivo', 'cash': 'Efectivo',
            'transferencia': 'Transferencia', 'bank_transfer': 'Transferencia',
            'pago_movil': 'Pago Móvil', 'pago movil': 'Pago Móvil', 'mobile_payment': 'Pago Móvil',
            'zelle': 'Zelle', 'paypal': 'PayPal',
            'binance': 'Binance', 'crypto': 'Crypto',
        }
        _EFECTIVO_KEYS = {'efectivo', 'cash'}

        negocio = self.db.query(Negocio).filter(Negocio.id == negocio_id).first()
        bot_config = self._get_bot_config(negocio_id)

        metodos_src = negocio.metodos_pago if negocio and negocio.metodos_pago else None
        methods = []
        if metodos_src:
            try:
                methods = json.loads(metodos_src) if isinstance(metodos_src, str) else metodos_src
            except Exception:
                pass

        datos_src = (negocio.datos_pago if negocio and negocio.datos_pago else None) or (
            bot_config.datos_pago if bot_config and getattr(bot_config, 'datos_pago', None) else None
        )
        datos_dict = {}
        if datos_src:
            try:
                datos_dict = json.loads(datos_src) if isinstance(datos_src, str) else datos_src
            except Exception:
                pass

        messages = []
        efectivo_msg = None
        for m in methods:
            key = m.lower()
            if key in _EFECTIVO_KEYS:
                efectivo_msg = "💵 *Efectivo*\nSi quieres pagar con efectivo, ¡mándanos una foto clara del billete!"
                continue
            icon = _ICONS.get(key, '💳')
            label = _LABELS.get(key, m.capitalize())
            datos = (
                datos_dict.get(m)
                or datos_dict.get(key)
                or datos_dict.get(key.replace(' ', '_'))
                or datos_dict.get(key.replace('_', ' '))
            )
            if datos:
                messages.append(f"{icon} *{label}*\n{datos}")
            else:
                messages.append(f"{icon} *{label}*")

        if efectivo_msg:
            messages.append(efectivo_msg)
        return messages

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

