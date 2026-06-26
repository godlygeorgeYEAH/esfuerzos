"""
Response Generator - Decide dinámicamente entre template o LLM para generar la respuesta.

Estrategia por tipo de nodo (cuando FORCE_LLM_RESPONSES=False):
  TEMPLATE_NODES → siempre usa templates existentes (FlowEngine)
  LLM_NODES      → siempre genera con DeepSeek
  HYBRID_NODES   → LLM si hay artículo específico detectado, template si no

Cuando FORCE_LLM_RESPONSES=True todos los nodos usan DeepSeek con prompts
específicos por nodo para respuestas más naturales y contextualizadas.

Si el LLM falla, cae silenciosamente al template como fallback.
"""
import json
import logging
from typing import Optional, Tuple

from openai import AsyncOpenAI

from app.bot.dev_logger import dlog
from app.bot.template_renderer import render_articulo_list, render_working_hours, render_payment_methods
from app.models.bot import BotConfig
from app.models.menu import Articulo
from app.models.negocio import Negocio

logger = logging.getLogger(__name__)

# Nodos que siempre usan template (mensajes institucionales, consistencia garantizada)
TEMPLATE_NODES = {
    "greeting", "farewell", "location", "contact_info",
    "payment_info", "confirmation", "closure",
}

# Nodos que siempre usan LLM (requieren flexibilidad total)
LLM_NODES = {"fallback"}

# Resto de nodos son HYBRID: LLM si hay entidad específica, template si no


class ResponseGenerator:
    """
    Generador de respuestas híbrido (template + LLM).

    Selecciona la estrategia óptima para cada nodo y contexto,
    garantizando siempre una respuesta via fallback a template.
    Cuando FORCE_LLM_RESPONSES=True usa prompts específicos por nodo.
    """

    def __init__(self):
        from app.config import get_settings
        settings = get_settings()
        self.client = AsyncOpenAI(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
        )

    async def generate(
        self,
        node,
        negocio_id: int,
        conversation,
        intent_result,
        flow_engine,
    ) -> Tuple[str, str]:
        """
        Genera la respuesta para el nodo destino.

        Args:
            node: FlowNode destino
            negocio_id: ID del negocio
            conversation: Conversación activa
            intent_result: IntentResult del Intent Detector
            flow_engine: Instancia de FlowEngine para fallback a template

        Returns:
            Tupla (respuesta: str, metodo: "template"|"llm")
        """
        from app.config import get_settings
        settings = get_settings()

        node_key = node.node_key
        strategy = self._select_strategy(node_key, intent_result, settings)

        dlog("RESPONSE GENERATOR", "Estrategia seleccionada",
             nodo=node_key,
             estrategia=strategy,
             articulo_detectado=intent_result.entidades.get("servicio_especifico") if intent_result and intent_result.entidades else "none")

        if strategy == "llm":
            response = await self._llm_response(node, negocio_id, conversation, intent_result, flow_engine, settings)
            return response, "llm"

        # template (default y fallback del LLM)
        response = flow_engine._generate_response(node, negocio_id, conversation)
        return response, "template"

    def _select_strategy(self, node_key: str, intent_result, settings) -> str:
        """Determina la estrategia de generación según nodo y entidades."""
        if settings.force_llm_responses:
            dlog("RESPONSE GENERATOR", "FORCE_LLM activo - usando LLM",
                 nodo=node_key)
            return "llm"

        if node_key in TEMPLATE_NODES:
            return "template"

        if node_key in LLM_NODES:
            return "llm"

        # HYBRID: LLM solo si hay artículo específico que personalizar
        if (intent_result and
                intent_result.entidades and
                intent_result.entidades.get("servicio_especifico")):
            return "llm"

        return "template"

    async def _llm_response(
        self,
        node,
        negocio_id: int,
        conversation,
        intent_result,
        flow_engine,
        settings,
    ) -> str:
        """
        Genera respuesta usando DeepSeek con contexto rico y prompts por nodo.

        Fallback a template si el LLM falla o no hay API key.
        """
        if not settings.deepseek_api_key:
            return flow_engine._generate_response(node, negocio_id, conversation)

        try:
            ctx = self._load_context_data(node, negocio_id, conversation, intent_result)
            system_prompt = self._build_system_prompt(ctx["business_name"], node.node_key)
            user_prompt = self._build_node_prompt(node.node_key, ctx)

            dlog("RESPONSE GENERATOR", "Llamando DeepSeek para respuesta",
                 nodo=node.node_key,
                 prompt_preview=user_prompt[:200])

            response = await self.client.chat.completions.create(
                model=settings.deepseek_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.7,
                max_tokens=300,
                timeout=settings.deepseek_timeout,
            )

            content = response.choices[0].message.content.strip()
            tokens = response.usage.total_tokens if response.usage else "?"

            dlog("RESPONSE GENERATOR", "Respuesta LLM recibida",
                 tokens=tokens,
                 respuesta_preview=content[:150])

            return content

        except Exception as e:
            logger.warning(f"ResponseGenerator: LLM falló, usando template. Error: {e}")
            dlog("RESPONSE GENERATOR", "LLM falló — fallback a template",
                 error=str(e))
            return flow_engine._generate_response(node, negocio_id, conversation)

    # ------------------------------------------------------------------
    # Helpers de modalidades de entrega
    # ------------------------------------------------------------------

    def _get_modalidades(self, db, negocio_id: int) -> dict:
        """Lee delivery_enabled / retiro_enabled desde Negocio."""
        default = {"delivery": False, "retiro": False}
        if db is None:
            return default
        try:
            negocio = db.get(Negocio, negocio_id)
            if not negocio:
                return default
            return {
                "delivery": bool(negocio.delivery_enabled),
                "retiro": bool(negocio.retiro_enabled),
            }
        except Exception as e:
            dlog("RESPONSE GENERATOR", "Error cargando modalidades", error=str(e))
            return default

    def _format_modalidades(self, modalidades: dict) -> str:
        labels = {
            "delivery": "Delivery (te lo llevamos)",
            "retiro": "Retiro en local",
        }
        active = [labels[k] for k, v in modalidades.items() if v and k in labels]
        return "\n".join(f"• {m}" for m in active)

    # ------------------------------------------------------------------
    # Carga de datos de contexto
    # ------------------------------------------------------------------

    def _load_context_data(self, node, negocio_id: int, conversation, intent_result) -> dict:
        """
        Carga todos los datos necesarios desde la DB y el contexto de conversación.

        Retorna un dict con todas las variables disponibles para los prompts por nodo.
        """
        db = None
        try:
            from sqlalchemy.orm import object_session
            db = object_session(conversation)
        except Exception:
            pass

        # Valores por defecto
        business_name = "el negocio"
        articulos_info = "No hay artículos disponibles."
        articulos_raw = []
        working_hours_info = "Consultar disponibilidad."
        area_cobertura = ""
        payment_methods_str = ""
        formatted_modalidades = ""
        context = {}

        if db:
            negocio = db.query(Negocio).filter(Negocio.id == negocio_id).first()
            if negocio and negocio.nombre:
                business_name = negocio.nombre

            bot_config = db.query(BotConfig).filter(BotConfig.negocio_id == negocio_id).first()
            if bot_config:
                # Horarios
                try:
                    working_days = (
                        json.loads(bot_config.working_days)
                        if isinstance(bot_config.working_days, str)
                        else (bot_config.working_days or [])
                    )
                    working_hours_info = render_working_hours(
                        bot_config.working_hours_start,
                        bot_config.working_hours_end,
                        working_days,
                    )
                except Exception:
                    pass

                # Área de cobertura
                if bot_config.area_cobertura:
                    area_cobertura = bot_config.area_cobertura

                # Modalidades de entrega
                formatted_modalidades = self._format_modalidades(
                    self._get_modalidades(db, negocio_id)
                )
                dlog("RESPONSE GENERATOR", "Modalidades cargadas",
                     negocio_id=negocio_id,
                     formatted=formatted_modalidades or "[vacío]")

                # Métodos de pago
                metodos_raw = negocio.metodos_pago if negocio and negocio.metodos_pago else None
                if metodos_raw:
                    try:
                        methods = (
                            json.loads(metodos_raw)
                            if isinstance(metodos_raw, str)
                            else metodos_raw
                        )
                        payment_methods_str = "\n".join(f"• {m}" for m in methods) if methods else ""
                    except Exception:
                        payment_methods_str = str(metodos_raw)

            articulos_raw = db.query(Articulo).filter(
                Articulo.negocio_id == negocio_id,
                Articulo.is_active == True,
            ).order_by(Articulo.id).all()
            if articulos_raw:
                articulos_info = render_articulo_list(articulos_raw)

            try:
                context = json.loads(conversation.context) if conversation.context else {}
            except Exception:
                context = {}

        # Mensajes recientes del cliente
        client_messages = context.get("client_messages", [])
        recent_messages = client_messages[-3:] if client_messages else []
        recent_str = "\n".join(f"- {m}" for m in recent_messages) if recent_messages else "Ninguno."
        mensaje_actual = client_messages[-1] if client_messages else ""

        # Datos del intent
        entidades = (intent_result.entidades or {}) if intent_result else {}
        servicio_especifico = entidades.get("servicio_especifico", "")
        horario_mencionado = entidades.get("horario_mencionado", "")
        intencion = intent_result.intencion_principal if intent_result else "unknown"
        sentiment = intent_result.sentiment if intent_result else "neutral"
        urgencia = intent_result.urgencia if intent_result else "low"

        # Datos de contexto de conversación
        selected_service = context.get("selected_service", "")

        # Detalles del artículo (para service_detail)
        # Prioridad: artículo mencionado en la intención actual > artículo seleccionado en contexto
        lookup_term = servicio_especifico or selected_service
        service_details = ""
        if lookup_term and articulos_raw:
            term_lower = lookup_term.lower()
            for a in articulos_raw:
                nombre_lower = a.nombre.lower()
                if term_lower in nombre_lower or nombre_lower in term_lower:
                    service_details = (
                        f"Nombre: {a.nombre}\n"
                        f"Precio: ${a.precio}\n"
                        f"Descripción: {a.descripcion or 'Sin descripción adicional.'}"
                    )
                    break
        if lookup_term and not service_details:
            service_details = f"[ARTÍCULO NO ENCONTRADO: '{lookup_term}']"

        return {
            "business_name": business_name,
            "services_info": articulos_info,
            "working_hours": working_hours_info,
            "service_area": area_cobertura or "Consultar cobertura.",
            "payment_methods": payment_methods_str or "Consultar métodos de pago.",
            "historial_reciente": recent_str,
            "mensaje_actual": mensaje_actual,
            "intencion": intencion,
            "intenciones_secundarias": intent_result.intenciones_secundarias if intent_result else [],
            "sentiment": sentiment,
            "urgencia": urgencia,
            "servicio_especifico": servicio_especifico,
            "horario_mencionado": horario_mencionado,
            "selected_service": selected_service,
            "service_details": service_details,
            "formatted_locations": formatted_modalidades,
            "current_node": node.node_key,
            "node_template": node.message_template or "",
        }

    # ------------------------------------------------------------------
    # System prompt
    # ------------------------------------------------------------------

    def _build_system_prompt(self, bot_name: str, node_key: str) -> str:
        """Prompt de sistema personalizado con nombre del negocio y contexto de nodo."""
        return (
            f"Eres el asistente de WhatsApp de {bot_name}, un negocio gastronómico en Venezuela. "
            "Tono: amigable y profesional, cercano pero respetuoso. "
            f"Contexto conversacional: {node_key}. "
            "Responde SOLO con el mensaje para WhatsApp. "
            "NO inventes precios, horarios ni artículos del menú. "
            "Usa solo la información que te proporciono."
        )

    # ------------------------------------------------------------------
    # Despachador de prompts por nodo
    # ------------------------------------------------------------------

    def _build_node_prompt(self, node_key: str, ctx: dict) -> str:
        """Despacha al prompt específico del nodo."""
        dispatch = {
            # Phase 2
            "bienvenida":           self._prompt_bienvenida,
            "ver_menu":             self._prompt_ver_menu,
            "pedido_recibido":      self._prompt_pedido_recibido,
            "instrucciones_pago":   self._prompt_instrucciones_pago,
            "esperar_comprobante":  self._prompt_esperar_comprobante,
            "comprobante_recibido": self._prompt_comprobante_recibido,
            "orden_confirmada":     self._prompt_orden_confirmada,
            "orden_rechazada":      self._prompt_orden_rechazada,
            "info_negocio":         self._prompt_info_negocio,
            "fallback":             self._prompt_fallback,
            # Legacy
            "greeting":       self._prompt_bienvenida,
            "pricing":        self._prompt_pricing,
            "service_list":   self._prompt_service_list,
            "service_detail": self._prompt_service_detail,
            "availability":   self._prompt_availability,
            "location":       self._prompt_location,
            "contact_info":   self._prompt_contact_info,
            "payment_info":   self._prompt_payment_info,
            "confirmation":   self._prompt_confirmation,
            "farewell":       self._prompt_farewell,
        }
        prompt_fn = dispatch.get(node_key, self._prompt_generic)
        return prompt_fn(ctx)

    # ------------------------------------------------------------------
    # Prompts Phase 2
    # ------------------------------------------------------------------

    def _prompt_bienvenida(self, ctx: dict) -> str:
        return f"""Estás recibiendo a un cliente en un negocio gastronómico venezolano.

Mensaje: "{ctx['mensaje_actual']}"
Sentiment: {ctx['sentiment']}

INSTRUCCIONES:
- Saluda de forma cálida y natural
- Preséntate como asistente de {ctx['business_name']}
- Ofrece exactamente 2 opciones: ver menú / información del negocio
- Si el cliente ya expresó qué quiere, adapta el saludo a eso
- Máximo 4 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_ver_menu(self, ctx: dict) -> str:
        webapp_link = ctx.get("webapp_link", "#")
        return f"""El cliente quiere ver el menú / hacer un pedido.

Menú disponible para referencia:
{ctx['services_info']}

Artículo de interés mencionado: {ctx['servicio_especifico'] or 'Ninguno'}
Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
- Envía el link al menú de forma entusiasta
- Si mencionó un artículo específico, menciónalo: "ahí encontrarás la {ctx['servicio_especifico'] or '...'}"
- Explica brevemente cómo funciona: armar carrito → confirmar → enviar número de orden
- Máximo 4 líneas

Responde SOLO con el mensaje para WhatsApp. Incluye el link: {webapp_link}"""

    def _prompt_pedido_recibido(self, ctx: dict) -> str:
        return f"""El cliente confirmó su pedido desde la webapp.

Número de orden: {ctx.get('orden_numero', ctx.get('servicio_especifico', '—'))}
Métodos de pago disponibles:
{ctx['payment_methods']}

Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
- Celebra la confirmación del pedido con entusiasmo moderado
- Indica el número de orden recibido
- Lista las opciones de pago con bullets (•)
- Pregunta por cuál van a pagar
- Máximo 5 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_instrucciones_pago(self, ctx: dict) -> str:
        metodo = ctx.get('metodo_pago', ctx.get('servicio_especifico', 'el método elegido'))
        return f"""El cliente eligió su método de pago.

Método elegido: {metodo}
Información de pago disponible:
{ctx['payment_methods']}

Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
- Confirma el método elegido
- Si tienes los datos de pago para ese método, proporciónalos claramente
- Si no tienes datos específicos, indica que coordinarán directamente
- Pide que envíe el comprobante (captura o foto) cuando haya pagado
- Tono práctico y claro
- Máximo 5 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_esperar_comprobante(self, ctx: dict) -> str:
        return f"""El cliente debería enviar su comprobante pero mandó texto.

Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
- Recuerda amablemente que necesitas la imagen/captura del comprobante
- No el texto, sino la foto o captura de pantalla del recibo
- Tono paciente y servicial
- Máximo 2 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_comprobante_recibido(self, ctx: dict) -> str:
        return f"""El cliente acaba de enviar su comprobante de pago.

INSTRUCCIONES:
- Confirma que recibiste el comprobante
- Indica que están verificando el pago
- Da un tiempo estimado (unos minutos)
- Agradece por elegir {ctx['business_name']}
- Tono cálido y tranquilizador
- Máximo 3 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_orden_confirmada(self, ctx: dict) -> str:
        return f"""El pago fue verificado y la orden está confirmada.

Número de orden: {ctx.get('selected_service') or ctx.get('orden_numero', '—')}
Negocio: {ctx['business_name']}

INSTRUCCIONES:
- Anuncia la confirmación con entusiasmo
- Menciona que el pedido está en preparación
- Si sabes la modalidad (delivery o retiro), menciónala
- Cierra con agradecimiento cálido
- Máximo 4 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_orden_rechazada(self, ctx: dict) -> str:
        return f"""El comprobante de pago fue rechazado por el operador.

INSTRUCCIONES:
- Comunica el rechazo con tacto (sin culpar al cliente)
- Explica posibles razones (imagen no legible, datos no coinciden)
- Pide que intente nuevamente con un comprobante más claro
- Tono comprensivo y servicial
- Máximo 3 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_info_negocio(self, ctx: dict) -> str:
        return f"""El cliente pregunta por información del negocio (horarios, delivery, pagos).

Información disponible:
- Horarios: {ctx['working_hours']}
- Cobertura: {ctx['service_area']}
- Métodos de pago: {ctx['payment_methods']}

Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
- Presenta la información solicitada de forma clara y organizada
- Usa emojis de categoría (🕐 horario, 📍 ubicación, 💳 pagos)
- Si el cliente preguntó algo específico, destaca eso primero
- Cierra ofreciendo ver el menú
- Máximo 6 líneas

Responde SOLO con el mensaje para WhatsApp."""

    # ------------------------------------------------------------------
    # Prompts legacy (por compatibilidad)
    # ------------------------------------------------------------------

    def _prompt_pricing(self, ctx: dict) -> str:
        tiene_saludo = "greeting" in ctx.get("intenciones_secundarias", [])
        saludo_instruccion = "- INICIA con saludo natural adaptado al tono del cliente\n" if tiene_saludo else ""
        return f"""El cliente pregunta por precios.

Menú disponible:
{ctx['services_info']}

Artículo específico mencionado: {ctx['servicio_especifico'] or 'Ninguno'}
Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
{saludo_instruccion}- Si mencionó artículo específico: responde SOLO sobre ese artículo
- Si pregunta genérica: muestra todos los artículos con precios
- Usa bullets (•) no números
- Menciona cuál es el más popular si aplica
- Incluye precio de forma natural
CTA DINÁMICO — lee el mensaje del cliente y elige:
  • Intención directa (quiero, me interesa el X, voy a pedir): menciona ese artículo y pregunta si confirma el pedido
  • Interés sin decisión: "¿Quieres saber más sobre alguno?"
  • Pregunta genérica: lista todos y pregunta cuál le interesa
- Máximo 6 líneas (incluyendo saludo si aplica)

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_service_list(self, ctx: dict) -> str:
        tiene_saludo = "greeting" in ctx.get("intenciones_secundarias", [])
        saludo_instruccion = "- INICIA con saludo natural adaptado al tono del cliente\n" if tiene_saludo else ""
        return f"""El cliente pregunta por los artículos del menú.

Menú:
{ctx['services_info']}

Mensaje: "{ctx['mensaje_actual']}"
Sentiment: {ctx['sentiment']}

INSTRUCCIONES:
{saludo_instruccion}- Describe cada artículo en 1 línea
- Enfócate en BENEFICIOS / descripción apetitosa
- Usa bullets (•)
- Tono entusiasta pero profesional
CTA DINÁMICO — lee el mensaje del cliente y elige:
  • Intención directa sobre un artículo específico: "¿Te cuento más sobre el X?"
  • Exploratoria: "¿Cuál te llama más la atención?"
- Máximo 7 líneas (incluyendo saludo si aplica)

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_service_detail(self, ctx: dict) -> str:
        tiene_saludo = "greeting" in ctx.get("intenciones_secundarias", [])
        saludo_instruccion = "- INICIA con saludo natural adaptado al tono del cliente\n" if tiene_saludo else ""
        service_details = ctx['service_details']
        not_found = service_details.startswith("[ARTÍCULO NO ENCONTRADO")

        if not_found:
            instrucciones = (
                f"{saludo_instruccion}"
                "- Indica amablemente que no tienes ese artículo disponible\n"
                "- Ofrece mostrar el menú completo\n"
                "- Tono comprensivo, no disculpas excesivas\n"
                "- Máximo 3 líneas"
            )
        else:
            instrucciones = (
                f"{saludo_instruccion}"
                "- Describe SOLO este artículo (no menciones otros)\n"
                "- Enfócate en lo apetitoso y el valor\n"
                "- Menciona precio de forma natural\n"
                "CTA DINÁMICO — lee el mensaje del cliente y elige:\n"
                "  • Si contiene intención directa (quiero, pedir, me lo llevo): usa CTA directo:\n"
                "    \"¿Lo confirmamos para tu pedido?\"\n"
                "  • Si muestra interés pero no decide aún (cuánto, cómo es, háblame de):\n"
                "    usa CTA: \"¿Lo agregamos al pedido?\"\n"
                "  • Si es exploratoria: tono informativo, ofrece más información\n"
                "- Máximo 4 líneas (incluyendo saludo si aplica)"
            )

        return f"""El cliente pregunta por detalles de un artículo del menú.

Artículo mencionado: {ctx['servicio_especifico'] or ctx['selected_service'] or 'No especificado'}
Detalles del artículo:
{service_details}

Mensaje del cliente: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
{instrucciones}
- Termina con pregunta de acción

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_availability(self, ctx: dict) -> str:
        tiene_saludo = "greeting" in ctx.get("intenciones_secundarias", [])
        saludo_instruccion = "- INICIA con saludo natural adaptado al tono del cliente\n" if tiene_saludo else ""

        horario_mencionado = ctx.get("horario_mencionado") or ""
        articulo_para_pedido = ctx.get("servicio_especifico") or ctx.get("selected_service") or ""
        formatted_modalidades = ctx.get("formatted_locations", "")

        alta_intencion = bool(horario_mencionado and articulo_para_pedido)

        if alta_intencion and formatted_modalidades:
            return f"""El cliente quiere hacer un pedido con horario específico.

Artículo seleccionado: {articulo_para_pedido or 'No especificado'}
Horario solicitado: {horario_mencionado or 'No especificado'}
Horarios de atención: {ctx['working_hours']}

Modalidades de entrega disponibles:
{formatted_modalidades}

Mensaje del cliente: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
{saludo_instruccion}- Confirma que hay disponibilidad para el horario solicitado
- Menciona artículo y horario de forma resumida
- INMEDIATAMENTE lista las modalidades disponibles
- Pregunta si prefiere delivery o retiro en local
- TODO en un solo mensaje fluido
- Máximo 5 líneas

Responde SOLO con el mensaje para WhatsApp."""

        elif alta_intencion:
            return f"""El cliente quiere hacer un pedido.

Artículo: {articulo_para_pedido or 'No especificado'}
Horario: {horario_mencionado or 'No especificado'}
Horarios de atención: {ctx['working_hours']}

Mensaje del cliente: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
{saludo_instruccion}- Confirma disponibilidad para el horario
- Indica que pueden coordinar delivery o retiro directamente por este chat
- Pregunta cuál modalidad prefiere
- Máximo 4 líneas

Responde SOLO con el mensaje para WhatsApp."""

        else:
            return f"""El cliente pregunta por disponibilidad de horarios.

Horarios de atención: {ctx['working_hours']}
Día/hora mencionado: {ctx.get('horario_mencionado') or 'No especificado'}
Artículo de interés: {ctx.get('servicio_especifico') or ctx.get('selected_service') or 'No especificado'}
Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
{saludo_instruccion}- Si mencionó día/hora específico: confirma si está disponible
- Si pregunta genérica: menciona días y horarios generales
- Sugiere horarios concretos si aplica
- CTA suave: "¿Hacemos el pedido?"
- Máximo 3 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_location(self, ctx: dict) -> str:
        tiene_saludo = "greeting" in ctx.get("intenciones_secundarias", [])
        saludo_instruccion = "- INICIA con saludo natural adaptado al tono del cliente\n" if tiene_saludo else ""
        formatted_modalidades = ctx.get("formatted_locations", "")
        hay_modalidades = bool(formatted_modalidades)

        if hay_modalidades:
            loc_section = f"Modalidades de entrega disponibles:\n{formatted_modalidades}"
            loc_instrucciones = (
                "- Menciona las modalidades disponibles de forma natural\n"
                "- Si hay múltiples opciones, lista con bullets y pregunta cuál prefiere\n"
                "- Si solo hay una, sé directo"
            )
        else:
            loc_section = f"Área de cobertura: {ctx['service_area']}"
            loc_instrucciones = (
                "- Menciona el área de cobertura si está disponible\n"
                "- Sugiere coordinar directamente por este mismo chat"
            )

        return f"""El cliente pregunta sobre delivery o dónde retirar.

{loc_section}

Mensaje del cliente: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
{saludo_instruccion}{loc_instrucciones}
- Tono amable y servicial
- Máximo 4 líneas (incluyendo saludo si aplica)

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_contact_info(self, ctx: dict) -> str:
        tiene_saludo = "greeting" in ctx.get("intenciones_secundarias", [])
        saludo_instruccion = "- INICIA con saludo natural adaptado al tono del cliente\n" if tiene_saludo else ""
        return f"""El cliente pide formas de contactar o confirmar algo.

Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
{saludo_instruccion}- Confirma que puede coordinar todo por este mismo WhatsApp
- Ofrece ayuda para lo que necesite
- Tono servicial y disponible
- Máximo 3 líneas (incluyendo saludo si aplica)

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_payment_info(self, ctx: dict) -> str:
        tiene_saludo = "greeting" in ctx.get("intenciones_secundarias", [])
        saludo_instruccion = "- INICIA con saludo natural adaptado al tono del cliente\n" if tiene_saludo else ""
        return f"""El cliente pregunta cómo puede pagar.

Métodos de pago:
{ctx['payment_methods']}

Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
{saludo_instruccion}- Lista los métodos de pago disponibles
- Sé claro y directo
- No agregues información innecesaria
- Máximo 3 líneas (incluyendo saludo si aplica)

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_confirmation(self, ctx: dict) -> str:
        tiene_saludo = "greeting" in ctx.get("intenciones_secundarias", [])
        saludo_instruccion = "- INICIA con saludo natural adaptado al tono del cliente\n" if tiene_saludo else ""
        return f"""El cliente está confirmando su pedido.

Artículo: {ctx['selected_service'] or ctx['servicio_especifico'] or 'Por confirmar'}
Horario: {ctx['horario_mencionado'] or 'Por confirmar'}
Mensaje: "{ctx['mensaje_actual']}"

INSTRUCCIONES:
{saludo_instruccion}- Confirma los detalles del pedido (artículo, precio si lo tienes, modalidad de entrega)
- Tono profesional pero cálido
- Pregunta si necesita algo más
- Menciona que pronto recibirá confirmación de pago
- Máximo 4 líneas (incluyendo saludo si aplica)

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_farewell(self, ctx: dict) -> str:
        return f"""El cliente se está despidiendo.

Mensaje: "{ctx['mensaje_actual']}"
Sentiment: {ctx['sentiment']}

INSTRUCCIONES:
- Despídete de forma cordial
- Invita a escribir cuando necesite pedir
- Tono cálido y profesional
- Máximo 2 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_fallback(self, ctx: dict) -> str:
        return f"""No entendiste claramente qué necesita el cliente.

Mensaje: "{ctx['mensaje_actual']}"
Historial: {ctx['historial_reciente']}
Nodo actual: {ctx['current_node']}

INSTRUCCIONES:
- Discúlpate amablemente
- Pide que reformule o sea más específico
- Si hay contexto previo, menciona las opciones del menú relevantes
- Tono amable y paciente
- Máximo 3 líneas

Responde SOLO con el mensaje para WhatsApp."""

    def _prompt_generic(self, ctx: dict) -> str:
        """Fallback para nodos no mapeados — usa el template del nodo como referencia de tono."""
        return f"""Genera una respuesta para este cliente de WhatsApp de un negocio gastronómico.

INFORMACIÓN DEL NEGOCIO:
Nombre: {ctx['business_name']}
Menú disponible:
{ctx['services_info']}
Horarios: {ctx['working_hours']}

NODO ACTUAL: {ctx['current_node']}
TEMPLATE DE REFERENCIA (úsalo como guía de tono y estructura):
{ctx['node_template'] or 'Sin template.'}

ÚLTIMOS MENSAJES DEL CLIENTE:
{ctx['historial_reciente']}

Genera la respuesta ahora. Recuerda: máximo 3 párrafos cortos, termina con pregunta."""
