"""
Orchestrator v2.0 - Coordinador principal del sistema de mensajería inteligente.

Pipeline de procesamiento:
  1.  Cliente bloqueado            → FlowEngine
  2.  Bot activo                   → FlowEngine
  3.  Horario de trabajo           → FlowEngine
  4.  Obtener/crear conversación   → FlowEngine
  5.  Guardar mensaje del cliente  → FlowEngine
  6.  Obtener nodo actual          → FlowEngine
      6b. Comprobante (media en esperar_comprobante) → cortocircuito directo
  7.  Intent Detection + Similarity en paralelo (asyncio.gather)
  8.  Decision Engine              → navegación por reglas
  9.  Obtener nodo destino         → FlowEngine
  10. Context Manager + entidades especiales (orden_numero)
  11. Response Generator           → template o LLM
  12. ABC Layer                    → micro-CTA de conversión
  13. Analytics Logger             → registra eventos en DB
  14. Guardar mensaje del bot      → MensajeConversacion con metadatos AI

Dev mode: activar DEV_FLOW_LOG=True en .env para ver cada paso en consola.
"""
import asyncio
import json
import logging
import re
import time
from datetime import datetime
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from app.bot.flow_engine import FlowEngine
from app.bot.intent_detector import IntentDetector, IntentResult
from app.bot.decision_engine import DecisionEngine, SimilarityResult
from app.bot.context_manager import ContextManager
from app.bot.response_generator import ResponseGenerator
from app.bot.abc_layer import ABCLayer
from app.bot.analytics_logger import AnalyticsLogger
from app.bot.message_parser import match_expected_response
from app.bot.dev_logger import dlog
from app.bot.faq_matcher import match_faq
from app.models.bot import Conversacion, MensajeConversacion

logger = logging.getLogger(__name__)

# Nodo de entrada del flujo Phase 2
ENTRY_NODE = "bienvenida"


class Orchestrator:
    """
    Coordinador v2.0 que extiende FlowEngine con inteligencia LLM.

    El FlowEngine sigue siendo responsable de toda la infraestructura
    (verificaciones, DB, templates). El Orchestrator agrega:
    - Intent Detection (DeepSeek)
    - Context Manager enriquecido
    - Response Generator híbrido (template + LLM)
    - ABC Layer (CTAs de conversión)
    - Analytics Logger (eventos en DB)
    """

    def __init__(self, db: Session):
        self.db = db
        self.engine = FlowEngine(db)
        self.intent_detector = IntentDetector()
        self.decision_engine = DecisionEngine()
        self.context_manager = ContextManager()
        self.response_generator = ResponseGenerator()
        self.abc_layer = ABCLayer()
        self.analytics_logger = AnalyticsLogger(db)

    async def process_message(
        self,
        negocio_id: int,
        client_phone: str,
        message_text: str,
        media_url: Optional[str] = None,
        waha_chat_id: Optional[str] = None,
    ) -> Tuple[str, bool]:
        """
        Procesa un mensaje con el pipeline v2.0 completo.

        Args:
            negocio_id: ID del negocio
            client_phone: Número de teléfono del cliente
            message_text: Texto del mensaje (puede ser vacío si hay media)
            media_url: URL del comprobante u otro archivo adjunto (opcional)
            waha_chat_id: chatId completo de WAHA (ej: 24700877054119@lid), para lookup prioritario

        Returns:
            Tupla (respuesta: str, should_send: bool)
        """
        dlog("ORCHESTRATOR", "INICIO",
             negocio_id=negocio_id,
             phone=client_phone,
             waha_chat_id=waha_chat_id,
             mensaje=message_text,
             media=bool(media_url))

        # --- Paso 1: Cliente bloqueado ---
        is_blocked = self.engine._is_client_blocked(negocio_id, client_phone)
        dlog("ORCHESTRATOR", "Paso 1: Cliente bloqueado",
             bloqueado=is_blocked,
             resultado="IGNORADO" if is_blocked else "OK -> continuar")
        if is_blocked:
            return "", False

        # --- Paso 2: Bot activo ---
        bot_config = self.engine._get_bot_config(negocio_id)
        bot_activo = bool(bot_config and bot_config.is_bot_active)
        dlog("ORCHESTRATOR", "Paso 2: Bot activo",
             is_bot_active=bot_activo,
             resultado="ACTIVO -> continuar" if bot_activo else "INACTIVO -> ignorado")
        if not bot_activo:
            return "", False

        # --- Paso 3: Horario de trabajo ---
        dentro_horario = self.engine._is_within_working_hours(bot_config)
        dlog("ORCHESTRATOR", "Paso 3: Horario de trabajo",
             dentro_horario=dentro_horario,
             resultado="DENTRO -> continuar" if dentro_horario else "FUERA -> away_message")
        if not dentro_horario:
            away_message = (
                bot_config.away_message
                or "Gracias por tu mensaje. En este momento no estamos disponibles. Te respondemos pronto."
            )
            self.engine._save_message(
                negocio_id, client_phone, "client", message_text, save_conversation=False
            )
            return away_message, True

        # --- Paso 4: Obtener o crear conversación ---
        conversation = self.engine._get_or_create_conversation(negocio_id, client_phone, waha_chat_id=waha_chat_id)
        dlog("ORCHESTRATOR", "Paso 4: Conversación",
             conversation_id=conversation.id,
             nodo_actual=conversation.current_node_key or "None (nueva)")

        # --- Paso 5: Guardar mensaje del cliente ---
        self.engine._save_message(
            negocio_id, client_phone, "client", message_text,
            conversacion_id=conversation.id,
        )
        dlog("ORCHESTRATOR", "Paso 5: Mensaje cliente guardado",
             sender="client",
             texto=message_text,
             media_url=media_url or "—")

        # Conversación escalada a humano: mensaje guardado, bot no responde
        if conversation.status == "escalated":
            dlog("ORCHESTRATOR", "Conversación escalada — bot silenciado")
            return "", False

        # --- Paso 6: Obtener nodo actual ---
        current_node = self.engine._get_current_node(conversation)

        # Primera interacción: establecer nodo de entrada
        if not current_node:
            current_node = self.engine._get_node_by_key(negocio_id, ENTRY_NODE)
            if current_node:
                conversation.current_node_key = ENTRY_NODE
                dlog("ORCHESTRATOR", "Paso 6: Primera interacción",
                     nodo_inicial=ENTRY_NODE)

        # Sin flujo configurado
        if not current_node:
            dlog("ORCHESTRATOR", "Paso 6: ERROR - Flujo no configurado")
            response = "Lo siento, estamos experimentando problemas técnicos. Por favor intenta más tarde."
            self._save_bot_message(conversation, response, None, ai_generated=False, ai_confidence=None)
            return response, True

        dlog("ORCHESTRATOR", "Paso 6: Nodo actual",
             node_key=current_node.node_key,
             expected_responses=current_node.expected_responses or "ninguna")

        # --- Paso 6b: Cortocircuito para comprobante ---
        if current_node.node_key == "esperar_comprobante" and media_url:
            return await self._handle_comprobante(negocio_id, conversation, media_url, message_text)

        # --- Paso 6c: Texto en esperar_comprobante → detectar problema de pago ---
        if current_node.node_key == "esperar_comprobante" and message_text:
            return await self._handle_texto_en_espera_comprobante(negocio_id, conversation, message_text)

        # Si no hay texto y tampoco es comprobante, ignorar
        if not message_text:
            dlog("ORCHESTRATOR", "Sin texto y sin media relevante — ignorado")
            return "", False

        # --- Paso 6d: Cortocircuito CTA ---
        cta_shortcircuit_node: str | None = None
        _ctx_cta = self.context_manager.get(conversation)
        _cta_pending = _ctx_cta.get("cta_pending")
        if _cta_pending:
            _ctx_cta.pop("cta_pending")
            conversation.context = json.dumps(_ctx_cta)
            _msg_norm = message_text.strip().lower()
            if _msg_norm in [r.lower() for r in _cta_pending.get("respuestas_esperadas", [])]:
                cta_shortcircuit_node = _cta_pending["nodo_destino"]
        dlog("ORCHESTRATOR", "Paso 6d: CTA pending",
             pendiente=bool(_cta_pending), match=bool(cta_shortcircuit_node),
             nodo_destino=cta_shortcircuit_node or "—")

        # --- Paso 6e: FAQ Match — cortocircuito sin LLM ---
        if not cta_shortcircuit_node:
            faq_respuesta = match_faq(self.db, negocio_id, message_text)
            if faq_respuesta:
                dlog("ORCHESTRATOR", "Paso 6e: FAQ match — cortocircuito",
                     respuesta=faq_respuesta[:60])
                self._save_bot_message(conversation, faq_respuesta, None, ai_generated=False, ai_confidence=None)
                return faq_respuesta, True
            dlog("ORCHESTRATOR", "Paso 6e: FAQ — sin match, continúa pipeline")

        # --- Paso 7: Intent Detection + Similarity Matching en paralelo ---
        if cta_shortcircuit_node:
            context = _ctx_cta
            intent_result = IntentResult()
            similarity_result = SimilarityResult()
            intent_latency_ms = 0
            target_node_key = cta_shortcircuit_node
            razon = "cta_match"
            metodo = "cta"
            dlog("ORCHESTRATOR", "Pasos 7-8: Saltados — cortocircuito CTA",
                 nodo_destino=target_node_key)
        else:
            context = self.context_manager.get(conversation)
            dlog("ORCHESTRATOR", "Paso 7: Lanzando análisis en paralelo",
                 contexto_keys=list(context.keys()) if context else "vacío",
                 modo="Intent Detector (DeepSeek) || Similarity Matcher")

            intent_start = time.monotonic()

            # Usar enable_intent_detection por negocio (BotConfig), fallback False
            intent_enabled = bool(bot_config and getattr(bot_config, 'enable_intent_detection', False))
            try:
                _expected = json.loads(current_node.expected_responses) if current_node.expected_responses else None
            except Exception:
                _expected = None

            intent_coro = self.intent_detector.detectar_intencion(
                mensaje=message_text,
                contexto=context,
                current_node=current_node.node_key,
                expected_responses=_expected,
                enabled=intent_enabled,
            )
            similarity_coro = self._run_similarity_matching(current_node, message_text, conversation)

            results = await asyncio.gather(intent_coro, similarity_coro, return_exceptions=True)
            intent_latency_ms = int((time.monotonic() - intent_start) * 1000)

            intent_result: IntentResult = (
                results[0] if not isinstance(results[0], Exception) else IntentResult()
            )
            similarity_result: SimilarityResult = (
                results[1] if not isinstance(results[1], Exception) else SimilarityResult()
            )

            if isinstance(results[0], Exception):
                logger.error(f"Orchestrator: intent detection falló: {results[0]}")
            if isinstance(results[1], Exception):
                logger.error(f"Orchestrator: similarity matching falló: {results[1]}")

            dlog("ORCHESTRATOR", "Paso 7: Resultados del análisis",
                 intent=f"{intent_result.intencion_principal} (conf={intent_result.confidence:.2f})",
                 similarity=f"{similarity_result.match} (conf={similarity_result.confidence:.2f})",
                 entidades=intent_result.entidades,
                 latency_ms=intent_latency_ms)

            # --- Paso 8: Decision Engine ---
            target_node_key, razon, metodo = self.decision_engine.decidir_navegacion(
                intent=intent_result,
                similarity=similarity_result,
                current_node=current_node.node_key,
                contexto=context,
            )
            dlog("ORCHESTRATOR", "Paso 8: Decision Engine",
                 nodo_origen=current_node.node_key,
                 nodo_destino=target_node_key,
                 razon=razon,
                 metodo=metodo)

            # --- Fallback ---
            if target_node_key == "fallback":
                dlog("ORCHESTRATOR", "Fallback activado", razon=razon)
                response = self.engine._handle_fallback(conversation, message_text)
                self.context_manager.update(conversation, intent_result, message_text)
                self.analytics_logger.log_intent(conversation.id, negocio_id, intent_result, intent_latency_ms)
                self.analytics_logger.log_decision(
                    conversation.id, negocio_id,
                    current_node.node_key, "fallback",
                    razon, metodo,
                    intent_result.confidence, similarity_result.confidence,
                )
                self._save_bot_message(conversation, response, "fallback", ai_generated=False, ai_confidence=None)
                return response, True

        # --- Paso 9: Obtener nodo destino ---
        next_node = self.engine._get_node_by_key(negocio_id, target_node_key)
        if not next_node:
            logger.warning(f"Orchestrator: nodo '{target_node_key}' no encontrado, usando fallback")
            response = self.engine._handle_fallback(conversation, message_text)
            self._save_bot_message(conversation, response, "fallback", ai_generated=False, ai_confidence=None)
            return response, True

        dlog("ORCHESTRATOR", "Paso 9: Nodo destino encontrado", node_key=next_node.node_key)

        # --- Paso 10: Context Manager + entidades especiales ---
        conversation.current_node_key = next_node.node_key
        conversation.last_message_at = datetime.utcnow()
        context = self.context_manager.update(conversation, intent_result, message_text)

        # Persistir entidades específicas del flujo Phase 2
        self._persist_flow_entities(conversation, next_node.node_key, intent_result, message_text)

        dlog("ORCHESTRATOR", "Paso 10: Context Manager",
             sentiment=context.get("sentiment_general", "neutral"))

        # --- Paso 11: Response Generator ---
        response, response_metodo = await self.response_generator.generate(
            node=next_node,
            negocio_id=negocio_id,
            conversation=conversation,
            intent_result=intent_result,
            flow_engine=self.engine,
        )
        dlog("ORCHESTRATOR", "Paso 11: Response Generator",
             metodo=response_metodo,
             respuesta_preview=response[:120])

        # --- Paso 12: ABC Layer ---
        response_pre_abc = response
        response, cta_obj = self.abc_layer.apply(response, next_node.node_key, context)
        abc_applied = response != response_pre_abc
        if cta_obj and cta_obj.get("nodo_destino"):
            context["cta_pending"] = {
                "respuestas_esperadas": cta_obj["respuestas_esperadas"],
                "nodo_destino": cta_obj["nodo_destino"],
            }
            conversation.context = json.dumps(context)
        dlog("ORCHESTRATOR", "Paso 12: ABC Layer",
             aplicado=abc_applied,
             cta_pending=bool(cta_obj),
             respuesta_final=response[:120])

        # --- Paso 13: Analytics Logger ---
        self.analytics_logger.log_intent(conversation.id, negocio_id, intent_result, intent_latency_ms)
        self.analytics_logger.log_decision(
            conversation.id, negocio_id,
            current_node.node_key, next_node.node_key,
            razon, metodo,
            intent_result.confidence, similarity_result.confidence,
        )
        self.analytics_logger.log_response(
            conversation.id, negocio_id,
            next_node.node_key, response_metodo,
            len(response), abc_applied,
        )

        # --- Paso 14: Guardar mensaje del bot + commit ---
        if next_node.node_key in ("orden_confirmada", "comprobante_recibido"):
            conversation.status = "converted"
        elif next_node.node_key == "escalado_humano":
            conversation.status = "escalated"
            try:
                from app.core.notificaciones import crear_notificacion
                from app.models.notificacion import TipoNotificacion
                crear_notificacion(
                    self.db,
                    negocio_id=negocio_id,
                    tipo=TipoNotificacion.CONVERSACION_ESCALADA,
                    titulo="Conversación escalada a humano",
                    detalle=f"Tel: {client_phone}",
                    ruta_destino="/dashboard/escaladas",
                    referencia_id=conversation.id,
                )
            except Exception as e:
                logger.error("Error creando notificación escalado: %s", e)

        self._save_bot_message(
            conversation, response,
            node_key=next_node.node_key,
            ai_generated=(response_metodo == "llm"),
            ai_confidence=intent_result.confidence if intent_result else None,
        )

        dlog("ORCHESTRATOR", "FIN", nodo=next_node.node_key, respuesta=response)
        return response, True

    # ------------------------------------------------------------------
    # Comprobante — cortocircuito cuando llega imagen en esperar_comprobante
    # ------------------------------------------------------------------

    async def _handle_comprobante(
        self,
        negocio_id: int,
        conversation: Conversacion,
        media_url: str,
        caption: str = "",
    ) -> Tuple[str, bool]:
        """
        Maneja la recepción del comprobante de pago.
        Avanza el nodo a comprobante_recibido, persiste la URL en contexto
        y crea/actualiza el registro Pago vinculado a la orden.
        """
        dlog("ORCHESTRATOR", "Comprobante recibido", media_url=media_url[:80])

        # Descargar imagen de WAHA y alojarla en nuestro propio storage
        try:
            from app.services.storage import download_and_save
            from app.config import get_settings as _get_settings
            saved_path = await download_and_save(media_url)
            # S3 retorna URL absoluta; LocalStorage retorna path relativo
            if saved_path.startswith("http"):
                media_url = saved_path
            else:
                media_url = f"{_get_settings().media_base_url}{saved_path}"
        except Exception as e:
            logger.error("Error al descargar comprobante de WAHA: %s — usando URL original", e)

        # Guardar URL del comprobante en contexto y resetear contador de rechazos
        ctx = self.engine._get_context(conversation)
        ctx['comprobante_url'] = media_url
        ctx['comprobante_rechazos'] = 0
        conversation.context = json.dumps(ctx)

        # Persistir en tabla Pago (linkeado a la orden)
        orden_id_str = ctx.get('orden_numero')

        # Fallback: LID vs @c.us mismatch — busca la orden pendiente más reciente sin Pago
        if not orden_id_str:
            try:
                from app.models.orden import Pago as _Pago, Orden as _Orden
                from sqlalchemy import desc
                orden_fb = (
                    self.db.query(_Orden)
                    .outerjoin(_Pago, _Pago.orden_id == _Orden.id)
                    .filter(_Orden.negocio_id == negocio_id, _Pago.id == None)
                    .order_by(desc(_Orden.created_at))
                    .first()
                )
                if orden_fb:
                    orden_id_str = str(orden_fb.id)
                    ctx['orden_numero'] = orden_id_str
                    conversation.context = json.dumps(ctx)
                    logger.info(
                        "Comprobante: orden_numero por fallback | orden_id=%d | conv_id=%d",
                        orden_fb.id, conversation.id,
                    )
            except Exception as e:
                logger.error("Error en fallback orden lookup: %s", e)

        if orden_id_str:
            try:
                from app.models.orden import Pago, Orden, EstadoPago
                orden_id = int(orden_id_str)
                orden = self.db.get(Orden, orden_id)
                if orden:
                    pago = self.db.query(Pago).filter(Pago.orden_id == orden_id).first()
                    metodo = ctx.get('metodo_pago', 'no especificado')
                    if pago:
                        pago.comprobante_url = media_url
                        if not pago.metodo:
                            pago.metodo = metodo
                    else:
                        pago = Pago(
                            orden_id=orden_id,
                            metodo=metodo,
                            monto=float(orden.total),
                            comprobante_url=media_url,
                            estado=EstadoPago.PENDIENTE,
                        )
                        self.db.add(pago)
                    logger.info(
                        "Comprobante guardado en Pago | orden_id=%d | url=%s",
                        orden_id, media_url[:80],
                    )
            except Exception as e:
                logger.error("Error guardando comprobante en Pago: %s", e)

        # Avanzar al nodo comprobante_recibido
        next_node = self.engine._get_node_by_key(negocio_id, "comprobante_recibido")
        if next_node:
            conversation.current_node_key = "comprobante_recibido"
            conversation.last_message_at = datetime.utcnow()
            response = self.engine._generate_response(next_node, negocio_id, conversation)
        else:
            response = (
                "✅ ¡Comprobante recibido!\n"
                "Estamos verificando tu pago. En unos minutos te confirmamos."
            )

        # Notificación al operador
        try:
            from app.core.notificaciones import crear_notificacion
            from app.models.notificacion import TipoNotificacion
            orden_id_notif = int(ctx.get("orden_numero")) if ctx.get("orden_numero") else None
            crear_notificacion(
                self.db,
                negocio_id=negocio_id,
                tipo=TipoNotificacion.COMPROBANTE_RECIBIDO,
                titulo="Comprobante recibido",
                detalle=f"Orden #{ctx.get('orden_numero')}" if ctx.get("orden_numero") else None,
                ruta_destino="/dashboard/pagos",
                referencia_id=orden_id_notif,
            )
        except Exception as e:
            logger.error("Error creando notificación comprobante: %s", e)

        # Guardar mensaje del cliente (imagen) con media_url
        mensaje_cliente = MensajeConversacion(
            conversacion_id=conversation.id,
            sender_type="client",
            message_text=caption or "[comprobante]",
            media_url=media_url,
        )
        self.db.add(mensaje_cliente)

        self._save_bot_message(
            conversation, response, "comprobante_recibido",
            ai_generated=False, ai_confidence=None,
        )

        dlog("ORCHESTRATOR", "Comprobante procesado", respuesta=response[:100])
        return response, True

    # ------------------------------------------------------------------
    # Texto en esperar_comprobante - detectar problema de pago
    # ------------------------------------------------------------------

    async def _handle_texto_en_espera_comprobante(
        self,
        negocio_id: int,
        conversation: Conversacion,
        message_text: str,
    ) -> Tuple[str, bool]:
        """
        Maneja texto recibido mientras se espera el comprobante.
        Usa LLM para detectar si el cliente solicita asistencia humana.
        Si no, recuerda al cliente que envíe la imagen.
        """
        es_solicitud_humano = await self.intent_detector.detectar_solicitud_humano(message_text)

        dlog("ORCHESTRATOR", "Paso 6c: Texto en esperar_comprobante",
             text=message_text[:60],
             es_solicitud_humano=es_solicitud_humano)

        if es_solicitud_humano:
            node = self.engine._get_node_by_key(negocio_id, "escalado_humano")
            if node:
                conversation.current_node_key = "escalado_humano"
                conversation.last_message_at = datetime.utcnow()
                conversation.status = "escalated"
                response = self.engine._generate_response(node, negocio_id, conversation)
            else:
                conversation.status = "escalated"
                response = (
                    "Entiendo que estas teniendo problemas con el pago. \n\n"
                    "Voy a conectarte con un agente que te ayudara directamente."
                )
            self._save_bot_message(
                conversation, response, "escalado_humano",
                ai_generated=False, ai_confidence=None,
            )
            dlog("ORCHESTRATOR", "Escalado a humano", status="escalated")
            return response, True
        else:
            response = (
                "Estoy esperando tu comprobante de pago. \n"
                "Por favor enviame una captura de pantalla o foto del recibo."
            )
            self._save_bot_message(
                conversation, response, "esperar_comprobante",
                ai_generated=False, ai_confidence=None,
            )
            return response, True

    # ------------------------------------------------------------------
    # Persistir entidades del flujo Phase 2 en contexto
    # ------------------------------------------------------------------

    def _persist_flow_entities(
        self,
        conversation: Conversacion,
        target_node_key: str,
        intent_result: IntentResult,
        message_text: str,
    ) -> None:
        """
        Extrae y persiste en contexto las entidades clave del flujo:
        - orden_numero: desde intent_result.entidades o regex en message_text
        """
        entidades = (intent_result.entidades or {}) if intent_result else {}
        ctx = self.engine._get_context(conversation)
        changed = False

        # Número de orden (cuando llega confirmación de la webapp)
        if target_node_key == "pedido_recibido":
            orden = entidades.get("orden_numero")
            if not orden:
                m = re.search(r'#?(\d{3,})', message_text)
                if m:
                    orden = m.group(1)
            if orden:
                ctx['orden_numero'] = orden
                dlog("ORCHESTRATOR", "orden_numero extraído", orden=orden)
                changed = True

        if changed:
            conversation.context = json.dumps(ctx)

    # ------------------------------------------------------------------
    # Guardar mensaje del bot
    # ------------------------------------------------------------------

    def _save_bot_message(
        self,
        conversation: Conversacion,
        response: str,
        node_key: Optional[str],
        ai_generated: bool,
        ai_confidence: Optional[float],
    ) -> None:
        """Guarda el mensaje del bot con metadatos AI y hace commit global."""
        message = MensajeConversacion(
            conversacion_id=conversation.id,
            sender_type="bot",
            message_text=response,
            node_key=node_key,
            ai_generated=ai_generated,
            ai_confidence=ai_confidence,
        )
        self.db.add(message)
        self.db.commit()

    # ------------------------------------------------------------------
    # Similarity matching (async wrapper)
    # ------------------------------------------------------------------

    async def _run_similarity_matching(
        self,
        current_node,
        message_text: str,
        conversation: Conversacion,
    ) -> SimilarityResult:
        """Ejecuta el matching de similitud tradicional como coroutine."""
        if not current_node.expected_responses:
            return SimilarityResult()

        try:
            expected = (
                json.loads(current_node.expected_responses)
                if isinstance(current_node.expected_responses, str)
                else current_node.expected_responses
            )
        except Exception:
            return SimilarityResult()

        matched_response = match_expected_response(message_text, expected, threshold=0.6)

        if not matched_response or not current_node.next_node_map:
            return SimilarityResult()

        try:
            next_map = (
                json.loads(current_node.next_node_map)
                if isinstance(current_node.next_node_map, str)
                else current_node.next_node_map
            )
            for key, next_node_key in next_map.items():
                if key == "default":
                    continue
                options = [opt.strip() for opt in key.split("|")]
                if matched_response in options:
                    result = self.decision_engine.build_similarity_result(matched_response, next_node_key)
                    dlog("SIMILARITY", "Match encontrado",
                         matched=matched_response, next=next_node_key)
                    return result

            # Comprobar "default" como fallback de similarity
            default_key = next_map.get("default")
            if default_key and matched_response:
                result = self.decision_engine.build_similarity_result(matched_response, default_key)
                return result
        except Exception:
            pass

        return SimilarityResult()
