"""
Orchestrator — Coordinador principal del pipeline de mensajería.

Pipeline:
  1.  Cliente bloqueado        → FlowEngine
  2.  Bot activo               → FlowEngine
  3.  Horario de trabajo       → FlowEngine
  4.  Obtener/crear conversación
  5.  Guardar mensaje cliente
  6.  Obtener nodo actual
      6b. FAQ Match — cortocircuito sin LLM
  7.  Intent Detection + Similarity en paralelo
  8.  Decision Engine
  9.  Nodo destino
  10. Context Manager
  11. Response Generator
  12. Analytics Logger
  13. Guardar mensaje del bot

Dev mode: DEV_FLOW_LOG=True en .env para logs verbose.
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from app.bot.flow_engine import FlowEngine
from app.bot.intent_detector import IntentDetector, IntentResult
from app.bot.decision_engine import DecisionEngine, SimilarityResult
from app.bot.context_manager import ContextManager
from app.bot.response_generator import ResponseGenerator
from app.bot.analytics_logger import AnalyticsLogger
from app.bot.dev_logger import dlog
from app.bot.faq_matcher import match_faq
from app.models.bot import Conversacion, MensajeConversacion

logger = logging.getLogger(__name__)

ENTRY_NODE = "bienvenida"


class Orchestrator:
    def __init__(self, db: Session):
        self.db = db
        self.engine = FlowEngine(db)
        self.intent_detector = IntentDetector()
        self.decision_engine = DecisionEngine()
        self.context_manager = ContextManager()
        self.response_generator = ResponseGenerator()
        self.analytics_logger = AnalyticsLogger(db)

    async def process_message(
        self,
        negocio_id: int,
        client_phone: str,
        message_text: str,
        media_url: Optional[str] = None,
        waha_chat_id: Optional[str] = None,
    ) -> Tuple[str, bool]:
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
        conversation = self.engine._get_or_create_conversation(
            negocio_id, client_phone, waha_chat_id=waha_chat_id
        )
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

        if conversation.status == "escalated":
            dlog("ORCHESTRATOR", "Conversación escalada — bot silenciado")
            return "", False

        # --- Paso 6: Obtener nodo actual ---
        current_node = self.engine._get_current_node(conversation)

        if not current_node:
            current_node = self.engine._get_node_by_key(negocio_id, ENTRY_NODE)
            if current_node:
                conversation.current_node_key = ENTRY_NODE
                dlog("ORCHESTRATOR", "Paso 6: Primera interacción", nodo_inicial=ENTRY_NODE)

        if not current_node:
            dlog("ORCHESTRATOR", "Paso 6: ERROR - Flujo no configurado")
            response = "Lo siento, estamos experimentando problemas técnicos. Por favor intenta más tarde."
            self._save_bot_message(conversation, response, None, ai_generated=False, ai_confidence=None)
            return response, True

        dlog("ORCHESTRATOR", "Paso 6: Nodo actual",
             node_key=current_node.node_key,
             expected_responses=current_node.expected_responses or "ninguna")

        if not message_text:
            # Foto recibida mientras se espera en nodo pedir_foto
            if media_url and current_node and current_node.node_key == "pedir_foto":
                response = "Estamos procesando tus imágenes 📸\nEnvía más o escribe *listo* cuando termines."
                self._save_bot_message(conversation, response, "pedir_foto", ai_generated=False, ai_confidence=None)
                return response, True
            dlog("ORCHESTRATOR", "Sin texto ni foto esperada — ignorado")
            return "", False

        # --- Paso 6b: FAQ Match — cortocircuito sin LLM ---
        faq_respuesta = match_faq(self.db, negocio_id, message_text)
        if faq_respuesta:
            dlog("ORCHESTRATOR", "Paso 6b: FAQ match — cortocircuito",
                 respuesta=faq_respuesta[:60])
            self._save_bot_message(conversation, faq_respuesta, None, ai_generated=False, ai_confidence=None)
            return faq_respuesta, True
        dlog("ORCHESTRATOR", "Paso 6b: FAQ — sin match, continúa pipeline")

        # --- Paso 7: Intent Detection + Similarity Matching en paralelo ---
        context = self.context_manager.get(conversation)
        dlog("ORCHESTRATOR", "Paso 7: Lanzando análisis en paralelo",
             contexto_keys=list(context.keys()) if context else "vacío",
             modo="Intent Detector (DeepSeek) || Similarity Matcher")

        intent_start = time.monotonic()

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
            logger.error("Orchestrator: intent detection falló: %s", results[0])
        if isinstance(results[1], Exception):
            logger.error("Orchestrator: similarity matching falló: %s", results[1])

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
            logger.warning("Orchestrator: nodo '%s' no encontrado, usando fallback", target_node_key)
            response = self.engine._handle_fallback(conversation, message_text)
            self._save_bot_message(conversation, response, "fallback", ai_generated=False, ai_confidence=None)
            return response, True

        dlog("ORCHESTRATOR", "Paso 9: Nodo destino encontrado", node_key=next_node.node_key)

        # --- Paso 10: Context Manager ---
        conversation.current_node_key = next_node.node_key
        conversation.last_message_at = datetime.utcnow()
        context = self.context_manager.update(conversation, intent_result, message_text)

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

        # --- Paso 12: Analytics Logger ---
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
            len(response),
        )

        # --- Paso 13: Guardar mensaje del bot + commit ---
        if next_node.node_key == "escalado_humano":
            conversation.status = "escalated"

        self._save_bot_message(
            conversation, response,
            node_key=next_node.node_key,
            ai_generated=(response_metodo == "llm"),
            ai_confidence=intent_result.confidence if intent_result else None,
        )

        dlog("ORCHESTRATOR", "FIN", nodo=next_node.node_key, respuesta=response)
        return response, True

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
        """
        Navega exclusivamente por next_node_map, sin expected_responses.
        Prioridad: clave exacta (case-insensitive) → "default" → sin match.
        """
        if not current_node.next_node_map or not message_text.strip():
            return SimilarityResult()

        try:
            next_map = (
                json.loads(current_node.next_node_map)
                if isinstance(current_node.next_node_map, str)
                else current_node.next_node_map
            )
        except Exception:
            return SimilarityResult()

        text = message_text.strip().lower()

        for key, next_node_key in next_map.items():
            if key == "default":
                continue
            if text in [opt.strip() for opt in key.split("|")]:
                dlog("SIMILARITY", "Match exacto", matched=text, next=next_node_key)
                return self.decision_engine.build_similarity_result(text, next_node_key)

        default_key = next_map.get("default")
        if default_key:
            dlog("SIMILARITY", "Default advance", next=default_key)
            return self.decision_engine.build_similarity_result(text, default_key)

        return SimilarityResult()
