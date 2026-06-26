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
import os
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
        operacion_id: int,
        client_phone: str,
        message_text: str,
        media_url: Optional[str] = None,
        waha_chat_id: Optional[str] = None,
    ) -> Tuple[str, bool]:
        dlog("ORCHESTRATOR", "INICIO",
             operacion_id=operacion_id,
             phone=client_phone,
             waha_chat_id=waha_chat_id,
             mensaje=message_text,
             media=bool(media_url))

        # --- Paso 1: Cliente bloqueado ---
        is_blocked = self.engine._is_client_blocked(operacion_id, client_phone)
        dlog("ORCHESTRATOR", "Paso 1: Cliente bloqueado",
             bloqueado=is_blocked,
             resultado="IGNORADO" if is_blocked else "OK -> continuar")
        if is_blocked:
            return "", False

        # --- Paso 2: Bot activo ---
        bot_config = self.engine._get_bot_config(operacion_id)
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
                operacion_id, client_phone, "client", message_text, save_conversation=False
            )
            return away_message, True

        # --- Paso 4: Obtener o crear conversación ---
        conversation = self.engine._get_or_create_conversation(
            operacion_id, client_phone, waha_chat_id=waha_chat_id
        )
        dlog("ORCHESTRATOR", "Paso 4: Conversación",
             conversation_id=conversation.id,
             nodo_actual=conversation.current_node_key or "None (nueva)")

        # --- Paso 5: Guardar mensaje del cliente ---
        self.engine._save_message(
            operacion_id, client_phone, "client", message_text,
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
            current_node = self.engine._get_node_by_key(operacion_id, ENTRY_NODE)
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

        # --- Paso 6c: Nodo pedir_foto — TTL + descarga de fotos ---
        if current_node and current_node.node_key == "pedir_foto":
            return await self._handle_pedir_foto(
                conversation, operacion_id, message_text, media_url
            )

        if not message_text:
            dlog("ORCHESTRATOR", "Sin texto — ignorado")
            return "", False

        # --- Paso 6b: FAQ Match — cortocircuito sin LLM ---
        faq_respuesta = match_faq(self.db, operacion_id, message_text)
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
            self.analytics_logger.log_intent(conversation.id, operacion_id, intent_result, intent_latency_ms)
            self.analytics_logger.log_decision(
                conversation.id, operacion_id,
                current_node.node_key, "fallback",
                razon, metodo,
                intent_result.confidence, similarity_result.confidence,
            )
            self._save_bot_message(conversation, response, "fallback", ai_generated=False, ai_confidence=None)
            return response, True

        # --- Paso 9: Obtener nodo destino ---
        next_node = self.engine._get_node_by_key(operacion_id, target_node_key)
        if not next_node:
            logger.warning("Orchestrator: nodo '%s' no encontrado, usando fallback", target_node_key)
            response = self.engine._handle_fallback(conversation, message_text)
            self._save_bot_message(conversation, response, "fallback", ai_generated=False, ai_confidence=None)
            return response, True

        dlog("ORCHESTRATOR", "Paso 9: Nodo destino encontrado", node_key=next_node.node_key)

        # --- Intake hooks ---
        if current_node.node_key == "guia_familiar" and next_node.node_key == "pedir_foto":
            _ctx = self.context_manager.get(conversation)
            _ctx["intake_person_raw"] = message_text
            conversation.context = json.dumps(_ctx)
            dlog("ORCHESTRATOR", "Intake: datos crudos capturados", raw=message_text[:80])

        if next_node.node_key == "reporte_guardado":
            from app.core.intake import commit_report
            _report = commit_report(self.db, conversation, client_phone, notes=message_text)
            if _report:
                dlog("ORCHESTRATOR", "Intake: report creado", report_id=_report.id)

        # --- Paso 10: Context Manager ---
        conversation.current_node_key = next_node.node_key
        conversation.last_message_at = datetime.utcnow()
        context = self.context_manager.update(conversation, intent_result, message_text)

        dlog("ORCHESTRATOR", "Paso 10: Context Manager",
             sentiment=context.get("sentiment_general", "neutral"))

        # --- Paso 11: Response Generator ---
        response, response_metodo = await self.response_generator.generate(
            node=next_node,
            operacion_id=operacion_id,
            conversation=conversation,
            intent_result=intent_result,
            flow_engine=self.engine,
        )
        dlog("ORCHESTRATOR", "Paso 11: Response Generator",
             metodo=response_metodo,
             respuesta_preview=response[:120])

        # --- Paso 12: Analytics Logger ---
        self.analytics_logger.log_intent(conversation.id, operacion_id, intent_result, intent_latency_ms)
        self.analytics_logger.log_decision(
            conversation.id, operacion_id,
            current_node.node_key, next_node.node_key,
            razon, metodo,
            intent_result.confidence, similarity_result.confidence,
        )
        self.analytics_logger.log_response(
            conversation.id, operacion_id,
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
    # Pedir foto — TTL + descarga + avance automático
    # ------------------------------------------------------------------

    async def _handle_pedir_foto(
        self,
        conversation: Conversacion,
        operacion_id: int,
        message_text: str,
        media_url: Optional[str],
    ) -> Tuple[str, bool]:
        from app.config import get_settings
        settings = get_settings()

        context = self.context_manager.get(conversation)
        pending_photos: list = context.get("pending_photos", [])
        last_photo_at_str: Optional[str] = context.get("last_photo_at")

        if media_url:
            local_path = await self._download_photo(media_url, conversation.id, len(pending_photos))
            pending_photos.append({
                "media_url": media_url,
                "local_path": local_path,
                "received_at": datetime.utcnow().isoformat(),
            })
            context["pending_photos"] = pending_photos
            context["last_photo_at"] = datetime.utcnow().isoformat()
            conversation.context = json.dumps(context)

            count = len(pending_photos)
            dlog("ORCHESTRATOR", "Foto recibida en pedir_foto",
                 count=count, max=settings.photo_max_count, local=local_path or "sin descarga")

            if count >= settings.photo_max_count:
                return self._advance_from_foto(conversation, operacion_id, context, count, "max_alcanzado")

            response = (
                f"📸 Imagen recibida ({count}/{settings.photo_max_count}).\n"
                'Puedes enviar más fotos. Escribe *listo* cuando termines.'
            )
            self._save_bot_message(conversation, response, "pedir_foto", ai_generated=False, ai_confidence=None)
            return response, True

        # Mensaje de texto en pedir_foto
        count = len(pending_photos)

        if message_text.strip().lower() == "listo":
            return self._advance_from_foto(conversation, operacion_id, context, count, "listo")

        response = (
            f"⏳ Tienes {count}/{settings.photo_max_count} foto(s) recibida(s).\n"
            'Puedes enviar más o escribe *listo* para continuar.'
        )
        self._save_bot_message(conversation, response, "pedir_foto", ai_generated=False, ai_confidence=None)
        return response, True

    def _advance_from_foto(
        self,
        conversation: Conversacion,
        operacion_id: int,
        context: dict,
        photo_count: int,
        motivo: str,
    ) -> Tuple[str, bool]:
        notas_node = self.engine._get_node_by_key(operacion_id, "notas_adicionales")
        if notas_node:
            response = self.engine._generate_response(notas_node, operacion_id, conversation)
        else:
            response = (
                "📸 Imágenes recibidas.\n\n"
                "¿Tienes señas, ropa u otros detalles? Escríbelos ahora.\n\n"
                "O escribe *reporte* para registrar un nuevo caso."
            )
        conversation.current_node_key = "notas_adicionales"
        dlog("ORCHESTRATOR", "Avance automático desde pedir_foto",
             fotos=photo_count, motivo=motivo)
        self._save_bot_message(conversation, response, "notas_adicionales", ai_generated=False, ai_confidence=None)
        return response, True

    async def _download_photo(
        self,
        media_url: str,
        conversation_id: int,
        index: int,
    ) -> Optional[str]:
        import httpx
        from app.config import get_settings
        settings = get_settings()

        storage_dir = settings.photo_storage_path
        os.makedirs(storage_dir, exist_ok=True)

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(media_url)
                resp.raise_for_status()

                content_type = resp.headers.get("content-type", "image/jpeg")
                ext = "jpg"
                if "png" in content_type:
                    ext = "png"
                elif "webp" in content_type:
                    ext = "webp"
                elif "gif" in content_type:
                    ext = "gif"

                filename = f"{conversation_id}_{index}.{ext}"
                local_path = os.path.join(storage_dir, filename)

                with open(local_path, "wb") as f:
                    f.write(resp.content)

                logger.info("Foto descargada: %s → %s", media_url, local_path)
                return local_path
        except Exception as e:
            logger.warning("No se pudo descargar foto %s: %s", media_url, e)
            return None

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
