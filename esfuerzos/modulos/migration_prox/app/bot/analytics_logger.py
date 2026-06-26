"""
Analytics Logger - Registra eventos estructurados del pipeline en DB.

No hace commit propio: los eventos se acumulan en la sesión DB y el
Orchestrator hace el commit global junto con el resto de los cambios.
"""
import json
import logging

from sqlalchemy.orm import Session

from app.bot.dev_logger import dlog
from app.models.bot import EventoConversacion

logger = logging.getLogger(__name__)


class AnalyticsLogger:
    """
    Logger de eventos del pipeline al modelo EventoConversacion.
    Si falla, loguea el error pero no interrumpe el flujo principal.
    """

    def __init__(self, db: Session):
        self.db = db

    def log_intent(self, conversacion_id: int, operacion_id: int, intent_result, latency_ms: int = 0) -> None:
        data = {
            "intent": intent_result.intencion_principal if intent_result else "none",
            "confidence": round(intent_result.confidence, 3) if intent_result else 0.0,
            "urgencia": intent_result.urgencia if intent_result else "low",
            "sentiment": intent_result.sentiment if intent_result else "neutral",
            "cambio_de_tema": intent_result.cambio_de_tema if intent_result else False,
            "node_key": intent_result.node_key if intent_result else None,
            "entidades": intent_result.entidades if intent_result else {},
            "latency_ms": latency_ms,
        }
        self._log("intent_detected", conversacion_id, operacion_id, data)
        dlog("ANALYTICS LOGGER", "Evento: intent_detected",
             intent=data["intent"], confidence=f"{data['confidence']:.2f}", latency_ms=latency_ms)

    def log_decision(
        self,
        conversacion_id: int,
        operacion_id: int,
        current_node: str,
        target_node: str,
        razon: str,
        metodo: str,
        intent_confidence: float = 0.0,
        similarity_confidence: float = 0.0,
    ) -> None:
        data = {
            "current_node": current_node,
            "target_node": target_node,
            "razon": razon,
            "metodo": metodo,
            "intent_confidence": round(intent_confidence, 3),
            "similarity_confidence": round(similarity_confidence, 3),
        }
        self._log("decision_made", conversacion_id, operacion_id, data)
        dlog("ANALYTICS LOGGER", "Evento: decision_made",
             de=current_node, a=target_node, razon=razon, metodo=metodo)

    def log_response(
        self,
        conversacion_id: int,
        operacion_id: int,
        node_key: str,
        metodo: str,
        response_length: int,
    ) -> None:
        data = {
            "node_key": node_key,
            "metodo": metodo,
            "response_length": response_length,
        }
        self._log("response_generated", conversacion_id, operacion_id, data)
        dlog("ANALYTICS LOGGER", "Evento: response_generated",
             nodo=node_key, metodo=metodo, chars=response_length)

    def _log(self, event_type: str, conversacion_id: int, operacion_id: int, data: dict) -> None:
        try:
            event = EventoConversacion(
                conversacion_id=conversacion_id,
                operacion_id=operacion_id,
                event_type=event_type,
                data=json.dumps(data),
            )
            self.db.add(event)
        except Exception as e:
            logger.error(f"AnalyticsLogger: no se pudo registrar evento '{event_type}': {e}")
