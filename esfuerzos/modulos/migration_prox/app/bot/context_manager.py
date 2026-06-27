"""
Context Manager - Gestiona el estado enriquecido de la conversación.

Extiende el context JSON de la conversación con:
- Historial de intenciones detectadas
- Últimos mensajes del cliente
- Sentiment general acumulado
- Contador de mensajes

Claves del contexto gestionadas por otros componentes (no tocar en update()):
- cta_pending  : escrito por Orchestrator (paso 12) tras aplicar un CTA con nodo_destino.
                 Leído y limpiado por Orchestrator (paso 6d) en el siguiente mensaje.
                 Estructura: {"respuestas_esperadas": [...], "nodo_destino": str}
"""
import json
import logging
from datetime import datetime, timezone

from app.bot.dev_logger import dlog

logger = logging.getLogger(__name__)

MAX_HISTORY = 10


class ContextManager:
    """
    Gestor de contexto de conversación.
    No hace commit — el Orchestrator gestiona las transacciones.
    """

    def get(self, conversation) -> dict:
        """Parsea el context JSON de la conversación de forma segura."""
        if not conversation.context:
            return {}
        try:
            return json.loads(conversation.context)
        except Exception:
            return {}

    def update(self, conversation, intent_result, mensaje: str) -> dict:
        """
        Actualiza el contexto con los datos de la interacción actual.

        Extrae entidades del intent_result, actualiza historial de intenciones,
        mantiene los últimos mensajes del cliente, calcula sentiment_general
        y probabilidad de conversión. Persiste en conversation.context.
        """
        context = self.get(conversation)

        # --- Entidades del intent ---
        if intent_result and intent_result.entidades:
            entidades = intent_result.entidades

            if entidades.get("servicio_especifico"):
                servicio = entidades["servicio_especifico"]
                context["selected_service"] = servicio
                if "interes_en" not in context:
                    context["interes_en"] = []
                if servicio not in context["interes_en"]:
                    context["interes_en"].append(servicio)

            if entidades.get("horario_mencionado"):
                context["horario_preferido"] = entidades["horario_mencionado"]


        # --- Historial de intenciones (últimas MAX_HISTORY) ---
        if intent_result and intent_result.intencion_principal != "unknown":
            if "historial_intenciones" not in context:
                context["historial_intenciones"] = []
            context["historial_intenciones"].append({
                "nodo": intent_result.node_key or "unknown",
                "intencion": intent_result.intencion_principal,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "confidence": round(intent_result.confidence, 2),
            })
            context["historial_intenciones"] = context["historial_intenciones"][-MAX_HISTORY:]

        # --- Mensajes del cliente (últimos MAX_HISTORY) ---
        if "client_messages" not in context:
            context["client_messages"] = []
        context["client_messages"].append(mensaje)
        context["client_messages"] = context["client_messages"][-MAX_HISTORY:]

        # --- Contador de mensajes ---
        context["message_count"] = context.get("message_count", 0) + 1

        # --- Sentiment general (moda de los últimos sentiments) ---
        if intent_result and intent_result.sentiment:
            if "sentiment_history" not in context:
                context["sentiment_history"] = []
            context["sentiment_history"].append(intent_result.sentiment)
            context["sentiment_history"] = context["sentiment_history"][-MAX_HISTORY:]
            context["sentiment_general"] = self._moda_sentiment(context["sentiment_history"])

        # --- Persistir ---
        conversation.context = json.dumps(context)

        dlog("CONTEXT MANAGER", "Contexto actualizado",
             interes_en=context.get("interes_en", []),
             horario=context.get("horario_preferido", "none"),
             sentiment=context.get("sentiment_general", "neutral"),
             mensaje_count=context.get("message_count", 0))

        return context

    def _moda_sentiment(self, sentiments: list) -> str:
        """Retorna el sentiment más frecuente de la lista."""
        if not sentiments:
            return "neutral"
        return max(set(sentiments), key=sentiments.count)
