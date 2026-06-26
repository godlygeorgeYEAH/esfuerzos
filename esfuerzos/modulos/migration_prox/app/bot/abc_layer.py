"""
ABC Layer (Always Be Closing) - Agrega micro-CTA al final de cada respuesta.

Empuja sutilmente hacia la conversión añadiendo una pregunta de cierre
adaptada al nodo actual.

No modifica la respuesta si:
  - El nodo es de cierre (farewell, confirmation, closure)
  - El último mensaje del cliente ya contiene una pregunta
  - El nodo no tiene CTA definido

Retorna tuple (mensaje_final, cta_obj | None).
El Orchestrator escribe cta_obj en context["cta_pending"] para que
el siguiente mensaje del cliente pueda hacer cortocircuito directo al nodo_destino.
"""
import logging

from app.bot.dev_logger import dlog

logger = logging.getLogger(__name__)

# Nodos donde NO se agrega CTA (mensajes de cierre o espera activa)
EXCLUDED_NODES = {
    # Phase 2
    "comprobante_recibido",
    "orden_confirmada",
    "orden_rechazada",
    "esperar_comprobante",
    "escalado_humano",
    # Legacy
    "farewell", "confirmation", "closure",
}

# Cada CTA es un objeto con:
#   texto              — se agrega al mensaje del bot
#   respuestas_esperadas — respuestas del cliente que activan el nodo_destino
#   nodo_destino       — node_key al que navegar si hay match
CTA_POOLS: dict[str, dict] = {
    "bienvenida": {
        "texto": "¿Quieres ver el menú?",
        "respuestas_esperadas": ["si", "sí", "ok", "está bien", "esta bien", "menu", "menú"],
        "nodo_destino": "ver_menu",
    },
    "info_negocio": {
        "texto": "¿Quieres hacer un pedido?",
        "respuestas_esperadas": ["si", "sí", "ok", "está bien", "esta bien"],
        "nodo_destino": "ver_menu",
    },
}



class ABCLayer:
    """
    Capa de optimización de conversión.
    Post-procesa la respuesta del bot para agregar CTA apropiado.
    """

    def apply(
        self, respuesta: str, target_node_key: str, context: dict
    ) -> tuple[str, dict | None]:
        """
        Aplica el CTA correspondiente al nodo si aplica.

        Returns:
            Tupla (respuesta_final, cta_obj).
            cta_obj es None si no se aplicó ningún CTA.
        """
        if target_node_key in EXCLUDED_NODES:
            dlog("ABC LAYER", "Nodo excluido — sin CTA", nodo=target_node_key)
            return respuesta, None

        if self._has_question(context):
            dlog("ABC LAYER", "Mensaje del cliente contiene pregunta — sin CTA",
                 nodo=target_node_key)
            return respuesta, None

        cta = CTA_POOLS.get(target_node_key)
        if not cta:
            dlog("ABC LAYER", "Sin CTA para este nodo", nodo=target_node_key)
            return respuesta, None

        dlog("ABC LAYER", "CTA aplicado", nodo=target_node_key, cta=cta["texto"])
        return f"{respuesta}\n\n{cta['texto']}", cta

    def _has_question(self, context: dict) -> bool:
        """Retorna True si el último mensaje del cliente contiene una pregunta."""
        messages = context.get("client_messages", [])
        if not messages:
            return False
        return "?" in messages[-1]
