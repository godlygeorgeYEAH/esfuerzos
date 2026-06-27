"""
Decision Engine v2.0 - Decide el siguiente nodo basado en intent detection y similarity matching.

Sistema de reglas priorizadas (en orden):
  1. Alta confianza + alta urgencia  → saltar al nodo del intent
  2. Cambio de tema claro            → respetar autonomía del cliente
  3. Intent baja confianza + match  → seguir el flujo tradicional
  4. Intent con confianza media      → confiar en el LLM
  5. Solo similarity disponible      → flujo tradicional
  6. Ninguna regla aplica            → fallback
"""
import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from app.config import get_settings
from app.bot.dev_logger import dlog

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class SimilarityResult:
    """Resultado del matching de similitud tradicional (keywords + Levenshtein)."""
    match: Optional[str] = None
    confidence: float = 0.0
    metodo: str = "none"


class DecisionEngine:
    """
    Motor de decisiones basado en reglas priorizadas.
    Lógica pura sin dependencias externas — 100% testeable sin mocks.
    """

    def decidir_navegacion(
        self,
        intent,
        similarity: SimilarityResult,
        current_node: str,
        contexto: dict,
    ) -> Tuple[str, str, str]:
        """
        Decide el siguiente nodo aplicando reglas en orden de prioridad.

        Returns:
            Tupla (target_node_key, razon, metodo)
        """
        ic = intent.confidence if intent else 0.0
        ik = intent.node_key if intent else None

        dlog("DECISION ENGINE", "Evaluando reglas",
             intent=f"{intent.intencion_principal if intent else 'none'} (conf={ic:.2f})",
             urgencia=intent.urgencia if intent else "none",
             cambio_tema=intent.cambio_de_tema if intent else False,
             similarity=f"{similarity.match} (conf={similarity.confidence:.2f})",
             umbrales=f"high={settings.intent_high_confidence} mid={settings.intent_medium_confidence} low={settings.intent_low_confidence}")

        # Regla 1: Alta confianza + alta urgencia
        if intent and ic > settings.intent_high_confidence and intent.urgencia == "high" and ik:
            dlog("DECISION ENGINE", "Regla 1: high_confidence_urgent", decision=ik)
            return self._decision(ik, "high_confidence_urgent", "intent", intent, similarity)

        # Regla 2: Cambio de tema claro
        if intent and intent.cambio_de_tema and ic > settings.intent_medium_confidence and ik:
            dlog("DECISION ENGINE", "Regla 2: topic_change", decision=ik)
            return self._decision(ik, "topic_change", "intent", intent, similarity)

        # Regla 3: Intent baja confianza pero hay similarity match
        if intent and ic < settings.intent_medium_confidence and similarity.match:
            dlog("DECISION ENGINE", "Regla 3: following_flow", decision=similarity.match)
            return self._decision(similarity.match, "following_flow", "similarity", intent, similarity)

        # Regla 4: Intent con confianza media-alta
        if intent and ic > settings.intent_low_confidence and ik:
            dlog("DECISION ENGINE", "Regla 4: intent_detected", decision=ik)
            return self._decision(ik, "intent_detected", "intent", intent, similarity)

        # Regla 5: Solo similarity disponible
        if similarity.match:
            dlog("DECISION ENGINE", "Regla 5: low_confidence_follow_flow", decision=similarity.match)
            return self._decision(similarity.match, "low_confidence_follow_flow", "similarity", intent, similarity)

        # Regla 6: Fallback total
        dlog("DECISION ENGINE", "Regla 6: fallback total", decision="fallback")
        return self._decision("fallback", "no_clear_intent", "fallback", intent, similarity)

    def _decision(self, target_node_key, razon, metodo, intent, similarity) -> Tuple[str, str, str]:
        logger.info(
            f"DecisionEngine | {razon} | "
            f"intent={intent.intencion_principal if intent else 'none'}({intent.confidence if intent else 0:.2f}) | "
            f"similarity={similarity.match}({similarity.confidence:.2f}) | "
            f"→ {target_node_key}"
        )
        return target_node_key, razon, metodo

    def build_similarity_result(self, matched_response: Optional[str], next_node_key: Optional[str]) -> SimilarityResult:
        if next_node_key:
            return SimilarityResult(match=next_node_key, confidence=1.0, metodo="keyword_levenshtein")
        return SimilarityResult()
