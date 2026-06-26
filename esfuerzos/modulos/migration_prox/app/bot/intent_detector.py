"""
Intent Detector v2.0 - Detecta la intención del mensaje usando DeepSeek LLM.

Analiza el mensaje del cliente y retorna un IntentResult estructurado con:
- Intención principal (greeting, services, pricing, etc.)
- Entidades detectadas (artículo específico, horario, precio)
- Nivel de urgencia, sentimiento y confidence
- Si hubo cambio de tema respecto al nodo actual
"""
import json
import logging
from dataclasses import dataclass, field
from typing import Optional

from openai import AsyncOpenAI

from app.config import get_settings
from app.bot.dev_logger import dlog

logger = logging.getLogger(__name__)
settings = get_settings()

# Mapeo de intenciones detectadas por el LLM → node_keys del flujo
INTENT_TO_NODE = {
    "saludo":              "bienvenida",
    "ver_menu":            "ver_menu",            # quiere ver el menú / hacer un pedido
    "confirmar_orden":     "pedido_recibido",      # webapp envió número de orden
    "enviar_comprobante":  "esperar_comprobante",  # quiere enviar/enviará comprobante
    "consulta_info":       "info_negocio",          # FAQ: horarios, delivery, métodos de pago
    "despedida":           "bienvenida",            # vuelve al inicio para futura interacción
    "solicitar_humano":    "escalado_humano",       # pide hablar con un agente humano
    "problemas_tecnicos":  "escalado_humano",        # problemas con la webapp, imágenes, carga, etc.
    "unknown":             "fallback",              # no encaja en ninguna intención conocida
}

VALID_INTENTS = list(INTENT_TO_NODE.keys())

GREETING_KEYWORDS = {
    "hola", "buenos días", "buenas tardes", "buenas noches",
    "buen día", "buenas", "hey", "qué tal", "que tal",
}


@dataclass
class IntentResult:
    """Resultado estructurado del análisis de intención."""
    intencion_principal: str = "unknown"
    intenciones_secundarias: list = field(default_factory=list)
    entidades: dict = field(default_factory=dict)
    urgencia: str = "low"           # "low" | "medium" | "high"
    sentiment: str = "neutral"      # "positive" | "neutral" | "negative"
    cambio_de_tema: bool = False
    confidence: float = 0.0
    node_key: Optional[str] = None


class IntentDetector:
    """
    Detector de intención usando DeepSeek LLM via OpenAI-compatible API.
    """

    def __init__(self):
        self.client = AsyncOpenAI(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
        )

    async def detectar_intencion(
        self,
        mensaje: str,
        contexto: dict,
        current_node: str = "greeting",
        expected_responses: list | None = None,
        enabled: bool = False,
    ) -> IntentResult:
        """Detecta la intención del mensaje del cliente usando DeepSeek."""
        if not enabled:
            dlog("INTENT DETECTOR", "Desactivado (enable_intent_detection=false)", resultado="IntentResult vacío")
            return IntentResult()

        if not settings.deepseek_api_key:
            logger.warning("IntentDetector: deepseek_api_key no configurada")
            dlog("INTENT DETECTOR", "Sin API key", resultado="IntentResult vacío")
            return IntentResult()

        prompt = self._build_prompt(mensaje, contexto, current_node, expected_responses)

        dlog("INTENT DETECTOR", "Llamando a DeepSeek",
             modelo=settings.deepseek_model,
             mensaje=mensaje,
             nodo_actual=current_node,
             prompt_preview=prompt[:200])

        for attempt in range(settings.deepseek_max_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=settings.deepseek_model,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "Eres un analizador de intenciones para un bot de WhatsApp "
                                "de un negocio gastronómico en Venezuela. "
                                "Analiza mensajes de clientes y responde SOLO con JSON válido, "
                                "sin markdown, sin texto adicional."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.1,
                    max_tokens=300,
                    timeout=settings.deepseek_timeout,
                )

                content = response.choices[0].message.content
                tokens = response.usage.total_tokens if response.usage else "?"

                dlog("INTENT DETECTOR", "Respuesta recibida",
                     tokens_usados=tokens, respuesta_raw=content)

                result = self._parse_response(content, mensaje)

                dlog("INTENT DETECTOR", "IntentResult parseado",
                     intent=result.intencion_principal,
                     confidence=f"{result.confidence:.2f}",
                     urgencia=result.urgencia,
                     sentiment=result.sentiment,
                     cambio_tema=result.cambio_de_tema,
                     entidades=result.entidades,
                     node_key=result.node_key)

                logger.info(
                    f"IntentDetector: intent={result.intencion_principal}, "
                    f"confidence={result.confidence:.2f}, node={result.node_key}"
                )
                return result

            except Exception as e:
                logger.warning(f"IntentDetector: intento {attempt + 1} fallido: {e}")
                dlog("INTENT DETECTOR", f"Intento {attempt + 1} fallido", error=str(e))
                if attempt < settings.deepseek_max_retries - 1:
                    continue

        logger.error("IntentDetector: todos los reintentos fallaron")
        return IntentResult()

    async def detectar_solicitud_humano(self, mensaje: str) -> bool:
        """
        Determina si el cliente está pidiendo hablar con un humano/agente.
        Llamada LLM enfocada, siempre activa (no depende del flag enable_intent_detection).
        Retorna False si la API no está disponible (fail-safe: no escalar por error).
        """
        if not settings.deepseek_api_key:
            return False

        prompt = (
            f"El cliente de un negocio gastronómico en Venezuela envió este mensaje: \"{mensaje}\"\n\n"
            "¿Está pidiendo hablar con una persona real, un agente humano o asistencia personalizada?\n"
            "Considera también señales implícitas como frustración extrema o situaciones que claramente "
            "requieren intervención humana.\n\n"
            "Responde SOLO con JSON: {\"solicita_humano\": true} o {\"solicita_humano\": false}"
        )

        for attempt in range(settings.deepseek_max_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=settings.deepseek_model,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "Eres un clasificador binario. Responde SOLO con JSON válido, "
                                "sin markdown, sin texto adicional."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.0,
                    max_tokens=20,
                    timeout=settings.deepseek_timeout,
                )
                content = response.choices[0].message.content.strip()
                if content.startswith("```"):
                    content = content.split("```")[1].lstrip("json").strip()
                data = json.loads(content)
                result = bool(data.get("solicita_humano", False))
                dlog("INTENT DETECTOR", "detectar_solicitud_humano",
                     mensaje=mensaje[:60], resultado=result)
                return result
            except Exception as e:
                logger.warning(f"detectar_solicitud_humano: intento {attempt + 1} fallido: {e}")
                if attempt < settings.deepseek_max_retries - 1:
                    continue

        logger.error("detectar_solicitud_humano: todos los reintentos fallaron")
        return False

    def _build_prompt(self, mensaje: str, contexto: dict, current_node: str, expected_responses: list | None = None) -> str:
        """Construye el prompt con contexto relevante y few-shot examples."""
        interes_en = contexto.get("interes_en", [])
        selected_service = contexto.get("selected_service")

        orden_numero = contexto.get("orden_numero")

        context_info = f"Nodo actual: {current_node}"
        if expected_responses:
            context_info += f"\nRespuestas esperadas en este nodo: {', '.join(expected_responses)}"
        if interes_en:
            context_info += f"\nArtículos de interés previos: {', '.join(interes_en)}"
        if orden_numero:
            context_info += f"\nNúmero de orden en curso: #{orden_numero}"

        intents_str = ", ".join(VALID_INTENTS)

        return f"""Analiza este mensaje de WhatsApp de un cliente de un negocio gastronómico y responde con JSON.

DEFINICIÓN EXACTA DE CADA INTENCIÓN — úsalas con precisión:
- saludo: el cliente saluda sin pedir nada específico.
- ver_menu: el cliente quiere ver el menú o hacer un pedido.
- confirmar_orden: el cliente confirma que realizó un pedido en la webapp (incluye número de orden).
- enviar_comprobante: el cliente dice que va a enviar o ya envió el comprobante.
- consulta_info: el cliente pregunta por horarios, zona de cobertura, métodos de pago aceptados o información del negocio. EXCLUSIVAMENTE para FAQ del negocio — NO para problemas técnicos.
- despedida: el cliente se despide.
- solicitar_humano: el cliente pide hablar con una persona real.
- problemas_tecnicos: el cliente reporta un problema técnico con la webapp, el carrito, imágenes que no cargan, errores de pantalla o cualquier falla digital. NO es consulta_info.
- unknown: el mensaje no encaja claramente en ninguna de las intenciones anteriores. Úsala cuando haya duda real.

REGLAS CRÍTICAS para intenciones_secundarias:
1. Si el mensaje incluye saludo ("hola", "buenos días", etc.), OBLIGATORIO incluir "saludo" en secundarias.
2. Si el mensaje menciona múltiples temas, listar TODOS excepto el principal.
3. NO dejar [] si hay saludo o múltiples temas.

ENTIDADES A EXTRAER:
- servicio_especifico: plato o artículo mencionado (ej: "pizza", "hamburguesa")
- orden_numero: número de orden si aparece (ej: "1234" de "Pedido #1234 confirmado")
- horario_mencionado: día u hora si la menciona

EJEMPLOS:
Mensaje: "Hola"
{{"intencion_principal":"saludo","intenciones_secundarias":[],"entidades":{{"servicio_especifico":null,"orden_numero":null,"horario_mencionado":null}},"urgencia":"low","sentiment":"positive","cambio_de_tema":false,"confidence":0.97}}

Mensaje: "Quiero ver el menú"
{{"intencion_principal":"ver_menu","intenciones_secundarias":[],"entidades":{{"servicio_especifico":null,"orden_numero":null,"horario_mencionado":null}},"urgencia":"low","sentiment":"positive","cambio_de_tema":false,"confidence":0.95}}

Mensaje: "Pedido #1234 confirmado ✓"
{{"intencion_principal":"confirmar_orden","intenciones_secundarias":[],"entidades":{{"servicio_especifico":null,"orden_numero":"1234","metodo_pago":null,"horario_mencionado":null}},"urgencia":"high","sentiment":"positive","cambio_de_tema":true,"confidence":0.98}}

Mensaje: "Cuáles son los horarios y hacen delivery?"
{{"intencion_principal":"consulta_info","intenciones_secundarias":[],"entidades":{{"servicio_especifico":null,"orden_numero":null,"horario_mencionado":null}},"urgencia":"low","sentiment":"neutral","cambio_de_tema":false,"confidence":0.90}}

Mensaje: "Hola, quiero pedir una pizza"
{{"intencion_principal":"ver_menu","intenciones_secundarias":["saludo"],"entidades":{{"servicio_especifico":"pizza","orden_numero":null,"horario_mencionado":null}},"urgencia":"low","sentiment":"positive","cambio_de_tema":false,"confidence":0.92}}

Mensaje: "No veo las imágenes del carrito"
{{"intencion_principal":"problemas_tecnicos","intenciones_secundarias":[],"entidades":{{"servicio_especifico":null,"orden_numero":null,"horario_mencionado":null}},"urgencia":"medium","sentiment":"negative","cambio_de_tema":true,"confidence":0.93}}

Mensaje: "Qué rico todo"
{{"intencion_principal":"unknown","intenciones_secundarias":[],"entidades":{{"servicio_especifico":null,"orden_numero":null,"horario_mencionado":null}},"urgencia":"low","sentiment":"positive","cambio_de_tema":false,"confidence":0.60}}

CONTEXTO:
{context_info}

MENSAJE DEL CLIENTE: "{mensaje}"

INTENCIONES VÁLIDAS: {intents_str}

Responde SOLO con JSON válido (sin markdown, sin texto adicional)."""

    def _parse_response(self, content: str, mensaje_original: str = "") -> IntentResult:
        """Parsea la respuesta JSON del LLM en un IntentResult."""
        try:
            content = content.strip()
            if content.startswith("```"):
                parts = content.split("```")
                content = parts[1] if len(parts) > 1 else content
                if content.startswith("json"):
                    content = content[4:]

            data = json.loads(content.strip())

            intent = data.get("intencion_principal", "unknown")
            if intent not in VALID_INTENTS:
                intent = "unknown"

            node_key = INTENT_TO_NODE.get(intent, "fallback")

            try:
                confidence = float(data.get("confidence", 0.0))
                confidence = max(0.0, min(1.0, confidence))
            except (TypeError, ValueError):
                confidence = 0.0

            urgencia = data.get("urgencia", "low")
            if urgencia not in ("low", "medium", "high"):
                urgencia = "low"

            sentiment = data.get("sentiment", "neutral")
            if sentiment not in ("positive", "neutral", "negative"):
                sentiment = "neutral"

            entidades = data.get("entidades", {})
            if not isinstance(entidades, dict):
                entidades = {}

            # Safety net: agregar "saludo" a secundarias si el LLM lo omitió
            intenciones_sec = data.get("intenciones_secundarias", []) or []
            if (mensaje_original and intent != "saludo" and "saludo" not in intenciones_sec):
                msg_lower = mensaje_original.lower()
                if any(kw in msg_lower for kw in GREETING_KEYWORDS):
                    intenciones_sec = ["saludo"] + intenciones_sec
                    dlog("INTENT DETECTOR", "Safety net: saludo agregado a secundarias")

            return IntentResult(
                intencion_principal=intent,
                intenciones_secundarias=intenciones_sec,
                entidades=entidades,
                urgencia=urgencia,
                sentiment=sentiment,
                cambio_de_tema=bool(data.get("cambio_de_tema", False)),
                confidence=confidence,
                node_key=node_key,
            )

        except Exception as e:
            logger.error(f"IntentDetector: error parseando respuesta: {e} | content: {content[:200]}")
            return IntentResult()
