"""
Response Generator - Decide entre template o LLM para generar la respuesta.

Estrategia:
  TEMPLATE_NODES → siempre usa templates del FlowEngine
  LLM_NODES      → siempre genera con DeepSeek
  Resto          → LLM si hay entidad específica detectada, template si no

Si el LLM falla, cae al template como fallback.
"""
import json
import logging
from typing import Tuple

from openai import AsyncOpenAI

from app.bot.dev_logger import dlog
from app.models.bot import BotConfig
from app.models.negocio import Negocio

logger = logging.getLogger(__name__)

TEMPLATE_NODES = {
    "greeting", "farewell", "location", "contact_info",
    "confirmation", "closure",
}

LLM_NODES = {"fallback"}


class ResponseGenerator:
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
        from app.config import get_settings
        settings = get_settings()

        node_key = node.node_key
        strategy = self._select_strategy(node_key, intent_result, settings)

        dlog("RESPONSE GENERATOR", "Estrategia seleccionada",
             nodo=node_key, estrategia=strategy)

        if strategy == "llm":
            response = await self._llm_response(
                node, negocio_id, conversation, intent_result, flow_engine, settings
            )
            return response, "llm"

        response = flow_engine._generate_response(node, negocio_id, conversation)
        return response, "template"

    def _select_strategy(self, node_key: str, intent_result, settings) -> str:
        if settings.force_llm_responses:
            return "llm"
        if node_key in TEMPLATE_NODES:
            return "template"
        if node_key in LLM_NODES:
            return "llm"
        if (intent_result and intent_result.entidades and
                intent_result.entidades.get("entidad_especifica")):
            return "llm"
        return "template"

    async def _llm_response(
        self, node, negocio_id, conversation, intent_result, flow_engine, settings
    ) -> str:
        if not settings.deepseek_api_key:
            return flow_engine._generate_response(node, negocio_id, conversation)

        try:
            ctx = self._load_context(node, negocio_id, conversation, intent_result)
            system_prompt = self._build_system_prompt(ctx["business_name"], node.node_key)
            user_prompt = self._build_node_prompt(node.node_key, ctx)

            dlog("RESPONSE GENERATOR", "Llamando DeepSeek",
                 nodo=node.node_key, prompt_preview=user_prompt[:200])

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
                 tokens=tokens, respuesta_preview=content[:150])
            return content

        except Exception as e:
            logger.warning("ResponseGenerator: LLM falló, usando template. Error: %s", e)
            return flow_engine._generate_response(node, negocio_id, conversation)

    def _load_context(self, node, negocio_id: int, conversation, intent_result) -> dict:
        db = None
        try:
            from sqlalchemy.orm import object_session
            db = object_session(conversation)
        except Exception:
            pass

        business_name = "el servicio"
        context = {}

        if db:
            negocio = db.query(Negocio).filter(Negocio.id == negocio_id).first()
            if negocio and negocio.nombre:
                business_name = negocio.nombre
            try:
                context = json.loads(conversation.context) if conversation.context else {}
            except Exception:
                context = {}

        client_messages = context.get("client_messages", [])
        recent_messages = client_messages[-3:] if client_messages else []
        recent_str = "\n".join(f"- {m}" for m in recent_messages) if recent_messages else "Ninguno."
        mensaje_actual = client_messages[-1] if client_messages else ""

        entidades = (intent_result.entidades or {}) if intent_result else {}
        intencion = intent_result.intencion_principal if intent_result else "unknown"
        sentiment = intent_result.sentiment if intent_result else "neutral"
        urgencia = intent_result.urgencia if intent_result else "low"

        return {
            "business_name": business_name,
            "historial_reciente": recent_str,
            "mensaje_actual": mensaje_actual,
            "intencion": intencion,
            "sentiment": sentiment,
            "urgencia": urgencia,
            "entidades": entidades,
            "current_node": node.node_key,
            "node_template": node.message_template or "",
        }

    def _build_system_prompt(self, bot_name: str, node_key: str) -> str:
        return (
            f"Eres el asistente de WhatsApp de {bot_name}. "
            "Tono: empático, claro y humano. "
            f"Contexto: {node_key}. "
            "Responde SOLO con el mensaje para WhatsApp. Sin explicaciones extra."
        )

    def _build_node_prompt(self, node_key: str, ctx: dict) -> str:
        return (
            f"Nodo: {node_key}\n"
            f"Template de referencia: {ctx['node_template']}\n"
            f"Mensaje del usuario: \"{ctx['mensaje_actual']}\"\n"
            f"Intención detectada: {ctx['intencion']} | Urgencia: {ctx['urgencia']}\n"
            f"Historial reciente:\n{ctx['historial_reciente']}\n\n"
            "Genera la respuesta para WhatsApp. Máximo 4 líneas."
        )

    def _prompt_fallback(self, ctx: dict) -> str:
        return (
            f"No entendiste el mensaje: \"{ctx['mensaje_actual']}\"\n"
            "Pide amablemente que lo reformule. Máximo 2 líneas."
        )
