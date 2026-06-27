import logging
import httpx
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def chat(messages: list[dict], system_prompt: str = "") -> str:
    """
    Envía una conversación a la API de DeepSeek y retorna el texto de respuesta.

    Args:
        messages: Lista de dicts con formato {"role": "user"|"assistant", "content": "..."}
        system_prompt: Prompt de sistema que define el comportamiento del modelo.

    Returns:
        Texto de la respuesta del modelo.
    """
    if not settings.deepseek_api_key:
        raise ValueError("DEEPSEEK_API_KEY no configurada")

    full_messages = []
    if system_prompt:
        full_messages.append({"role": "system", "content": system_prompt})
    full_messages.extend(messages)

    headers = {
        "Authorization": f"Bearer {settings.deepseek_api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": settings.deepseek_model,
        "messages": full_messages,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{settings.deepseek_base_url}/chat/completions",
            json=body,
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()

    content = data["choices"][0]["message"]["content"]
    logger.debug("DeepSeek response: %s", content[:100])
    return content
