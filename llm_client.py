"""
llm_client.py - OpenAI-compatible LLM call with a provider fallback chain.

One entry point, `chat_json()`, that tries each provider in order (primary Groq
first, then configured fallbacks). Within a provider it retries 429s using the
server-advertised reset (capped so a WhatsApp user is never left hanging), and on
exhaustion or any hard error it moves to the next provider. This centralizes the
retry/fallback logic that `waha_intake` and `llm_extractor` used to do inline.

Providers come from config:
  - primary: settings.llm_* (Groq)
  - fallbacks: settings.llm_fallbacks, a JSON list in env LLM_FALLBACKS, each:
      {"name": "...", "base_url": "...", "api_key": "...", "model": "...", "headers": {...}?}
Any fallback missing an api_key is skipped, UNLESS its base_url is local http://
(reserved for a future self-hosted Hermes). All providers must be OpenAI-compatible
(/chat/completions with messages + response_format json_object).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re

import httpx

from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_MAX_ATTEMPTS_PER_PROVIDER = 2   # 1 retry on 429 before moving to the next provider
_RETRY_CAP = 4.0                 # seconds; never leave a WhatsApp user hanging longer

# Bound concurrent LLM calls. Each webhook message is a BackgroundTask that may hold
# a provider call for many seconds; without a ceiling a surge of distinct phones
# piles up tasks (each holding an httpx client + a Groq slot) and grows memory toward
# the container limit. Excess callers wait here instead of hammering the providers.
_MAX_CONCURRENT = 8
_LLM_SEMAPHORE = asyncio.Semaphore(_MAX_CONCURRENT)


class LLMUnavailable(Exception):
    """Every provider in the chain failed. `rate_limited` is True when at least
    one provider failed specifically with a 429 (so the caller can show an
    'alta demanda' message instead of a generic error)."""

    def __init__(self, rate_limited: bool, last_error: object = None):
        self.rate_limited = rate_limited
        self.last_error = last_error
        super().__init__(
            f"all LLM providers failed (rate_limited={rate_limited}): {last_error}")


def _parse_duration(s: str) -> float:
    """Parse rate-limit reset strings ('235ms', '2.5s', '1m26.4s') to seconds."""
    s = (s or "").strip().lower()
    if not s:
        return 0.0
    if s.endswith("ms"):
        try:
            return float(s[:-2]) / 1000.0
        except ValueError:
            return 0.0
    total = 0.0
    m = re.search(r"([\d.]+)m(?!s)", s)
    if m:
        total += float(m.group(1)) * 60
    sec = re.search(r"([\d.]+)s", s)
    if sec:
        total += float(sec.group(1))
    return total


def _retry_after_seconds(resp: httpx.Response) -> float:
    """How long to wait before retrying a 429, from headers, capped."""
    ra = resp.headers.get("retry-after")
    if ra:
        try:
            return min(float(ra), _RETRY_CAP)
        except ValueError:
            pass
    reset = (resp.headers.get("x-ratelimit-reset-tokens")
             or resp.headers.get("x-ratelimit-reset-requests") or "")
    return min(_parse_duration(reset) or 2.0, _RETRY_CAP)


def _build_providers() -> list[dict]:
    chain: list[dict] = []
    if settings.llm_api_key:
        chain.append({
            "name": "groq",
            "base_url": settings.llm_base_url,
            "api_key": settings.llm_api_key,
            "model": settings.llm_model,
            "headers": {},
            "extra": {},
        })
    for fb in (settings.llm_fallbacks or []):
        if not isinstance(fb, dict):
            continue
        base_url = (fb.get("base_url") or "").strip()
        api_key = (fb.get("api_key") or "").strip()
        model = (fb.get("model") or "").strip()
        if not base_url or not model:
            continue
        # Require a key unless it's a local http endpoint (future self-hosted Hermes).
        if not api_key and not base_url.startswith("http://"):
            continue
        chain.append({
            "name": fb.get("name") or base_url,
            "base_url": base_url,
            "api_key": api_key,
            "model": model,
            "headers": fb.get("headers") or {},
            # extra body params merged into the request (e.g. reasoning_effort
            # for gpt-oss reasoning models so the JSON content isn't truncated).
            "extra": fb.get("extra") or {},
        })
    return chain


PROVIDERS = _build_providers()
logger.info("LLM provider chain: %s", [p["name"] for p in PROVIDERS] or "EMPTY")


async def chat_json(messages: list[dict], *, temperature: float = 0.3,
                    max_tokens: int = 400, timeout: float = 15.0) -> dict:
    """Call the provider chain and return the parsed JSON object from the first
    provider that responds. Raises LLMUnavailable if all providers fail."""
    last_error: object = None
    rate_limited_any = False
    async with _LLM_SEMAPHORE:
      for prov in PROVIDERS:
        url = prov["base_url"].rstrip("/") + "/chat/completions"
        headers = {"Content-Type": "application/json", **prov.get("headers", {})}
        if prov.get("api_key"):
            headers["Authorization"] = f"Bearer {prov['api_key']}"
        payload = {
            "model": prov["model"],
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
            **prov.get("extra", {}),
        }
        for attempt in range(_MAX_ATTEMPTS_PER_PROVIDER):
            try:
                async with httpx.AsyncClient(timeout=timeout) as cl:
                    resp = await cl.post(url, headers=headers, json=payload)
                if resp.status_code == 429:
                    rate_limited_any = True
                    wait = _retry_after_seconds(resp)
                    logger.warning("LLM 429 on %s (attempt %d/%d), waiting %.2fs",
                                   prov["name"], attempt + 1,
                                   _MAX_ATTEMPTS_PER_PROVIDER, wait)
                    if attempt + 1 < _MAX_ATTEMPTS_PER_PROVIDER:
                        await asyncio.sleep(wait)
                        continue
                    break  # provider exhausted → next provider
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
                result = json.loads(content)
                if PROVIDERS and prov is not PROVIDERS[0]:
                    logger.info("LLM served by fallback provider %s", prov["name"])
                return result
            except Exception as exc:  # noqa: BLE001 - any failure → next provider
                last_error = exc
                logger.warning("LLM provider %s failed: %s", prov["name"], exc)
                break  # non-429 error → next provider
    raise LLMUnavailable(rate_limited_any, last_error)
