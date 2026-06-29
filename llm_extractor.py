"""
llm_extractor.py - On-demand LLM extraction of person records (Mode A).

A volunteer pastes a URL (Facebook/news/post) or raw text; the LLM pulls
structured missing/found person records out of unstructured content and lands
them in the llm_leads REVIEW QUEUE. Nothing here enters the canonical reports
table automatically — a human approves a lead first (see approve_lead()).

Safety:
  - Every lead stores a verbatim source quote (context) + an LLM confidence.
  - Low-confidence (<0.6) or nameless rows are dropped.
  - Output goes ONLY to llm_leads (RLS, service_role). Approval is a separate,
    explicit step that copies a lead into reports via the normal upsert path.

No new dependencies: HTML is stripped with BeautifulSoup (already used by
scrapers). Extraction uses the existing Groq/OpenAI-compatible LLM config.
"""
from __future__ import annotations

import logging
import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

from config import get_settings
from llm_client import chat_json

logger = logging.getLogger(__name__)
settings = get_settings()

_UA = {"User-Agent": "Mozilla/5.0 (compatible; ReuneVE-LLM/1.0)"}
_MAX_CHARS = 12000          # cap content sent to the LLM
_MIN_CONFIDENCE = 0.6

_EXTRACT_PROMPT = """Eres un extractor de datos para Reune VE (reunificación familiar en emergencias).
Del texto te doy, extrae TODAS las personas desaparecidas o encontradas que se mencionen.

Para cada persona devuelve:
  full_name: nombre completo (obligatorio; si no hay nombre claro, omite la persona)
  age: edad en número o null
  location: último lugar visto / dónde fue encontrada, o null
  kind: "missing" (la buscan) | "found" (fue encontrada/localizada/hospitalizada)
  contact: teléfono de contacto si aparece, o null
  confidence: 0.0 a 1.0, qué tan seguro estás de que es un reporte real de persona
  context: cita textual breve (máx 200 chars) de la parte del texto que respalda este registro

Reglas:
- NUNCA inventes datos. Si un campo no está, usa null.
- NUNCA marques a alguien como fallecido.
- Ignora texto que no sea sobre personas (noticias generales, publicidad, navegación).

Devuelve SOLO este JSON:
{"persons": [ { "full_name": ..., "age": ..., "location": ..., "kind": ..., "contact": ..., "confidence": ..., "context": ... } ]}
Si no hay personas, devuelve {"persons": []}."""


async def _fetch_text(url: str) -> str:
    async with httpx.AsyncClient(timeout=20, headers=_UA, follow_redirects=True) as cl:
        r = await cl.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer", "noscript"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)
        return re.sub(r"\s+", " ", text)[:_MAX_CHARS]


async def _llm_extract_persons(content: str) -> list[dict]:
    messages = [
        {"role": "system", "content": _EXTRACT_PROMPT},
        {"role": "user", "content": content},
    ]
    # Goes through the provider fallback chain (Groq → fallbacks) with retry.
    data = await chat_json(messages, temperature=0.1, max_tokens=1500, timeout=30)
    return data.get("persons") or []


def _sb_headers(prefer: str = "return=minimal") -> dict:
    k = settings.supabase_service_role_key
    return {"apikey": k, "Authorization": f"Bearer {k}",
            "Content-Type": "application/json", "Prefer": prefer}


async def extract_to_queue(url: str | None = None, text: str | None = None,
                           dry_run: bool = False) -> dict:
    """Extract persons from a URL or raw text into the llm_leads review queue.
    Returns {found, queued, dropped, persons(dry_run)}."""
    sb = settings.supabase_url.rstrip("/")
    src = "llm:url" if url else "llm:text"
    try:
        content = await _fetch_text(url) if url else (text or "")[:_MAX_CHARS]
    except Exception as exc:
        logger.warning("llm_extractor fetch failed: %s", exc)
        return {"error": f"fetch failed: {exc}", "found": 0, "queued": 0}
    if not content.strip():
        return {"found": 0, "queued": 0, "dropped": 0, "note": "empty content"}

    try:
        persons = await _llm_extract_persons(content)
    except Exception as exc:
        logger.error("llm_extractor LLM failed: %s", exc)
        return {"error": f"llm failed: {exc}", "found": 0, "queued": 0}

    # Validate
    valid = []
    dropped = 0
    for p in persons:
        name = (p.get("full_name") or "").strip()
        try:
            conf = float(p.get("confidence", 0))
        except (TypeError, ValueError):
            conf = 0.0
        if not name or conf < _MIN_CONFIDENCE:
            dropped += 1
            continue
        age = p.get("age")
        try:
            age = int(age) if age not in (None, "") else None
        except (TypeError, ValueError):
            age = None
        valid.append({
            "source": src, "source_url": url,
            "full_name": name, "age": age,
            "location": p.get("location") or None,
            "kind": (p.get("kind") or "missing"),
            "contact": p.get("contact") or None,
            "confidence": round(conf, 3),
            "context": (p.get("context") or "")[:200],
            "raw_data": p,
        })

    if dry_run:
        return {"found": len(persons), "queued": 0, "dropped": dropped, "persons": valid}

    queued = 0
    if valid:
        async with httpx.AsyncClient(timeout=20) as cl:
            resp = await cl.post(
                f"{sb}/rest/v1/llm_leads",
                headers=_sb_headers("resolution=ignore-duplicates,return=minimal"),
                json=valid,
            )
            if resp.status_code in (200, 201, 204):
                queued = len(valid)
            else:
                logger.warning("llm_leads insert %d: %s", resp.status_code, resp.text[:150])
    logger.info("llm_extractor: found=%d queued=%d dropped=%d src=%s",
                len(persons), queued, dropped, url or "text")
    return {"found": len(persons), "queued": queued, "dropped": dropped}


async def approve_lead(lead_id: str, app: Any) -> dict:
    """Promote a reviewed lead from llm_leads into the canonical reports table
    (via the normal scraper upsert path) and mark it approved."""
    from scrapers.base import BaseScraper  # reuse upsert + embedding path indirectly
    sb = settings.supabase_url.rstrip("/")
    async with httpx.AsyncClient(timeout=20) as cl:
        r = await cl.get(f"{sb}/rest/v1/llm_leads",
                         headers=_sb_headers("return=representation"),
                         params={"id": f"eq.{lead_id}", "select": "*"})
        rows = r.json() if r.status_code == 200 else []
        if not rows:
            return {"ok": False, "error": "lead not found"}
        lead = rows[0]
        row = {
            "source": "llm_approved",
            "source_url": f"llm:{lead_id}",
            "kind": lead.get("kind") or "missing",
            "full_name": lead["full_name"],
            "age": lead.get("age"),
            "last_seen_location": lead.get("location"),
            "raw_data": {"approved_from_lead": lead_id, "confidence": lead.get("confidence")},
        }
        ins = await cl.post(f"{sb}/rest/v1/reports",
                            headers=_sb_headers("resolution=merge-duplicates,return=representation"),
                            params={"on_conflict": "source,source_url"}, json=row)
        if ins.status_code not in (200, 201):
            return {"ok": False, "error": f"report insert {ins.status_code}"}
        report_id = ins.json()[0]["id"]
        await cl.patch(f"{sb}/rest/v1/llm_leads", headers=_sb_headers(),
                       params={"id": f"eq.{lead_id}"},
                       json={"review_status": "approved", "reviewed_at": "now()"})
    # Embed + match the newly approved report
    try:
        from consolidation_pipeline import embed_and_match_report
        import asyncio
        asyncio.create_task(embed_and_match_report(report_id, {
            "full_name": row["full_name"], "age": row["age"],
            "last_seen_location": row["last_seen_location"],
            "distinguishing_marks": None, "kind": row["kind"],
        }, app))
    except Exception as exc:
        logger.warning("approve_lead embed failed: %s", exc)
    return {"ok": True, "report_id": report_id}
