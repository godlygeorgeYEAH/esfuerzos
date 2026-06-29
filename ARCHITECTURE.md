# Arquitectura · Reúne VE

**Versión:** 3.0 · **Fecha:** 2026-06-29 · **Estado:** Producción

> La documentación de arquitectura **completa y vigente** vive en
> **[`README.md`](README.md)** (visión general, diagramas de flujo Mermaid, pipeline de
> matching, fuentes/scrapers, dashboard, schedulers, cadena LLM, seguridad, deploy/ops)
> y el modelo de datos del esquema **vivo** en **[`DATA-MODEL.md`](DATA-MODEL.md)**.
> El DDL de referencia (extraído de la DB viva) está en
> [`migrations/000_current_schema_reference.sql`](migrations/000_current_schema_reference.sql).
>
> Este archivo era el doc de arquitectura previo (v2). Describía **Base44 Superagent** como
> transporte primario, lo cual **ya no aplica**: Base44 fue **removido** del proyecto. Se
> reemplazó por este stub para evitar drift y referencias obsoletas. Ante cualquier duda,
> manda el README + DATA-MODEL.

## Resumen de una línea

Un solo proceso FastAPI (`reune-ve-api`, :8080) que recibe reportes por **Telegram**
(`telegram_intake.py`, long-polling, bot **@Reuneve_bot**) con extracción por **Groq** y
cadena de fallback (`llm_client.py`), corre 14+ scrapers + pipelines de embeddings/cara/cédula/dedup
en APScheduler, persiste en **Supabase** (Postgres + pgvector), y expone un **dashboard de
aprobación humana** (`/admin/dashboard`) + un **buscador/analizador de fotos** (`/admin/search-ui`)
detrás de `ADMIN_KEY` por túnel SSH.

> **Cutover 2026-06-29:** el canal de intake migró de **WhatsApp (WAHA)** a **Telegram**. WAHA fue
> apagado (contenedor `reune_waha` removido, servicio fuera del compose). El core de intake es
> agnóstico de canal (`waha_intake.py` reusado por el adaptador de Telegram vía un dispatcher de envío).

## Hechos clave (la fuente de verdad es el README)

- **Canal único en producción:** Telegram (bot @Reuneve_bot) + Groq. WAHA/WhatsApp apagado en el
  cutover 2026-06-29. No hay Base44. El código `api/bot/*` está deprecado y no se ejecuta.
- **Contenedor:** `reune-ve-api` (`docker-compose.yml`, `mem_limit: 2g`, repo bind-mounted en `/app`).
  Deploy = `git pull` + `docker restart`; cambio de dependencias = `docker compose up --build`.
- **DB viva = fuente de verdad.** Las migraciones están drifted. Enums verificados en vivo:
  `person_state` = `unknown|alive|injured|deceased`; `match_status` = `pending|confirmed|dismissed|found`
  (ojo: **no** existen `found`/`discharged` en person_state ni `rejected` en match_status).
- **Confirmación humana obligatoria** antes de notificar a una familia: el notifier solo dispara
  sobre `status=confirmed`.

Para todo lo demás (diagramas, thresholds de matching, lista de scrapers, endpoints, seguridad,
recuperación de WAHA), ver **[`README.md`](README.md)**.
