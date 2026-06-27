# Reune VE

Missing persons matching system for Venezuela. WhatsApp intake via Base44 agent, face recognition via InsightFace, text similarity via SentenceTransformer, storage in Supabase (pgvector).

## Stack

- FastAPI + Uvicorn
- InsightFace buffalo_sc (512-dim face embeddings, CPU)
- SentenceTransformer paraphrase-multilingual-mpnet-base-v2 (768-dim text)
- Supabase (Postgres + pgvector)
- Base44 WhatsApp agent (polling loop, 30s interval)
- APScheduler (scraper jobs every 5min poll / 1hr full sweep)

## Setup

```bash
cp .env.example .env
# Fill in SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, BASE44_AGENT_ID, BASE44_API_KEY
docker compose up -d --build
```

## Key endpoints

| Endpoint | Description |
|----------|-------------|
| GET /health | Service health + model status |
| POST /hooks/base44 | Base44 webhook (requires BASE44_WEBHOOK_SECRET) |
| POST /admin/bulk_import | Bulk import from /crisis_data (requires X-Admin-Key) |

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for data flow, schema, and matching logic.

## Human-in-the-loop

All matches go into `matches` table with `status = "pending"`. The bot sends "posible coincidencia, en verificacion" to both parties. A human must confirm via Supabase dashboard before any definitive information is shared. The bot never says "encontrado" or communicates a death.

## Security constraints

- All Supabase writes use the service role key (never anon key)
- Bot never communicates a death or confirms a match without human review
- UI messages always say "posible coincidencia, en verificacion" - never "encontrado"
- pnp_cedulas.db excluded - legal/privacy risk
- ANTHROPIC_API_KEY lives in .env on server only, never committed
