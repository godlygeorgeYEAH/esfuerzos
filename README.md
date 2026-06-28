# Reúne VE

Bot de reunificación familiar post-crisis para Venezuela. Recibe reportes de personas desaparecidas y personas encontradas vía WhatsApp, cruza los datos con fuentes externas (hospitales, redes sociales, organizaciones solidarias) y detecta coincidencias por texto y reconocimiento facial.

## Arquitectura

```
WhatsApp → WAHA (self-hosted) → POST /webhook/waha
                                        │
                               FastAPI (reune-api)
                               ├── Groq LLaMA 3.3 70B  — extrae nombre/edad/tipo del chat
                               ├── SentenceTransformer  — embedding de texto (768 dim)
                               ├── InsightFace buffalo_sc — embedding facial (512 dim)
                               ├── APScheduler  — scrapers cada 5 min (poll) / 1 h (full)
                               └── pgvector RPC — match semántico + facial
                                        │
                                  Supabase (Postgres + pgvector)
                                  tablas: reports, photos, matches
```

**Scrapers activos** (background, automáticos):
`reconexion`, `sos_venezuela`, `venezreporta` (API REST), `terremotove`, `google_drive_hospital`,
`red_solidaria_venezuela`, `localizados_venezuela`, `venezuela_te_busca`, `sos_laguaira`,
`pacientes_terremoto`, `tuayudave`, `hospitales_ve`*, `redayuda_ve`*

\* Solo se activan si la clave externa correspondiente está configurada (ver `.env.example`).

## Flujo del bot

1. Usuario envía mensaje describiendo a una persona desaparecida o encontrada.
2. WAHA envía el evento a `POST /webhook/waha`.
3. Groq extrae: nombre, edad, ubicación, tipo (`missing`/`found`), descripción.
4. Cuando el reporte está completo, se inserta en `reports` con `source=waha_whatsapp`.
5. Se corre búsqueda sincrónica por nombre (ILIKE) y se informa candidatos al usuario.
6. En background: embedding de texto + match vectorial contra toda la tabla vía pgvector.
7. Si el usuario envía foto: embedding facial + búsqueda visual contra `reports` del tipo opuesto.

## Requisitos

- Docker y Docker Compose
- Proyecto Supabase con pgvector habilitado y migraciones aplicadas (`migrations/`)
- Instancia WAHA corriendo con número de WhatsApp conectado y webhook apuntando a este servicio
- API key de Groq (modelo: `llama-3.3-70b-versatile`)

## Correr localmente

```bash
git clone https://github.com/godlygeorgeYEAH/esfuerzos.git
cd esfuerzos

cp .env.example .env
# Edita .env con tus valores reales

# Red compartida entre reune-api y waha (solo la primera vez)
docker network create reune_default

# Levanta los servicios
docker-compose up -d

# Logs en tiempo real
docker-compose logs -f reune-api

# Health check
curl http://localhost:8080/health
```

Primer arranque: SentenceTransformer e InsightFace se descargan automáticamente (~1.5 GB).
La API tarda ~60 segundos en estar lista.

## Endpoints

| Endpoint | Método | Auth | Descripción |
|---|---|---|---|
| `GET /health` | GET | — | Estado de WAHA, Supabase, modelos y scrapers |
| `POST /webhook/waha` | POST | HMAC opcional | Webhook entrante de WAHA |
| `POST /admin/bulk_import` | POST | X-Admin-Key | Importa datos históricos en batch |
| `POST /admin/consolidate` | POST | X-Admin-Key | Corre embedding + match vectorial/facial |

## Variables de entorno

| Variable | Requerida | Descripción |
|---|---|---|
| `SUPABASE_URL` | Sí | URL del proyecto Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Sí | Service role key (bypasa RLS) |
| `LLM_API_KEY` | Sí | Groq API key para el bot de WhatsApp |
| `WAHA_URL` | No | URL interna de WAHA (default: `http://waha:3000`) |
| `WAHA_WEBHOOK_SECRET` | No | Secreto HMAC para validar webhooks de WAHA |
| `ADMIN_KEY` | No | Protege `/admin/*`. Sin valor: endpoints abiertos |

Ver `.env.example` para la lista completa incluyendo thresholds de matching y scrapers opcionales.

## Deploy en VPS

El código está montado por bind-mount (`.:/app` en docker-compose), así que un
`git pull` actualiza el código en vivo. Solo hace falta reiniciar para recargar
los módulos de Python (no hace falta `build` salvo que cambien dependencias).

```bash
ssh root@13.140.166.72
cd /root/esfuerzos
git pull origin main
docker restart reune-ve-api
docker logs -f reune-ve-api --tail 50
curl http://localhost:8080/health
```

## Migraciones de base de datos

Aplica en orden numérico desde el Supabase SQL Editor. La base en producción ya
las tiene aplicadas; esta lista es para reconstruir el esquema desde cero:

```
migrations/001_initial.sql
migrations/002_match_functions.sql   ← tablas reports/photos/matches + RPC pgvector
migrations/002_unique_constraint.sql ← UNIQUE(source, source_url) para upsert
migrations/003_face_rpc.sql
migrations/004_conversation_ids.sql
migrations/005_fix_face_rpc.sql      ← RPC match_reports_by_face
migrations/create_external_leads.sql
migrations/006_drop_crisis_tables.sql ← elimina tablas del viejo Crisis VE
migrations/007_optimize_ivfflat.sql
migrations/008_ivfflat_probes.sql
```

## Notas técnicas

- `terremotove` inserta filas con prefijo `EVENTO:` (localizaciones de daños, no personas). El pipeline de consolidación las omite.
- Estado de conversación en memoria (`_conv_state`). Un reinicio del contenedor borra las sesiones activas en curso (el reporte ya persistido en `reports` no se pierde).
- `dedup_pipeline.py` corre cada 4h: agrupa reportes casi-duplicados entre scrapers (mismo nombre/ubicación con variaciones) y marca los no-canónicos en `raw_data.possible_duplicate_of`. Nunca borra filas.
- Las fotos de WhatsApp llegan vía WAHA por HTTP interno (`http://waha:3000`); el pipeline facial confía explícitamente en ese host y descarga con `X-Api-Key`.
