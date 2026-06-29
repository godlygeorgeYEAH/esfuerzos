# Arquitectura - Reune VE

**Version:** 2.0
**Fecha:** 2026-06-29
**Estado:** Producción

> **DOC MAESTRO:** la documentación completa y actual (arquitectura, flujos con diagramas, matching,
> fuentes, dashboard, operaciones) vive en [`README.md`](README.md). Este archivo conserva detalle
> histórico; ante conflicto, manda el README + `DATA-MODEL.md`.
>
> **ACTUALIZACIÓN 2026-06-29 (corrige drift).** El canal primario es **WAHA WhatsApp + Groq**,
> NO Base44. Base44 fue **removido** del proyecto. El bot WAHA (`waha_intake.py`) SÍ está montado
> en `main.py` y es el único canal en producción. El contenedor se llama **`reune-ve-api`**.
> Cambios recientes: (1) **cadena de fallback LLM** (`llm_client.py`: Groq → Cerebras gpt-oss-120b →
> OpenRouter) ante el rate-limit de Groq; (2) **vista `canonical_reports`** (base deduplicada,
> migración 014); (3) **dashboard de aprobación humana** (`/admin/dashboard`, túnel SSH); (4) scraper
> **`hospital_consolidado`** (xlsx maestro de pacientes hospital/refugio, con cédula → exact match).
> El esquema vivo está en **`DATA-MODEL.md`**; las migraciones drifted, ver
> `migrations/000_current_schema_reference.sql`. Las secciones abajo pueden contener detalles
> históricos (Base44) que ya no aplican.

---

## 1. Descripción general

Reune VE es el módulo de reunificación de personas de la plataforma de respuesta al sismo Venezuela 2026. Mientras Crisis VE atiende la evaluación de daños estructurales y el registro de sobrevivientes, Reune VE resuelve el problema de las personas desaparecidas: correlaciona reportes de "busco a X" con reportes de "encontré a X" provenientes de múltiples fuentes (Base44 Superagent, redes sociales, hospitales, organizaciones de ayuda) usando embeddings de texto y reconocimiento facial.

El sistema opera en un solo contenedor Docker (`reune-ve-api`, puerto 8080) sobre un VPS. **Advertencia:** los modelos ML en reposo consumen ~620 MB (InsightFace ~200 MB + SentenceTransformer ~420 MB), lo que excede el límite declarado en `docker-compose.yml`. Esta contradicción debe resolverse: o bien el límite se eleva en el compose del droplet de producción, o los modelos no pueden cargarse simultáneamente bajo esa configuración. Ver Sección 7.

No hay microservicios separados: todo el pipeline de ML, los scrapers, el canal conversacional y los consumidores de webhooks corren dentro del mismo proceso Python gestionados por APScheduler y asyncio.

El transporte primario y único en producción es **WAHA WhatsApp** (`waha_intake.py`, webhook `POST /webhook/waha`, firma HMAC `X-Webhook-Hmac` sha512) con **Groq** (llama-3.3-70b) para la extracción conversacional, respaldado por la cadena de fallback de `llm_client.py`. Base44 Superagent fue removido. El código histórico `api/bot/*` está deprecado y no se ejecuta.

### Filosofía de diseño

1. **Un solo proceso, múltiples roles.** El costo de operar servicios separados (coordinación de red, latencia IPC, configuración adicional) no se justifica para la carga esperada. FastAPI + asyncio + APScheduler cubren todos los roles concurrentes sin overhead de infraestructura.

2. **Supabase como capa de persistencia y búsqueda vectorial.** pgvector en Supabase elimina la necesidad de un servicio de embeddings externo (Pinecone, Weaviate). Las RPCs de Postgres hacen la búsqueda 1:N directamente en la base de datos.

3. **ML en CPU, sin GPU.** InsightFace buffalo_sc y SentenceTransformer corren en CPU. La precisión es suficiente para el caso de uso; el costo de GPU en producción no está justificado para el volumen de crisis.

4. **Ingesta por múltiples canales, esquema unificado.** WAHA WhatsApp (canal conversacional activo), ~13 scrapers autónomos (incluido `hospital_consolidado`, el xlsx maestro de hospitales/refugios) y el panel LLM (`llm_leads`, con aprobación humana) convergen todos en la tabla `reports`. El canal de origen queda en `source` y el matching es canal-agnóstico.

---

## 2. Componentes

### 2.1 API FastAPI (`main.py`)

El entry point del sistema. Registra middlewares, monta routers, carga modelos ML en el lifespan y arranca el scheduler.

**Middlewares activos:**
- `CORSMiddleware`: `allow_origins=['*']` hardcodeado en `main.py`; no existe variable de entorno que lo configure
- `SlowAPIMiddleware`: rate limit de 60 solicitudes por minuto por IP remota

**Endpoints registrados:**

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/health` | Retorna `ok`, `base44` (bool), `supabase` (bool), `text_model` (bool), `face_model` (bool), `scrapers` (lista de nombres de scrapers activos) |
| POST | `/admin/bulk_import` | Importación batch manual; **actualmente sin autenticación** (hallazgo de seguridad pendiente) |
| * | Rutas Base44 | Montados via `base44_router` (transporte primario activo) |

`waha_router` (`api/bot/webhook_router.py`) existe en el código pero **no está montado en `main.py`**; el path WAHA no está activo.

**Secuencia de startup (lifespan):**

1. Carga SentenceTransformer (`paraphrase-multilingual-mpnet-base-v2`) en `app.state.text_model`
2. Carga InsightFace FaceAnalysis `buffalo_sc` en CPU en `app.state.face_model`
3. Construye el diccionario de scrapers via `_make_scrapers()`
4. Crea `AsyncIOScheduler` (UTC) y registra dos jobs por scraper: poll cada 300 s, full sweep cada 3600 s
5. Arranca el scheduler
6. Lanza tres tareas asyncio en background: `_startup_sweep` (sweep completo inmediato), `_register_base44_webhook` (registro idempotente del webhook con Base44), `run_polling_loop` (loop de polling Base44)

**app.state al terminar el startup:**

```
text_model            SentenceTransformer   embeddings de texto 768-dim
face_model            FaceAnalysis          embeddings faciales 512-dim, CPU
scrapers              dict                  instancias de scraper activas
scheduler             AsyncIOScheduler      jobs de scraping
supabase_url          str                   URL del proyecto Supabase
supabase_service_key  str                   clave service role
limiter               Limiter               SlowAPI, 60 req/min por IP
```

---

### 2.2 Canal primario: Base44 Superagent (`base44_webhook_router.py`, `base44_poller.py`)

Base44 es la plataforma no-code que expone el Superagent conversacional activo en producción. El Superagent conduce la conversación con el usuario (vía WhatsApp u otros canales de Base44), extrae los datos estructurados de personas buscadas o encontradas, y notifica a Reune VE via webhook o mediante polling activo.

**Dos mecanismos de integración:**

- **Webhook:** Base44 envía eventos a Reune VE cuando el Superagent completa una conversación. El router valida el secreto `BASE44_WEBHOOK_SECRET` y procesa el payload con los datos estructurados (nombre, edad, última ubicación, tipo de reporte, foto si aplica).
- **Polling:** `run_polling_loop` consulta activamente la API de Base44 con `BASE44_API_KEY` y `BASE44_AGENT_ID` para detectar conversaciones nuevas no notificadas via webhook. Es un fallback contra pérdida de webhooks.

La tabla `conversation_ids` guarda el mapeo entre `conv_id` de Base44, el número de teléfono y el `report_id` generado, junto con `last_seen_at` para saber hasta dónde se ha procesado.

---

### 2.3 Canal secundario (no registrado): WAHA + BotState (`api/bot/webhook_router.py`, `flows.py`, `sessions.py`)

El bot de WhatsApp via WAHA existe como código en `api/bot/` pero **no está montado en `main.py` y por lo tanto no está activo en producción**. Es un camino secundario sin cablear.

El bot implementa una **máquina de estados determinística (`BotState`)**, no un LLM. `flows.py` define las transiciones de estado y los mensajes de respuesta para cada paso del flujo de reporte; `sessions.py` gestiona el estado de conversación por número de teléfono. No hay llamadas a ningún modelo de lenguaje ni extracción via LLM.

WAHA (WhatsApp HTTP API) sería el transporte para este bot: recibiría mensajes del número registrado y haría POST al webhook. Para activar este path: (1) montar `waha_router` en `main.py`, (2) desplegar WAHA en el droplet o en red accesible, (3) configurar las variables `WAHA_URL`, `WAHA_API_KEY`, `WAHA_WEBHOOK_SECRET`, `WAHA_SESSION`.

---

### 2.4 Pipeline facial (`face_pipeline.py`)

Procesa fotos de personas desaparecidas y genera embeddings faciales para matching 1:N contra la base de datos.

**Modelos:**
- InsightFace `FaceAnalysis("buffalo_sc")`, `CPUExecutionProvider`
- Embeddings: 512 dimensiones por cara
- Gate de calidad: el detector InsightFace aplica internamente `det_score >= 0.5` como `det_thresh` por defecto en `prepare()`; no hay filtro a nivel de aplicación sobre este score. `embed_photo_from_url` selecciona `best = max(faces, key=det_score)` sin descartar caras por umbral.

**Proceso por foto:**

1. Descarga de la imagen via `httpx.get(photo_url)`. No hay validación SSRF; el URL se consume directamente.
2. Extracción de embedding facial con InsightFace.
3. Llamada RPC `match_reports_by_face` a Supabase para búsqueda 1:N por similitud coseno; recupera hasta 10 candidatos.
4. Si ningún candidato supera `FACE_MATCH_THRESHOLD (0.50)`, termina sin match.
5. Para cada candidato que supera el umbral: `combined_score = face_score` (no hay señal de texto disponible en el momento de ingesta de foto; `text_score = 0.0`). El gate activo de inserción es `face_score >= FACE_MATCH_THRESHOLD (0.50)`. `COMBINED_MATCH_THRESHOLD (0.65)` está definido en config pero **no se aplica como gate en ninguna rama del código actual**. Es configuración muerta.
6. Si `face_score >= FACE_MATCH_THRESHOLD (0.50)`, se escribe una fila en `matches` y se retorna el `match_id`.

**Bug conocido (falla en runtime):** `_search_face_matches` inserta en `matches` con las claves `missing_id` / `found_id`, pero las columnas reales de la tabla son `report_missing_id` / `report_found_id` (migración 002). El insert falla en runtime; la tabla `matches` nunca recibe filas mientras este bug exista. Corrección: renombrar las claves del dict de inserción en `face_pipeline.py`.

**Thresholds (todos configurables via env):**

| Variable | Default | Descripción |
|----------|---------|-------------|
| `FACE_MATCH_THRESHOLD` | 0.50 | Gate activo: score facial mínimo para insertar en `matches` |
| `TEXT_MATCH_THRESHOLD` | 0.75 | Score de texto mínimo (paths futuros de matching por texto) |
| `COMBINED_MATCH_THRESHOLD` | 0.65 | Definido en config; no aplicado como gate actualmente (configuración muerta) |
| `FACE_WEIGHT` | 0.35 | Peso facial en blend ponderado (planificado, no activo) |
| `TEXT_WEIGHT` | 0.65 | Peso textual en blend ponderado (planificado, no activo) |

---

### 2.5 Scrapers (`scrapers/`)

El directorio contiene 8 archivos de scraper, pero solo 7 están registrados en el orquestador: 5 siempre activos y 2 condicionales según variables de entorno. `internal_source.py` existe como dead code y no se importa ni instancia en ningún path activo; por lo tanto no ingiere datos.

Todos los scrapers registrados comparten la interfaz de `base.py` y producen filas para la tabla `reports`.

**Estado de scrapers:**

| Scraper | Activo por defecto | Condición |
|---------|-------------------|-----------|
| `reconexion` | Sí | Siempre |
| `sos_venezuela` | Sí | Siempre |
| `venezreporta` | Sí | Siempre |
| `terremotove` | Sí | Siempre |
| `google_drive_hospital` | Sí | Siempre |
| `hospitales_ve` | No | Requiere `HOSPITALES_ANON_KEY` no vacío |
| `redayuda_ve` | No | Requiere `REDAYUDA_ANON_KEY` no vacío |
| `internal_source` | No | Dead code; nunca se registra en el orquestador |

**Frecuencia:**
- Poll incremental: cada 300 s (5 min)
- Full sweep: cada 3600 s (1 hora)
- Al startup: full sweep inmediato antes de que el scheduler tome el control

**Ingestion idempotente:** La columna `source_url` tiene un UNIQUE constraint sobre `(source, source_url)`. Los upserts usan `ignore-duplicates`, lo que permite reejecutar el mismo scraper sin duplicar filas.

---

## 3. Flujo de datos - Base44 a notificación de match

El flujo completo desde que alguien reporta una persona via el Superagent de Base44 hasta que se genera una posible coincidencia:

```
1. Usuario chatea con el Superagent de Base44 (WhatsApp u otro canal via la plataforma)
2. El Superagent conduce la conversación y extrae datos estructurados

3. Al completar la conversación, Base44 hace POST a /hooks/base44 en crisis-ve
   (fallback: run_polling_loop detecta conversaciones nuevas si el webhook no llega)
4. El endpoint valida BASE44_WEBHOOK_SECRET y devuelve {"ok": true} inmediatamente
5. BackgroundTask procesa el payload

6. [Si el payload incluye media (foto)]
   6a. Se obtiene el report_id del reporte en curso
   6b. Se inserta fila en tabla photos (ignore-duplicates)
   6c. process_photo_for_report(report_id, media_url, app):
       6c-i.  Descarga imagen via httpx.get(media_url); sin validación SSRF
       6c-ii. InsightFace extrae embedding 512-dim; el umbral det_score >= 0.5 es el
              default interno del detector InsightFace (det_thresh en prepare()), no un
              filtro de aplicacion. Se selecciona best = max(faces, key=det_score).
       6c-iii. Supabase RPC match_reports_by_face: coseno 1:N, top-10 candidatos
       6c-iv.  Para cada candidato con face_score >= FACE_MATCH_THRESHOLD (0.50):
               combined_score = face_score  (no hay texto disponible en este momento;
               COMBINED_MATCH_THRESHOLD 0.65 está definido pero no se aplica como gate)
       6c-v.   Si face_score >= 0.50: intenta INSERT en matches (status=pending)
               [Bug: el INSERT usa claves missing_id/found_id; las columnas son
                report_missing_id/report_found_id; falla en runtime]
   6d. Si match_id existe: caso disponible para revisión de case worker

7. [Si report_ready=True en el payload de Base44]
   7a. Upsert en tabla reports con merge-duplicates
       kind: "missing" o "found"
       text_embedding: SentenceTransformer 768-dim sobre descripción textual
   7b. Supabase pgvector hace disponible el embedding para búsquedas semánticas

8. [Matching asincrónico por scrapers - flujo paralelo]
   8a. APScheduler cada 5 min ejecuta poll() en cada scraper activo
   8b. Nuevas filas de reports quedan disponibles para el pipeline de matching
   8c. El algoritmo combina similitud coseno de text_embedding (768-dim)
       y face_embedding (512-dim) para correlacionar reportes de búsqueda con hallazgos

9. Cuando un case worker verifica un match, notifica al familiar
   (flujo manual actualmente, pendiente de automatización)
```

---

## 4. Schema de Supabase

Todas las tablas usan pgvector para embeddings. El cliente de backend usa siempre la `SUPABASE_SERVICE_ROLE_KEY` para bypasear RLS.

### `damage_reports` (migración 001)

Reportes de daño estructural del módulo Crisis VE (Inspector de Grietas).

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID PK | Identificador del reporte |
| `state` | text | Estado venezolano |
| `lat` | float | Latitud GPS |
| `lng` | float | Longitud GPS |
| `risk_level` | enum | BAJO / MEDIO / ALTO |
| `fema_category` | int (1-5) | Clasificación de daño estructural FEMA |
| `damage_type` | text | Tipo de daño (grietas, derrumbe, etc.) |
| `ai_analysis` | text | Análisis generado por Claude Vision |
| `verified` | bool | Verificado por moderador |
| `false_report` | bool | Marcado como falso positivo |

### `safe_checkins` (migración 001)

Registros del módulo "Estoy Bien".

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID PK | Identificador |
| `created_at` | timestamptz | Momento del checkin |

### `reports` (migración 002)

Tabla central de Reune VE. Agrega reportes de personas buscadas y encontradas de todos los canales.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID PK | Identificador del reporte |
| `kind` | enum | `missing` o `found` |
| `source` | text | Canal de origen (base44, waha, reconexion, sos_venezuela, etc.) |
| `source_url` | text | URL original del reporte en la fuente externa |
| `full_name` | text | Nombre completo de la persona |
| `age` | int | Edad aproximada |
| `last_seen_location` | text | Último lugar conocido |
| `distinguishing_marks` | text | Señas particulares (lunares, cicatrices, tatuajes) |
| `clothing` | text | Ropa que vestía cuando fue visto por última vez |
| `text_embedding` | vector(768) | Embedding semántico del reporte completo |
| `dedup_group_id` | UUID | Agrupación lógica de reportes relacionados (deduplicación) |

Constraint: `UNIQUE(source, source_url)` - clave de idempotencia para ingestion por scrapers.

### `photos` (migración 002)

Fotos asociadas a reportes, con embeddings faciales.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID PK | Identificador |
| `report_id` | UUID FK -> reports | Reporte al que pertenece la foto |
| `face_embedding` | vector(512) | Embedding InsightFace buffalo_sc |
| `storage_path` | text | URL de la foto en Supabase Storage o fuente externa |
| `created_at` | timestamptz | Fecha de ingesta |

RPC `match_reports_by_face` (migración 003): recibe `query_embedding` (512-dim), `query_kind` (filtra por `r.kind = query_kind`), `match_threshold` y `match_count`. Calcula similitud coseno contra todos los embeddings en `photos` donde `quality_ok = true` y retorna top-N candidatos con su score. Llamada directamente por `face_pipeline.py`.

Retención: `PHOTO_RETENTION_DAYS` (default 30) controla purga automática de fotos.

### `matches` (migración 002)

Pares candidatos de reunificación pendientes de revisión humana. Generados automáticamente por el pipeline facial; el status solo avanza por revisión explícita.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID PK | Identificador del match |
| `created_at` | timestamptz | Momento de creación |
| `report_missing_id` | UUID FK -> reports | Reporte de tipo `missing` |
| `report_found_id` | UUID FK -> reports | Reporte de tipo `found` |
| `face_score` | float8 | Similitud coseno facial (0-1) |
| `text_score` | float8 | Similitud coseno textual (0-1; 0.0 cuando no hay señal de texto) |
| `combined_score` | float8 | Actualmente igual a `face_score` (blend ponderado no activo) |
| `status` | text | `pending` / `confirmed` / `rejected` (default `pending`) |
| `reviewed_by` | text | Identificador del revisor que resolvió el match |
| `reviewed_at` | timestamptz | Momento de la revisión |

Constraint: `UNIQUE(report_found_id, report_missing_id)` - evita duplicar el mismo par candidato.

**Bug conocido:** `face_pipeline._search_face_matches` inserta con claves `missing_id` / `found_id`; las columnas reales son `report_missing_id` / `report_found_id`. El insert falla en runtime; esta tabla permanece vacía mientras el bug no se corrija.

### `conversation_ids` (migración 004)

Mapeo entre conversaciones Base44 y reportes generados.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `conv_id` | text PK | ID de conversación en Base44 |
| `phone` | text | Número de teléfono del usuario |
| `report_id` | UUID FK -> reports | Reporte asociado |
| `last_seen_at` | timestamptz | Último evento procesado del conversation polling |

### `external_leads`

Resultados de búsqueda exploratoria en fuentes externas (hospitales, redes de ayuda). Uno por consulta por fuente.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID PK | Identificador |
| `report_id` | UUID FK -> reports | Reporte que originó la búsqueda |
| `source` | text | Fuente externa (hospitales_ve, redayuda_ve, etc.) |
| `full_name` | text | Nombre encontrado en la fuente |
| `score` | float (0-1) | Score compuesto del match |
| `name_similarity` | float | Similitud de nombre específicamente (rapidfuzz) |
| `kind` | text | missing / found en la fuente externa |
| `raw_data` | JSONB | Datos crudos de la fuente |
| `notified_at` | timestamptz | Cuando se envió el seguimiento via WhatsApp |

---

## 5. Red Docker

El `docker-compose.yml` del repositorio **no define un bloque `networks`**. El contenedor `crisis-ve` opera en la red bridge por defecto que Docker Compose crea automáticamente para el proyecto. No hay redes externas declaradas ni comunicación directa con un contenedor WAHA (el path WAHA no está activo en producción).

Si en el futuro se agregan servicios al mismo compose file (worker de matching, Redis, WAHA), se comunicarán por la red default de Docker Compose sin configuración adicional mientras estén en el mismo archivo.

`mem_limit: 256m` según `docker-compose.yml` y `CLAUDE.md`. Ver limitación crítica de memoria en Sección 7.

---

## 6. Decisiones técnicas

### InsightFace buffalo_sc sobre alternativas (DeepFace, dlib, face_recognition)

buffalo_sc ofrece el mejor trade-off entre precisión en caras hispánicas y velocidad de inferencia en CPU. dlib con HOG es más rápido pero significativamente menos preciso en imágenes de baja calidad (fotos de WhatsApp, comprimidas, ángulos no frontales). DeepFace tiene mejor precisión general pero sus dependencias (TensorFlow) triplican el tamaño de la imagen Docker y el tiempo de startup. buffalo_sc en ONNX Runtime carga en segundos y genera embeddings 512-dim de calidad suficiente para el umbral 0.50 que usa el sistema.

El modelo se cachea en el volumen nombrado `insightface_cache` montado en `/root/.insightface`. Sin este volumen, el contenedor descargaría el modelo (~100 MB) en cada restart, lo que rompería el startup en ambientes con latencia de red alta.

### Base44 Superagent como transporte primario

Base44 elimina la necesidad de operar infraestructura conversacional propia durante la ventana de crisis. El Superagent maneja el flujo de preguntas y respuestas, la validación de inputs y el formateo de los datos extraídos; Reune VE solo consume el resultado estructurado via webhook. Esto reduce la superficie de fallo operacional en las primeras 72-168 horas post-desastre.

El tradeoff es dependencia de una plataforma externa y menos control sobre el flujo de conversación. El bot WAHA con máquina de estados (`api/bot/`) es el camino hacia la independencia de Base44, pero requiere cablearlo en `main.py` y desplegar WAHA en el droplet.

### WAHA sobre la API oficial de Meta WhatsApp Business (canal secundario planificado)

La API oficial de Meta requiere aprobación de cuenta de business verificada, número de teléfono registrado por Meta, y un proceso de aprobación que puede tomar días. En una ventana de 72-168 horas post-desastre, eso no es viable.

WAHA opera enlazando un número de WhatsApp existente via el protocolo de WhatsApp Web (igual que el cliente de escritorio). No requiere aprobación de Meta. El tradeoff es que técnicamente viola los términos de servicio de WhatsApp, lo que implica riesgo de ban del número. Para un sistema de crisis temporal ese riesgo es aceptable frente a la imposibilidad práctica de obtener acceso oficial a tiempo.

### `pip install --prefer-binary`

Los paquetes ML (insightface, onnxruntime, opencv-python-headless) tienen extensiones C/C++ que requieren compilación si no hay wheel binaria disponible para la plataforma. En python:3.11-slim (Debian bookworm, amd64), estas wheels binarias existen en PyPI. Sin `--prefer-binary`, pip puede intentar compilar desde fuente si detecta una versión de wheel incompatible, lo que falla en la imagen slim por ausencia de compiladores o tarda decenas de minutos. `--prefer-binary` fuerza la descarga de wheel precompilada aunque exista una versión de source más reciente, haciendo el build reproducible y rápido.

---

## 7. Limitaciones conocidas

### Límite de memoria vs. huella ML (crítico)

`docker-compose.yml` establece `mem_limit: 256m`. Los modelos ML consumen aproximadamente:
- InsightFace buffalo_sc: ~200 MB
- SentenceTransformer paraphrase-multilingual-mpnet-base-v2: ~420 MB
- Total ML en reposo: ~620 MB

La huella ML (~620 MB) supera el `mem_limit` declarado (256m). El contenedor sería terminado por OOM al cargar ambos modelos simultáneamente. Resolver: elevar el límite en el compose del droplet (recomendado: mínimo 1.5g para dejar margen operativo al proceso FastAPI y a los scrapers en ejecución paralela), o verificar si el compose en el droplet de producción ya tiene un valor distinto al del repositorio.

Monitorear con: `docker stats crisis-ve --no-stream`

### Bug de inserción en `matches` (falla en runtime)

`face_pipeline._search_face_matches` inserta con claves `missing_id` / `found_id`, pero las columnas de la tabla `matches` son `report_missing_id` / `report_found_id` (migración 002). El insert falla en runtime cada vez que el pipeline encuentra un candidato que supera `FACE_MATCH_THRESHOLD`. La tabla `matches` permanece vacía mientras este bug exista.

Corrección: renombrar las claves del dict de inserción en `face_pipeline.py` a `report_missing_id` / `report_found_id`.

### Bot WAHA no cableado en producción

El código del bot (`api/bot/webhook_router.py`, `flows.py`, `sessions.py`) no está montado en `main.py`. El canal WAHA no procesa mensajes en producción. Ver Sección 2.3 para los pasos de activación.

### Scraper `reconexion` devuelve 403 en algunos endpoints

La fuente Reconexion Venezuela aplica rate limiting y en ciertos endpoints devuelve 403 después de algunas peticiones seguidas. El scraper maneja el error logeando y continuando, pero esas publicaciones no se ingresan en ese ciclo.

Mitigación: el full sweep cada hora reintenta con backoff implícito. No hay implementación de User-Agent rotation ni cookies de sesión.

### `internal_source.py` sin registrar

El archivo `scrapers/internal_source.py` existe en el directorio pero el orquestador nunca lo importa ni instancia. Es dead code. Su propósito original no está documentado. No remover sin investigar si hay lógica de negocio única ahí que se necesite para otras fuentes.

---

## 8. Variables de entorno (referencia completa)

| Variable | Tipo | Default | Propósito |
|----------|------|---------|-----------|
| `SUPABASE_URL` | str | requerida | URL del proyecto Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | str | requerida | Clave service role; bypasea RLS |
| `WAHA_URL` | str | `http://waha:3000` | Base URL de la API WAHA (canal sin cablear) |
| `WAHA_API_KEY` | str | `""` | Auth key para llamadas a WAHA (canal sin cablear) |
| `WAHA_WEBHOOK_SECRET` | str | `""` | Secreto para validar webhooks entrantes de WAHA (canal sin cablear) |
| `WAHA_SESSION` | str | `"default"` | Nombre de sesión WAHA (canal sin cablear) |
| `BASE44_WEBHOOK_SECRET` | str | `""` | Secreto para validar webhooks de Base44 |
| `BASE44_AGENT_ID` | str | `""` | ID del Superagent en Base44 |
| `BASE44_API_KEY` | str | `""` | Auth key para la API de Base44 |
| `VPS_PUBLIC_URL` | str | `""` | URL pública del VPS; usada para registrar webhooks |
| `FACE_MATCH_THRESHOLD` | float | 0.50 | Gate activo: score facial mínimo para insertar en `matches` |
| `TEXT_MATCH_THRESHOLD` | float | 0.75 | Score textual mínimo (paths futuros de matching por texto) |
| `COMBINED_MATCH_THRESHOLD` | float | 0.65 | Definido en config; no aplicado como gate actualmente (configuración muerta) |
| `FACE_WEIGHT` | float | 0.35 | Peso facial en blend ponderado (planificado, no activo) |
| `TEXT_WEIGHT` | float | 0.65 | Peso textual en blend ponderado (planificado, no activo) |
| `PHOTO_RETENTION_DAYS` | int | 30 | Días antes de purgar fotos |
| `EMBEDDINGS_MODEL` | str | `paraphrase-multilingual-mpnet-base-v2` | Modelo SentenceTransformer |
| `HOSPITALES_ANON_KEY` | str | `""` | Clave para scraper hospitales_ve; vacío = desactivado |
| `REDAYUDA_ANON_KEY` | str | `""` | Clave para scraper redayuda_ve; vacío = desactivado |

---

## 9. Diagrama de componentes (texto)

```
                    Base44 Platform
                    (Superagent)
                         |
               POST /hooks/base44
               (+ polling fallback)
                         |
                +-----------------+
                |   crisis-ve     |
                |   (FastAPI)     |
                |                 |
                | base44_router --+---> Base44 API
                | base44_poller   |     (polling fallback)
                |    |            |
                |    v            |
                | face_pipeline --+---> InsightFace buffalo_sc
                |                 |     (embeddings 512-dim, CPU)
                |                 |
                | APScheduler ----+---> Scrapers (5 activos + 2 condicionales)
                |                 |     reconexion, sos_venezuela, venezreporta
                |                 |     terremotove, google_drive_hospital
                |                 |
                | [sin cablear]   |
                | waha_router ----+---> WAHA (bot BotState, no activo en produccion)
                +-----------------+
                         |
                 Supabase REST API
                         |
                +--------+--------+
                |                 |
           pgvector           Postgres
        (face 512-dim,      (reports, photos,
         text 768-dim)       matches,
                             conversation_ids,
                             damage_reports,
                             external_leads)
```
