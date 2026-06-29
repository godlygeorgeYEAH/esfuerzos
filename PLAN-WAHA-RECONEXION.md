# Plan de implementación — Reconexión de WAHA V1 (sendList) con las mejoras de V2

Plan consolidado y final. Reúne todas las decisiones tomadas durante el análisis de
las dos implementaciones de WAHA del repositorio.

---

## Objetivo

Reactivar la implementación **V1** de WAHA — el bot FSM con `sendList`, 3 flujos por
tipo de usuario, hoy en `esfuerzos/modulos/migration_prox/` — como el bot vivo,
**montado en el proceso raíz** para matching instantáneo, con el estado conversacional
en un **Postgres local durable** (cero carga por-mensaje a Supabase) y el reporte final
integrado a la tabla `reports` que alimenta todo el ecosistema de matching/notificación.

## Contexto: las dos implementaciones

- **V1 (sendList)** — `esfuerzos/modulos/migration_prox/`: app FastAPI independiente.
  FSM con nodos en base de datos (`flow_seeder`), 3 flujos diferenciados
  (familiar / rescatista / hospital), menús interactivos `sendList`, soporte GPS,
  resolución `@lid`, multi-operación, horarios, analytics. Estado en SQLAlchemy.
  Escribe el reporte final en `reunion_reports`.

- **V2 (actual, post commit `020743e`)** — raíz del repo: `waha_intake.py`. Sin FSM,
  conversación LLM-driven en texto plano (solo `sendText`). Agregó: rate limit por
  teléfono, sesión durable (Supabase `waha_sessions`), dedup de webhooks, multi-persona,
  no identificados, búsqueda inline, fallback chain de LLM, y los pipelines batch
  (`notify_pipeline`, `dedup_pipeline`, `face_backfill`). Escribe en `reports`.

Volvemos a V1 por sus flujos interactivos y diferenciados, **preservando** lo que V2
aportó en robustez y conectividad con el ecosistema.

## Decisiones cerradas

1. **Topología**: prox montado en el proceso raíz (`reune-ve-api`). Un solo proceso.
   → matching en proceso, instantáneo.
2. **Estado conversacional**: Postgres **local** (contenedor `db` + volumen), vía
   SQLAlchemy. Nunca toca Supabase. → durable ante reinicios + cero latencia de red +
   descarga a Supabase.
3. **Reporte final**: migrar intake de `reunion_reports` (silo aislado) a **`reports`**
   (tabla del ecosistema). → dedup, face, cross-match y notify proactivo funcionan.
4. **Motor FSM intacto**: como el estado vive en PG local, NO convertimos
   `flow_nodes`/`bot_config`/FAQ a constantes ni cacheamos `blocked_clients`. Prox se
   queda como está; solo cambia a dónde apunta y a dónde escribe el reporte.

## Arquitectura final

```
   WAHA ──webhook──►  UN SOLO PROCESO: reune-ve-api
                        ├─ router prox (FSM + sendList, 3 flujos)
                        ├─ modelos ML en RAM (SentenceTransformer 768d + InsightFace 512d)
                        ├─ APScheduler (scrapers, embeddings, notify, dedup, face_backfill)
                        └─ embed_and_match_report()  ← EN PROCESO, instantáneo
        ┌───────────────────────┴───────────────────────┐
        ▼                                                ▼
  Postgres LOCAL (contenedor db + volumen)        Supabase (REST)
  vía SQLAlchemy — cero tráfico por-mensaje       solo salidas compartidas
  • conversaciones (durable)                      • reports · photos · matches
  • mensajes_conversacion · eventos · clientes    • bot_subscribers
  • flow_nodes · bot_config · FAQ (seed startup)  • hospitales · hospital_listas
```

El Postgres local corre como contenedor aparte (`db`); no consume de los 2 GB del bot/ML.

## Inventario: dónde vive cada tabla

| Tabla | Almacén | Acceso | Patrón |
|---|---|---|---|
| `conversaciones` | PG local | SQLAlchemy | leída/escrita cada mensaje (hot, durable) |
| `mensajes_conversacion`, `eventos_conversacion`, `clientes` | PG local | SQLAlchemy | write-only (logs/registro) |
| `flow_nodes`, `flow_templates`, `operacion_flows`, `operaciones`, `bot_config`, `preguntas_frecuentes` | PG local | SQLAlchemy | seed al arrancar; lectura local barata |
| `blocked_clients` | PG local | SQLAlchemy | check cada mensaje (lectura local) |
| `reports`, `photos`, `matches`, `bot_subscribers` | Supabase | REST | salida; alimenta matching/notify/dashboard |
| `hospitales`, `hospital_listas` | Supabase | REST (ya funciona) | salida hospital |
| ~~`reunion_reports`~~, ~~modelos `Report`/`Photo` de `reporte.py`~~ | — | — | **borrar (muertos)** |

---

## FASE 0 — Fundación de almacenamiento (Postgres local durable)

| # | Acción | Archivo |
|---|---|---|
| 0.1 | Agregar servicio `db` (Postgres) con named volume al compose raíz | `docker-compose.yml` |
| 0.2 | Definir `DATABASE_URL=postgresql+psycopg2://reune:***@db:5432/reune` en `reune-api` | `docker-compose.yml` / `.env` |
| 0.3 | Verificar que el lifespan cree tablas (`Base.metadata.create_all`) y `seed_default_flow` en el PG local | `main.py` (Fase 1.4) |
| 0.4 | Backup del volumen: `pg_dump` 1×/hora a Supabase Storage o disco | script/cron |

**Cierra la feature "sesión durable" sin código de sesión.** Caveat: un named volume
sobrevive `restart`/`down && up`, **no** un `down -v` ni borrado de host → de ahí el
backup 0.4.

---

## FASE 1 — Montar prox en el proceso raíz

| # | Acción | Archivo |
|---|---|---|
| 1.1 | Hacer importable prox: `sys.path.insert(0, "esfuerzos/modulos/migration_prox")` (no hay `app/` en raíz → sin colisión) | `main.py` |
| 1.2 | Quitar el bot viejo: borrar `from waha_intake import router` + `app.include_router(waha_router)` | `main.py` |
| 1.3 | Montar router prox: `from app.routers.webhook import router as bot_router; app.include_router(bot_router, prefix="/webhook")` → sirve `/webhook/waha` | `main.py` |
| 1.4 | Fusionar lifespan: tras cargar modelos → `Base.metadata.create_all(engine)` + `seed_default_flow(db)` + crear `Operacion`/`BotConfig`/`OperacionFlow` idempotentes + `await ensure_default_session()` | `main.py` lifespan |
| 1.5 | Sumar deps de prox al raíz: `sqlalchemy`, `psycopg2-binary`, `openai`, `supabase`, `python-multipart` | `requirements.txt` |
| 1.6 | Repuntar `notify_pipeline`: `from app.services.waha import send_message`; mover `SOURCE_LABELS`/`_source_label` a un módulo propio; borrar `waha_intake.py` y ajustar/retirar `tests/test_e2e_entities.py` | `notify_pipeline.py`, `tests/` |
| 1.7 | `.env` raíz: agregar `DATABASE_URL` + vars de prox (`WAHA_WEBHOOK_URL`, `PHOTO_*`, `DEEPSEEK_*`). `.env` = fuente única | `.env`, `.env.example` |
| 1.8 | Jubilar el compose de prox | `esfuerzos/modulos/migration_prox/docker-compose.yml` |

**Riesgos asumidos (ojos abiertos):** dos `Settings` conviviendo (módulos distintos,
mismo `.env`); un solo proceso (reinicio baja el bot, pero retoma por PG durable);
event-loop compartido con el scheduler (encode de 1 texto corto = ms; jobs pesados ya
usan executor).

---

## FASE 2 — Migrar intake a `reports`

Reescribir `app/core/intake.py::commit_report` para escribir en Supabase `reports`
(no `reunion_reports`):

**Mapeo de columnas:**

| `reunion_reports` (viejo) | → | `reports` (ecosistema) |
|---|---|---|
| `name` | → | `full_name` |
| `age` (texto) | → | `age` (int parseado; `NULL` si no hay) |
| `location` | → | `last_seen_location` |
| `marks`/`notes` | → | `distinguishing_marks` |
| `found_state` | → | `person_state` (enum `alive`/`injured`/`deceased`/`unknown`) |
| — | → | `source = "waha_whatsapp"` |
| — | → | `source_url = "waha:{hash8}:{uuid8}"` único por persona (respeta `UNIQUE(source, source_url)`) |
| — | → | `reporter_wa_hash = sha256(phone)[:32]` |
| — | → | `raw_data` (texto crudo, género, notas, photo_count) |

**Además:**
- **Fotos** → subir al bucket **y** `INSERT` en tabla `photos` (FK `report_id`) para que
  `face_backfill`/face matching las procese. (Hoy solo setea `photo_url`.)
- **`bot_subscribers`** → `INSERT` `{report_id, phone, full_name, kind}`. **Es el enlace
  que `notify_pipeline` necesita para avisar a la familia. Sin esto, el notify proactivo
  queda muerto.**
- **No identificados:** nombre `"desconocido"`/vacío, o flujo rescatista/hospital sin
  nombre → `full_name = "No identificado"`; el reporte se crea igual.
- **Seguridad:** nunca setear `person_state="deceased"` sin verificación humana.
- Borrar escrituras a `reunion_reports` y los modelos muertos `Report`/`Photo` de
  `reporte.py`.

---

## FASE 3 — Features de V2 + el payoff de montar

| # | Feature | Acción | Estado |
|---|---|---|---|
| **3.0** | **Matching instantáneo en proceso** | Pasar `request.app` por `Orchestrator.process_message` hasta el hook de commit; tras `commit_report` exitoso → `await embed_and_match_report(report_id, report_data, app)` (usa `app.state.text_model`, escribe `text_embedding` + cruza + puebla `matches`) | **NUEVO** |
| **3.1** | Rate limit por teléfono | Sliding window en memoria (20/60s) en `webhook.py`, antes del Orchestrator; GC de teléfonos idle | Portar de V2 |
| **3.2** | Sesión durable | — | ✅ Fase 0 |
| **3.3** | No identificados | — | ✅ Fase 2 |
| **3.4** | Reply inline con candidatos | Nuevo `app/core/match_notifier.py`: ILIKE sobre `reports` (kind opuesto) vía REST → respuesta inmediata, guard 1×/reporte. **Siempre** con marco "posible coincidencia, en verificación" | Portar de V2 |
| **3.5** | Cadena de fallback LLM | Envolver DeepSeek del `IntentDetector` con proveedores alternos | Opcional (intent detection `False` por defecto) |

> Nota 3.0 vs 3.4: `embed_and_match_report` alimenta `matches` (dashboard + notify con
> verificación humana); `match_notifier` (3.4) da el feedback léxico **inmediato** al
> usuario en el chat. Complementarios y compatibles con "verificar-luego-notificar".

---

## FASE 4 — Verificación

1. **Durabilidad un-proceso**: conversación a medias → `docker compose restart reune-api`
   → confirmar que retoma en el mismo nodo (estado en PG local).
2. **Carga Supabase**: tail de logs → **cero** llamadas a Supabase por mensaje; solo en
   commit del reporte.
3. **E2E los 3 flujos** (familiar / rescatista / hospital): reporte en `reports` con
   columnas correctas → foto en `photos` → fila en `bot_subscribers` → ILIKE responde
   inline → `embed_and_match_report` deja fila en `matches` **al instante**.
4. **Notify loop**: aprobar un match en el dashboard → `notify_pipeline` envía WhatsApp
   a la familia vía `bot_subscribers`.
5. **Concurrencia**: ráfaga de webhooks simultáneos → confirmar que el PG local no
   serializa escrituras.

## Criterios de aceptación

- [ ] Un mensaje de WhatsApp no genera ninguna llamada a Supabase (solo el commit del reporte sí).
- [ ] Reiniciar la API no pierde conversaciones en curso.
- [ ] Un reporte del bot aparece en `reports` con `source="waha_whatsapp"`, embedding poblado y (si hubo foto) fila en `photos`.
- [ ] Existe `bot_subscribers` para cada reporte → notify proactivo operativo.
- [ ] Los 3 flujos `sendList` (familiar/rescatista/hospital) funcionan igual que en prox.
- [ ] Match semántico/facial aparece en segundos, no en el batch.

## Secuencia de trabajo

`Fase 0` → `Fase 1` → **arrancar y testear transporte** → `Fase 2` (el corazón) →
`Fase 3.0 + 3.1 + 3.4` → `Fase 4` → `Fase 3.5` (opcional).

---

# Decisiones de implementación (registro explícito)

Esta sección documenta, de forma explícita y trazable, **por qué** se tomó cada decisión.
Se separan en dos categorías: decisiones para **preservar la funcionalidad de V2** sobre
la implementación de V1, y decisiones para ser **cost-efficient**.

## A. Decisiones para preservar la funcionalidad de V2 sobre V1

Cada fila responde: "V2 hacía X; ¿cómo lo conservamos al volver a V1?"

| # | Funcionalidad de V2 a preservar | Decisión de implementación | Por qué / mecanismo |
|---|---|---|---|
| **A1** | **Sesión durable** (V2 guardaba estado en Supabase `waha_sessions`; sobrevivía reinicios) | Estado conversacional en **Postgres local + volumen** vía SQLAlchemy (Fase 0) | V1 ya persistía en SQLAlchemy pero en DB efímera. Con volumen persistente, una conversación a medias **sobrevive el reinicio** del contenedor — misma garantía que daba V2, sin código de sesión. |
| **A2** | **Reporte conectado al ecosistema** (V2 escribía en `reports` → matching, dedup, face, notify) | Migrar intake de `reunion_reports` a **`reports`** con mapeo de columnas (Fase 2) | `reunion_reports` es un silo aislado, fuera de las migraciones, que el matching nunca ve. Escribir en `reports` reconecta dedup/face/cross-match/notify, igual que V2. |
| **A3** | **Notificación proactiva** (V2 poblaba `bot_subscribers`; `notify_pipeline` avisaba a la familia) | El nuevo `commit_report` **inserta en `bot_subscribers`** `{report_id, phone, full_name, kind}` (Fase 2) | `bot_subscribers` lo escribía **solo** `waha_intake.py`. Al borrar ese archivo, sin esta inserción `notify_pipeline` quedaría en no-op. Esta línea mantiene viva la notificación proactiva. |
| **A4** | **Matching facial por foto** (V2 metía fotos donde el face pipeline las veía) | `commit_report` hace `INSERT` en la tabla **`photos`** (FK `report_id`), no solo `photo_url` (Fase 2) | `face_backfill` y el face cross-match operan sobre la tabla `photos`. Sin la fila, las fotos del bot no participarían en el matching facial. |
| **A5** | **Matching inmediato al reportar** (V2 corría `embed_and_match_report` inline) | **Montar prox en el proceso raíz** y llamar `embed_and_match_report(report_id, data, app)` tras el commit (Fase 3.0 / topología B) | El embedding necesita `app.state.text_model` (~1 GB) en RAM. Solo estando en el mismo proceso se obtiene match semántico **instantáneo**; un servicio separado lo relegaría al batch (≤30–60 min). |
| **A6** | **Respuesta con candidatos en el chat** (V2 respondía coincidencias al estar listo el reporte) | Portar la búsqueda inline a `app/core/match_notifier.py` (ILIKE, guard 1×/reporte) (Fase 3.4) | Da el feedback léxico inmediato que tenía V2, **siempre** con el marco "posible coincidencia, en verificación". |
| **A7** | **Rate limit por teléfono** (V2: 20 msg/60s por número) | Portar el sliding window en memoria a `webhook.py` (Fase 3.1) | Protege LLM/DB de un número en bucle o spam, sin throttlear a todos por IP (los webhooks llegan todos de la misma IP de WAHA). |
| **A8** | **Personas no identificadas** (V2 registraba hallazgos sin nombre) | `full_name = "No identificado"` cuando no hay nombre; el reporte se crea igual (Fase 2) | El caso de mayor valor (rescatista/hospital con persona inconsciente) no debe bloquearse por falta de nombre. |
| **A9** | **Dedup de webhooks** (V2 descartaba el mismo `msg_id`) | **Ya existe en V1**: `webhook.py` deduplica por `event:msg_id` con `OrderedDict` (TTL 30s) | No requiere trabajo; se documenta para confirmar paridad con V2. |
| **A10** | **Verificación HMAC del webhook** (V2: validación de firma) | **Ya existe en V1**: `X-WAHA-Token` con `secrets.compare_digest` si `WAHA_WEBHOOK_SECRET` está seteado | Paridad ya cubierta por V1. |
| **A11** | **Fallback chain de LLM** (V2: Groq → proveedores alternos) | Envolver el `IntentDetector` (DeepSeek) con proveedores alternos (Fase 3.5, opcional) | Baja prioridad: el intent detection viene **apagado** por defecto (`enable_intent_detection=False`); no es ruta crítica hoy. |

## B. Decisiones para ser cost-efficient

Cada fila responde: "¿cómo reducimos costo (carga a Supabase, latencia, RAM, infraestructura, trabajo) sin perder funcionalidad?"

| # | Decisión de implementación | Ahorro / eficiencia | Trade-off aceptado |
|---|---|---|---|
| **B1** | **Estado conversacional fuera de Supabase**, en Postgres local | V2 pegaba a Supabase **en cada mensaje** (`waha_sessions`: 1 GET + 1 POST por turno). Moverlo a PG local elimina **todo** ese tráfico por-mensaje. Ataca directo el "Supabase está sudando". | Operar un contenedor `db` + volumen. |
| **B2** | **Acceso local del hot-path** (SQLAlchemy a PG en el mismo host) | Lecturas/escrituras de `conversaciones` por conexión persistente y transacción local — **milisegundos**, sin viaje a internet ni HTTP round-trips por fila (lo que tendría REST puro). | La base relacional vive junto al bot (volumen). |
| **B3** | **No convertir `flow_nodes`/`bot_config`/FAQ a constantes en código** | Esa optimización solo servía para evitar round-trips REST a Supabase. Con PG **local**, leer esas tablas ya es barato → se evita una refactorización del motor FSM (menos trabajo, menos riesgo de regресión). | El flujo se edita en DB (seed), no en código — de hecho una ventaja (cambios sin redeploy). |
| **B4** | **No cachear `blocked_clients` en memoria** | Mismo razonamiento que B3: con PG local el check es una lectura local barata; el cache añadía complejidad e invalidación innecesarias. | Ninguno relevante. |
| **B5** | **Escrituras write-only desechables** (`clientes`, `mensajes_conversacion`, `eventos_conversacion`) **en PG local** | Nadie las lee en la ruta de decisión. Mantenerlas locales (y candidatas a fire-and-forget) hace que su latencia sea **invisible** a la respuesta y no consuman cuota de Supabase. | Analytics/registro no están en Supabase (se pueden exportar si se requiere). |
| **B6** | **Postgres local como contenedor aparte** (no SQLite, no en el contenedor del bot) | Bajo carga "multitudinaria", SQLite serializa escrituras en un lock (un solo escritor) → latencia. Postgres maneja concurrencia nativa. Y al ser contenedor aparte, **no consume** de los 2 GB del bot/ML. | Un servicio más en el compose. |
| **B7** | **Supabase solo para salidas compartidas** (`reports`, `photos`, `matches`, `bot_subscribers`, `hospitales`) | Supabase recibe únicamente lo que **otros componentes leen** (matching, dashboard, notify). El "chat" interno por-mensaje no le cuesta nada. | Ninguno: es justo el límite correcto de responsabilidad. |
| **B8** | **Backup del volumen 1×/hora** (no por evento) | Durabilidad ante desastre (no solo reinicio) con **1 escritura/hora** a Supabase Storage — irrelevante para la cuota, frente a escribir por mensaje. | Ventana de hasta 1 h de pérdida ante destrucción total del volumen (aceptable). |
| **B9** | **Reutilizar `embed_and_match_report` en proceso** en vez de un sidecar de embeddings | Evita levantar un segundo servicio con su propia copia del modelo (~1 GB de RAM duplicada) y la red entre servicios. El modelo ya está cargado en el raíz. | El proceso raíz es un único punto de fallo (mitigado por PG durable: retoma al reiniciar). |
| **B10** | **Reusar el motor FSM de V1 tal cual** (solo cambiar destino de datos) | Se evita reescribir orquestador, flow engine, decision engine y los 3 flujos `sendList` — código ya probado. El cambio se concentra en `intake.py` (destino) y `main.py` (montaje). | Se arrastran las deps de prox (`sqlalchemy`, `psycopg2`, `openai`, `supabase`) al raíz — RAM despreciable. |
| **B11** | **Montar en el raíz en vez de servicio separado** | Un solo despliegue, un solo modelo ML en RAM, sin red entre servicios ni endpoint interno autenticado que mantener. | Deps de prox sumadas al raíz y un proceso compartido (ver B9). |

## Principio rector

> **Supabase es para datos que se comparten entre componentes; el Postgres local es para
> el estado efímero-pero-durable de la conversación.** Todo lo que solo le importa al bot
> (en qué nodo va el usuario, su contexto, los logs) se queda local y gratis; todo lo que
> el matching, el dashboard o la notificación necesitan leer, va a Supabase.
