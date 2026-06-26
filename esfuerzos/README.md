# PRD — Reúne v1

Sistema de reunificación familiar por WhatsApp. Cruza reportes de personas desaparecidas y encontradas por datos y por cara, deduplica, y entrega coincidencias a revisión humana.

**Para:** Claude Code · **Estado:** listo para construir · **Fecha:** junio 2026

---

## 1. Objetivo

Construir un sistema que reciba por WhatsApp reportes de desaparecidos (familias) y de encontrados (rescatistas, hospitales, refugios), genere embeddings de texto y de cara, deduplique, cruce 1:N, y escriba coincidencias candidatas a una cola de revisión humana. Un humano confirma o descarta. Al confirmar, una plantilla notifica a la familia y una persona hace el contacto.

---

## 2. Contexto

Terremoto doblete M7.2 / M7.5, 24 jun 2026, Venezuela. La data de desaparecidos está fragmentada en 5 o más sitios sin base común, con cifras infladas por duplicados. Conectividad degradada tras el sismo. WhatsApp es el canal con mejor alcance.

---

## 3. Alcance v1

**Incluye:**
- Webhook de WhatsApp con dos flujos de intake
- Almacenamiento, embeddings de texto y de cara
- Gate de calidad de foto
- Deduplicación
- Motor de match 1:N
- Cola de revisión
- Consola web
- Notificación por plantilla Utility

**Excluye:**
- Scraping de redes sociales
- Ingesta de sitios externos
- Mapa de daños
- App móvil nativa
- Red de sensores
- Multi-idioma
- Notificación automática de estado
- Galería pública

---

## 4. Restricciones no negociables

- **Humano en el loop.** Ninguna coincidencia se confirma ni se comunica sin revisión humana.
- **El bot nunca comunica un fallecimiento.** El estado sensible lo da una persona.
- **Lenguaje cauteloso.** La UI y los mensajes dicen "posible coincidencia, en verificación", nunca "encontrado".
- **Privacidad.** Guardar embeddings, no fotos crudas a largo plazo. Cifrado en reposo. RBAC. IP allowlist. Retención configurable.
- **Cara como señal secundaria.** Umbral alto. El texto es la señal primaria.
- **Seguridad.** Validar firma del webhook. Sanitizar todo input. Rate limit. Cero endpoints sin auth.

---

## 5. Stack

| Capa | Tecnología |
|------|-----------|
| Backend | Supabase Edge Functions (Deno / TypeScript) |
| DB | Supabase Postgres con pgvector |
| Reconocimiento facial | CompreFace en Docker sobre GCP (Compute Engine o Cloud Run), modelo InsightFace / ArcFace, `ANONYMIZE_DATA=true` |
| Mensajería | WAHA (WhatsApp HTTP API, auto-hospedado) |
| Consola de revisión | Lovable consumiendo Supabase |
| Embeddings de texto | text-embedding-3-small (1536 dim) u otro equivalente |

---

## 6. Modelo de datos

Los tres conjuntos lógicos (desaparecidos, encontrados, externos) viven en `reports` distinguidos por `kind` y `source`. Es la forma normalizada correcta en lugar de tres tablas físicas.

```sql
create extension if not exists vector;

create type report_kind as enum ('missing', 'found');
create type person_state as enum ('alive', 'injured', 'deceased', 'unknown');
create type match_status as enum ('pending', 'confirmed', 'dismissed');

create table reports (
  id                   uuid primary key default gen_random_uuid(),
  kind                 report_kind not null,
  full_name            text,
  age                  int,
  last_seen_location   text,
  last_seen_lat        double precision,
  last_seen_lng        double precision,
  distinguishing_marks text,
  clothing             text,
  person_state         person_state default 'unknown',
  reporter_wa_hash     text,
  reporter_contact_enc text,
  source               text default 'whatsapp',
  source_url           text,
  consent              boolean default false,
  text_embedding       vector(1536),
  created_at           timestamptz default now(),
  expires_at           timestamptz
);

create table photos (
  id             uuid primary key default gen_random_uuid(),
  report_id      uuid references reports(id) on delete cascade,
  storage_path   text not null,
  face_embedding vector(512),
  quality_ok     boolean,
  created_at     timestamptz default now()
);

create table matches (
  id             uuid primary key default gen_random_uuid(),
  missing_id     uuid references reports(id),
  found_id       uuid references reports(id),
  text_score     real,
  face_score     real,
  combined_score real,
  status         match_status default 'pending',
  reviewer       text,
  reviewed_at    timestamptz,
  created_at     timestamptz default now(),
  unique (missing_id, found_id)
);

create table audit_log (
  id        bigserial primary key,
  actor     text,
  action    text,
  entity    text,
  entity_id uuid,
  meta      jsonb,
  created_at timestamptz default now()
);

-- Índices
create index on reports using ivfflat (text_embedding vector_cosine_ops) with (lists = 100);
create index on photos  using ivfflat (face_embedding vector_cosine_ops) with (lists = 100);
create index on reports (kind);
create index on matches (status);
```

---

## 7. Componentes

### 7.1 Intake WhatsApp (webhook)

- WAHA entrega los mensajes vía `POST /webhook/waha`. No requiere `hub.challenge` ni verificación de Meta.
- Recibir mensajes de texto e imagen. Las fotos llegan con `hasMedia: true` y una `mediaUrl` directa servida por WAHA.
- Dos flujos conversacionales de texto guiado: **"Reporto un desaparecido"** y **"Soy rescatista / encontré a alguien"**.
- Campos recopilados mediante conversación estructurada: nombre, edad, última ubicación, señas, ropa.
- Fotos múltiples: agrupar mensajes por sesión con clave `reporter_wa_hash` y TTL. El usuario envía "listo" para cerrar la sesión de fotos.
- Sin restricción de ventana de 24h: WAHA permite mensajes libres en cualquier momento.

### 7.2 Embeddings y gate de calidad

- **Foto recibida:** llamar a CompreFace para detectar cara. Si no hay cara o la calidad es baja, rechazar con mensaje guía. Si pasa, obtener el embedding de 512 dim y guardarlo en `photos.face_embedding`.
- **Campos completos:** construir el string `{nombre + edad + ubicación + señas + ropa}`, generar el embedding de 1536 dim y guardarlo en `reports.text_embedding`.

### 7.3 Motor de match (1:N)

- **Disparador:** nuevo reporte `found` con al menos un embedding de cara.
- **Búsqueda por cara:** por cada foto del `found`, cosine search contra los embeddings de cara de los `missing`. Top-K con `face_score`.
- **Búsqueda por texto:** cosine search de `found.text_embedding` contra `missing.text_embedding` → `text_score`.
- **Fusión:** `combined_score = w_face * face_score + w_text * text_score`, válido solo cuando `face_score` supera el umbral duro. Documentar pesos y umbrales como config.
- Escribir candidatos en `matches` con `status = pending` por encima de un mínimo de `combined_score`.
- **Dedup:** dentro del mismo `kind`, detectar near-duplicates por alta similitud de texto y cara, y marcarlos.

### 7.4 Consola de revisión (Lovable)

- Listar matches pendientes ordenados por `combined_score` descendente.
- Mostrar ambos reportes lado a lado: fotos, campos, scores.
- Acciones: **confirmar** o **descartar**. Al confirmar, disparar la notificación. Registrar en `audit_log`.
- Auth obligatoria, rol `reviewer`.

### 7.5 Notificación

- Al confirmar un match, enviar un mensaje libre de texto al número de la familia vía WAHA `send_message`.
- Texto provisional: *"Tenemos una posible coincidencia con tu reporte de [nombre]. Un voluntario la verificará y te contactará pronto."*
- Nunca incluir estado ni detalles. El humano hace el contacto.
- No se requieren plantillas aprobadas por Meta; WAHA permite mensajes salientes sin restricciones de template.

---

## 8. WAHA — consideraciones operacionales

| Consideración | Detalle |
|---------------|---------|
| **Sin ventana de 24h** | WAHA no impone la restricción de Meta. Se puede responder en cualquier momento sin plantillas. |
| **Sin aprobación de templates** | Los mensajes de notificación son texto libre; no requieren aprobación previa ni Business Manager. |
| **Volumen** | Limitado por la capacidad del dispositivo/número. Para alta concurrencia, usar múltiples sesiones WAHA. |
| **Media** | Las fotos llegan con `mediaUrl` directa servida por el servidor WAHA. Se descarga con un GET simple. |
| **Sin opt-in formal de Meta** | El usuario inicia la conversación; eso constituye el consentimiento implícito del canal. El sistema registra consentimiento explícito durante el intake. |
| **Sesión WAHA** | Requiere mantener la sesión WhatsApp Web activa. Configurar reinicio automático ante desconexión. |
| **Token de webhook** | Opcional: `X-WAHA-Token` en la cabecera para validar el origen. Configurado en `WAHA_WEBHOOK_SECRET`. |

---

## 9. CompreFace (deploy)

- `docker-compose` en GCP (VM de Compute Engine o Cloud Run).
- Config: `ANONYMIZE_DATA=true` para guardar solo embeddings, `DATA_RETENTION_DAYS=N`, API keys por servicio, IP whitelist en el reverse proxy.
- Modelo InsightFace / ArcFace, 512 dim.
- Endpoint usado: detección con calidad y embedding. Opcional: reconocimiento 1:N nativo si se elige el Design A (ver sección 14).

---

## 10. Seguridad

- Validar el token de webhook WAHA con `X-WAHA-Token` (comparación constante via `secrets.compare_digest`).
- Secrets en variables de entorno, nunca en el repo.
- RLS en Supabase para las tablas sensibles. La `service role` solo en el backend.
- Hash SHA-256 de `reporter_wa_id` y cifrado del contacto.
- Fotos crudas en storage temporal con expiración. Conservar solo embeddings.
- Rate limit en el webhook (`slowapi`) y en la consola.

---

## 11. Variables de entorno

```env
# WAHA (mensajería WhatsApp)
WAHA_URL=http://localhost:3000
WAHA_SESSION=default
WAHA_WEBHOOK_SECRET=          # Opcional: token para validar origen del webhook

# Base de datos
DATABASE_URL=postgresql://user:pass@localhost:5432/reune

# DeepSeek (NLU del bot)
DEEPSEEK_API_KEY=sk-...
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-chat

# CompreFace (reconocimiento facial)
COMPREFACE_URL=
COMPREFACE_API_KEY=

# Supabase (futura integración pgvector)
SUPABASE_URL=
SUPABASE_SERVICE_KEY=

# Embeddings de texto
EMBEDDINGS_API_KEY=

# Umbrales de matching
FACE_MATCH_THRESHOLD=
COMBINED_MATCH_THRESHOLD=
PHOTO_RETENTION_DAYS=

# Entorno
ENVIRONMENT=development
BOT_SELF_MESSAGE_TESTING=true
```

---

## 12. Orden de build

1. Schema PostgreSQL y modelos SQLAlchemy (reports, photos, matches, audit_log).
2. Deploy de WAHA y verificación de sesión activa.
3. Webhook WAHA: recepción de texto e imagen, resolución de negocio/sesión.
4. Flujos de intake conversacionales (missing y found) con manejo de sesión de fotos.
5. Deploy de CompreFace y smoke test: `detect` devuelve embedding de 512 dim.
6. Embeddings de texto y de cara con gate de calidad fotográfica.
7. Motor de match: cara 1:N, texto, fusión, escritura en `matches`, dedup.
8. Consola de revisión: confirmar, descartar, audit_log.
9. Notificación al confirmar vía WAHA `send_message`.
10. Hardening: token webhook, rate limit, job de retención de fotos.

---

## 13. Criterios de aceptación (gate 95/100)

- [ ] Un reporte de desaparecido y uno de encontrado de la misma persona producen un match pendiente con score visible.
- [ ] Personas distintas no producen match por encima del umbral (probado con pares).
- [ ] El bot nunca envía estado ni confirma sin acción humana.
- [ ] El webhook valida el token WAHA. Endpoints sin auth: cero.
- [ ] Las fotos crudas expiran. Los embeddings persisten.
- [ ] La notificación de match se entrega correctamente al número de la familia vía WAHA.
- [ ] Dos reportes de la misma persona se detectan como duplicados.

---

## 14. Decisiones abiertas

| Decisión | Opciones |
|----------|---------|
| **Design A vs B** | A: CompreFace gestiona la búsqueda facial 1:N nativa. **B (default):** los embeddings viven en pgvector y la búsqueda de texto y de cara se hace con cosine en Postgres, un solo store. B unifica dedup y match y encaja con el stack. |
| **Pesos w_face / w_text** | A calibrar. |
| **Umbrales exactos** | A calibrar con datos reales. |
| **Partner humanitario** | Para la capa humana y la notificación sensible. |
| **Modelo de embeddings de texto** | Por definir (default: text-embedding-3-small). |

---

## 15. Arranque con Claude Code

**Primer objetivo (WhatsApp):** levantar WAHA + webhook funcional (sección 7.1) con los flujos de intake de texto (sección 7.1) antes de integrar CompreFace o embeddings.

**Prompt inicial sugerido:**

> Lee este PRD. Implementa el milestone de WhatsApp: modelos SQLAlchemy de la sección 6 (sin pgvector por ahora), flujos de intake conversacional missing y found (sección 7.1), y el seed del bot. Canal: WAHA. No tocar Supabase ni CompreFace en este paso. Código conciso, sin comentarios.
