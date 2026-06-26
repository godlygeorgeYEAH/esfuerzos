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
| Mensajería | WhatsApp Cloud API (Graph API v22 o superior) |
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

- Verificar el webhook con `hub.challenge`.
- Recibir mensajes de texto, imagen (descargar por media id vía Graph API) y respuestas de Flow.
- Dos entradas por botones interactivos: **"Reporto un desaparecido"** y **"Soy rescatista"**.
- Campos estructurados vía WhatsApp Flows: nombre, edad, última ubicación, señas, ropa.
- Fotos múltiples: agrupar mensajes por sesión con clave `reporter_wa_hash` y TTL.
- Dentro de la ventana de 24h las respuestas libres son válidas. Guardar el `wa_id` hasheado.

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

### 7.5 Notificación (plantilla Utility)

- Al confirmar un match, enviar la plantilla Utility aprobada al `wa_id` de la familia.
- Texto provisional: *"Tenemos una posible coincidencia con tu reporte de {{1}}. Un voluntario la verificará y te contactará pronto."*
- Nunca incluir estado ni detalles. El humano hace el contacto.

---

## 8. WhatsApp Cloud API — restricciones

| Restricción | Detalle |
|-------------|---------|
| **Ventana de 24h** | Texto libre solo dentro de las 24h desde el último mensaje del usuario. Fuera, solo plantilla aprobada. |
| **Plantilla Utility** | Gratis si se envía dentro de la ventana de 24h, se cobra fuera. Enviar a aprobación antes de usar (hasta 24h de revisión). |
| **Volumen** | 250 conversaciones iniciadas por el negocio en 24h (compartidas en el portfolio). Las iniciadas por el usuario no cuentan. Para superar 250 se necesita verificación de negocio de Meta (2-10 días). **Arrancar la verificación ya.** |
| **Media** | Descargar por media id vía Graph API. Las fotos múltiples llegan como mensajes separados. |
| **Opt-in** | Obligatorio. El quality rating puede pausar plantillas si los usuarios reportan. |
| **Errores comunes** | `131026` fuera de ventana · `131047` número no registrado en WhatsApp |

---

## 9. CompreFace (deploy)

- `docker-compose` en GCP (VM de Compute Engine o Cloud Run).
- Config: `ANONYMIZE_DATA=true` para guardar solo embeddings, `DATA_RETENTION_DAYS=N`, API keys por servicio, IP whitelist en el reverse proxy.
- Modelo InsightFace / ArcFace, 512 dim.
- Endpoint usado: detección con calidad y embedding. Opcional: reconocimiento 1:N nativo si se elige el Design A (ver sección 14).

---

## 10. Seguridad

- Verificar la firma de Meta del webhook con `X-Hub-Signature-256`.
- Secrets en variables de entorno o Supabase Vault, nunca en el repo.
- RLS en Supabase para las tablas sensibles. La `service role` solo en el backend.
- Hash de `reporter_wa_id` y cifrado del contacto.
- Fotos crudas en storage temporal con expiración. Conservar solo embeddings.
- Rate limit en el webhook y en la consola.

---

## 11. Variables de entorno

```env
WHATSAPP_TOKEN=
WHATSAPP_PHONE_NUMBER_ID=
WHATSAPP_VERIFY_TOKEN=
WHATSAPP_APP_SECRET=
WABA_ID=

COMPREFACE_URL=
COMPREFACE_API_KEY=

SUPABASE_URL=
SUPABASE_SERVICE_KEY=

EMBEDDINGS_API_KEY=

FACE_MATCH_THRESHOLD=
COMBINED_MATCH_THRESHOLD=
PHOTO_RETENTION_DAYS=
```

---

## 12. Orden de build

1. Schema, extensiones y RLS.
2. Deploy de CompreFace y smoke test: `detect` devuelve embedding.
3. Webhook de WhatsApp: verify, recepción y descarga de media.
4. Flujos de intake (missing y found) con Flows y manejo de sesión.
5. Embeddings de texto y de cara, con gate de calidad.
6. Motor de match: cara 1:N, texto, fusión, escritura en `matches`, dedup.
7. Consola de revisión en Lovable: confirmar, descartar, audit.
8. Submit de la plantilla Utility y notificación al confirmar.
9. Hardening: firma, rate limit, job de retención, verificación de RLS.

---

## 13. Criterios de aceptación (gate 95/100)

- [ ] Un reporte de desaparecido y uno de encontrado de la misma persona producen un match pendiente con score visible.
- [ ] Personas distintas no producen match por encima del umbral (probado con pares).
- [ ] El bot nunca envía estado ni confirma sin acción humana.
- [ ] El webhook valida firma. Endpoints sin auth: cero.
- [ ] Las fotos crudas expiran. Los embeddings persisten.
- [ ] La plantilla Utility está aprobada y la notificación se entrega en prueba.
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

**Primer objetivo:** secciones 6 y 9 (schema + CompreFace) y luego 7.1 (webhook).

**Prompt inicial sugerido:**

> Lee este PRD. Implementa el milestone 1 y 2: el schema SQL de la sección 6 como migración de Supabase, y el docker-compose de CompreFace de la sección 9 con `ANONYMIZE_DATA=true` e IP whitelist. No agregues nada fuera del alcance. Código conciso, sin comentarios. Al terminar, dame el smoke test que confirma que CompreFace detecta una cara y devuelve un embedding de 512 dim.
