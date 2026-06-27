# Refactor: intake.py → Supabase

## Qué cambió

`app/core/intake.py` fue reescrito para escribir reportes directamente en **Supabase** (`reunion_reports`) en lugar de la base de datos SQLite local (`reports`, `photos`).

## Por qué

El sistema de matching de Gerardo (InsightFace + SentenceTransformer) lee de `reunion_reports` en Supabase. Para que los reportes capturados por el bot ProX sean procesados por ese pipeline, deben persistirse ahí.

## Tabla destino

```
reunion_reports (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  kind           TEXT  -- 'missing' | 'found'
  reporter_wa_hash TEXT  -- SHA-256 del teléfono, primeros 32 chars
  name           TEXT
  age            TEXT
  location       TEXT
  marks          TEXT  -- solo para kind='missing'
  found_state    TEXT  -- solo para kind='found', siempre 'unknown' desde ProX
  photo_url      TEXT  -- URL pública de la primera foto en Supabase Storage
  raw_data       JSONB -- texto crudo, notas, género, conteo de fotos, fuente
  verified       BOOLEAN DEFAULT false
)
```

Bucket de fotos: `reunion-photos` → path `{kind}/{report_id}_{index}.jpg`

## Variables de entorno requeridas

```
SUPABASE_URL=https://bgebvwchqtrhvdhkpzgk.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<service role key>
```

Ambas se agregaron a `app/config.py` con default vacío — si no están configuradas, `commit_report` loguea un warning y descarta el reporte silenciosamente sin romper el flujo.

## Flujo de datos

```
Conversación → context["intake_person_raw"] + context["pending_photos"]
                        ↓
                  commit_report()
                        ↓
              parse_person_data(raw_text)
                        ↓
         INSERT → reunion_reports (sin foto_url aún)
                        ↓
         Para cada foto: leer archivo local → upload Supabase Storage
                        ↓
         UPDATE reunion_reports SET photo_url = <primera foto>
                        ↓
              Limpiar contexto de conversación
```

## Qué se eliminó

- Import de `app.models.reporte.Report` y `Photo`
- Escritura a tablas SQLite `reports` y `photos`
- El modelo `Report` sigue existiendo en `app/models/reporte.py` para no romper `Base.metadata.create_all`, pero ya no se usa para nuevos reportes

## Tipo de retorno

Antes devolvía un ORM `Report` con `.id` entero. Ahora devuelve `ReportResult(id: str)` con el UUID de Supabase. El Orchestrator solo usa `.id` para logging, no hay impacto funcional.

## Comportamiento sin Supabase configurado

Si `SUPABASE_URL` o `SUPABASE_SERVICE_ROLE_KEY` están vacíos, el cliente no se inicializa y `commit_report` retorna `None` después de limpiar el contexto. El bot responde normalmente al usuario pero el reporte no se persiste.
