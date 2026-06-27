# DB — Tabla `hospitales`

Registra hospitales y refugios que se registran en el bot.

**Destino:** Supabase

## Esquema

| Campo | Tipo | Descripción |
|---|---|---|
| `id` | UUID PK | Identificador Supabase |
| `wa_chat_id` | TEXT UNIQUE | Chat ID de WAHA. Clave natural. |
| `nombre` | TEXT | Nombre del hospital o refugio |
| `ubicacion_texto` | TEXT | Texto completo enviado por el usuario |
| `lat` | FLOAT | Latitud (si compartió GPS) |
| `lng` | FLOAT | Longitud (si compartió GPS) |
| `created_at` | TIMESTAMPTZ | Fecha de registro |

## Servicio

`app/services/hospital_service.py` → `upsert_hospital()`
