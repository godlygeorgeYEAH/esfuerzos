# DB — Tabla `clientes`

Registra cada persona que contacta el bot. Se crea al primer mensaje y se actualiza en cada interacción.

**Destino:** Supabase

## Esquema

| Campo | Tipo | Descripción |
|---|---|---|
| `id` | UUID PK | Identificador Supabase |
| `wa_chat_id` | TEXT UNIQUE | Chat ID de WAHA (`584...@c.us`). Clave natural. |
| `phone` | TEXT | Número de teléfono |
| `user_type` | TEXT | Perfil declarado: `familiar` · `rescatista` · `hospital` |
| `is_blocked` | BOOLEAN | `true` = bot ignora mensajes de este número |
| `created_at` | TIMESTAMPTZ | Primera interacción |
| `last_seen_at` | TIMESTAMPTZ | Último mensaje recibido |

## Servicio

`app/services/cliente_service.py`

- `upsert_cliente()` — crea o actualiza `last_seen_at` en cada mensaje
- `set_user_type()` — persiste el perfil al salir del nodo `bienvenida`
