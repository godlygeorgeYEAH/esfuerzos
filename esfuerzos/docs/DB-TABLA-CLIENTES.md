# DB — Tabla `clientes`

Registra cada persona que contacta el bot. Se crea al primer mensaje recibido.

## Esquema

| Campo | Tipo | Descripción |
|---|---|---|
| `id` | int PK | Identificador interno |
| `wa_chat_id` | str UNIQUE | Chat ID de WAHA (`584...@c.us` o `...@lid`). Clave natural del cliente. |
| `phone` | str | Número en E.164 (`584244107121`). Resuelto desde `@lid` si aplica. |
| `user_type` | str nullable | Perfil declarado: `familiar` · `rescatista` · `hospital` |
| `is_blocked` | bool | `true` = el bot ignora mensajes de este número |
| `created_at` | timestamptz | Primera interacción |
| `last_seen_at` | timestamptz | Última interacción |

## Modelo

`esfuerzos/modulos/migration_prox/app/models/cliente.py`

## Relaciones

- `clientes.wa_chat_id` ↔ `conversaciones.waha_chat_id`
- `clientes.id` → `reports.cliente_id` (pendiente de implementar)

## Pendientes

- Hacer upsert en `clientes` al recibir cada mensaje (registra primera visita y actualiza `last_seen_at`).
- Poblar `user_type` cuando el usuario elige su perfil en el nodo `bienvenida`.
- Usar `user_type` persistido para no pedirle al usuario que se identifique de nuevo si el bot se reinicia.
- Cruzar `is_blocked` en el webhook antes de pasar al orquestador.
