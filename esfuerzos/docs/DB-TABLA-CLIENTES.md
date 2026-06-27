# Tabla: clientes (Supabase)

Registra cada persona que interactúa con el bot, su tipo de usuario y actividad.

## SQL — crear en Supabase

```sql
CREATE TABLE IF NOT EXISTS clientes (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wa_chat_id     TEXT UNIQUE NOT NULL,
    phone          TEXT NOT NULL,
    user_type      TEXT CHECK (user_type IN ('familiar', 'rescatista', 'hospital')),
    is_blocked     BOOLEAN DEFAULT false,
    created_at     TIMESTAMPTZ DEFAULT now(),
    last_seen_at   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS clientes_phone_idx ON clientes (phone);
```

## Tipos de usuario

| user_type | Quién es | Nodo que lo activa |
|---|---|---|
| `familiar` | Persona buscando a un desaparecido | `guia_familiar` |
| `rescatista` | Rescatista reportando a alguien encontrado | `guia_rescatista` |
| `hospital` | Hospital o refugio registrando ingresos | `guia_hospital` |
| `NULL` | Usuario que no ha elegido perfil aún | — |

El `user_type` se asigna al salir del nodo `bienvenida` cuando el usuario elige 1, 2 o 3.

## Campos

| Campo | Tipo | Descripción |
|---|---|---|
| `wa_chat_id` | TEXT UNIQUE | ID de chat de WAHA (`123456789@c.us`) |
| `phone` | TEXT | Número de teléfono sin formato |
| `user_type` | TEXT | Rol elegido por el usuario |
| `is_blocked` | BOOLEAN | Si el bot ignora sus mensajes |
| `created_at` | TIMESTAMPTZ | Primera interacción |
| `last_seen_at` | TIMESTAMPTZ | Último mensaje recibido |
