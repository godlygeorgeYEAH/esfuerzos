# Tabla: hospitales + hospital_listas (Supabase)

## SQL — crear en Supabase

```sql
CREATE TABLE IF NOT EXISTS hospitales (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wa_chat_id     TEXT UNIQUE NOT NULL,
    nombre         TEXT,
    ubicacion_texto TEXT,
    lat            FLOAT,
    lng            FLOAT,
    created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hospital_listas (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hospital_id    UUID REFERENCES hospitales(id) NOT NULL,
    media_url      TEXT NOT NULL,
    photo_url      TEXT,
    received_at    TIMESTAMPTZ DEFAULT now()
);
```

## Flujo

1. Hospital elige perfil **3** → nodo `guia_hospital` pide nombre y ubicación
2. Hospital envía texto o GPS → `upsert_hospital()` crea/actualiza registro en `hospitales`
3. Bot avanza a `hospital_registrado` y pide fotos de listas de ingresos
4. Cada foto recibida → `add_lista()` inserta en `hospital_listas` con la URL de WAHA y el path local descargado

## Campos

### hospitales
| Campo | Descripción |
|---|---|
| `wa_chat_id` | ID de chat WAHA (`123456789@c.us`) |
| `nombre` | Nombre del hospital o refugio |
| `ubicacion_texto` | Texto completo enviado (incluye GPS si aplica) |
| `lat` / `lng` | Coordenadas si compartió ubicación por GPS |

### hospital_listas
| Campo | Descripción |
|---|---|
| `hospital_id` | FK a `hospitales` |
| `media_url` | URL de WAHA de la foto original |
| `photo_url` | Path local descargado en el servidor |
