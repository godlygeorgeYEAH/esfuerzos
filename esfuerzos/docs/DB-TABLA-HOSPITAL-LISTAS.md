# DB — Tabla `hospital_listas`

Fotos de listas de ingresos enviadas por hospitales registrados.

**Destino:** Supabase

## Esquema

| Campo | Tipo | Descripción |
|---|---|---|
| `id` | UUID PK | Identificador Supabase |
| `hospital_id` | UUID FK | Referencia a `hospitales.id` |
| `media_url` | TEXT | URL de WAHA de la foto original |
| `photo_url` | TEXT | Path local descargado en el servidor |
| `received_at` | TIMESTAMPTZ | Fecha de recepción |

## Servicio

`app/services/hospital_service.py` → `add_lista()`
