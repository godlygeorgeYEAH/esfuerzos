# Flujo conversacional — Hospital / Refugio

Activado cuando el usuario escribe `3` en `bienvenida`.

## Nodos

| Nodo | Entrada | Respuesta | Avanza a |
|---|---|---|---|
| `guia_hospital` | sin texto | "Por favor envíen el nombre y ubicación…" | — permanece |
| `guia_hospital` | texto libre / GPS | Registra en Supabase. "✅ Registro completado. Envíen fotos de listas de ingresos cuando puedan." | `hospital_registrado` |
| `hospital_registrado` | imagen | Guarda en `hospital_listas`. "Lista recibida (N). Pueden seguir enviando." | — permanece (loop) |
| `hospital_registrado` | texto | "Para enviar listas, envíen una foto de la lista de ingresos." | — permanece |

## Parseo de ubicación

El texto de `guia_hospital` puede ser:

- **Texto libre**: `"Clínica El Ángel, Av. Principal Cumaná"` → se guarda en `ubicacion_texto`, `lat/lng = null`
- **GPS compartido por WhatsApp**: WAHA entrega coordenadas en `payload.location` → se parsean `lat`, `lng` y opcionalmente `description` como nombre

El webhook siempre prioriza `payload.location` sobre el `body` del mensaje (que en GPS contiene un thumbnail JPEG descartable).

## Datos persistidos → Supabase

### `hospitales`

| Campo | Fuente |
|---|---|
| `wa_chat_id` | Chat ID WAHA del hospital |
| `nombre` | Extraído del texto (primera línea) o `location.description` |
| `ubicacion_texto` | Texto completo enviado |
| `lat` / `lng` | Coordenadas GPS si se compartió ubicación |

### `hospital_listas`

| Campo | Fuente |
|---|---|
| `hospital_id` | FK a `hospitales` vía `wa_chat_id` |
| `media_url` | URL WAHA de la foto original |
| `photo_url` | Path local descargado en el servidor |

## Notas

- El hospital queda en `hospital_registrado` indefinidamente — loop de recepción de listas sin expiración.
- Un hospital que vuelve a escribir su ubicación actualiza su registro (`upsert` por `wa_chat_id`).
- El conteo de listas recibidas se trackea en `context["hospital_lista_count"]` (no en DB).
