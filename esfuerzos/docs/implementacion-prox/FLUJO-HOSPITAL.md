# Flujo conversacional — Hospital / Institución

Activado cuando el usuario escribe `3` en `bienvenida`.

El flujo no asume que el usuario trabaja en un hospital — cualquier persona con acceso a listas de ingresos puede reportar. El lenguaje es neutral y agradecido.

## Nodos

| Nodo | Entrada | Respuesta | Avanza a |
|---|---|---|---|
| `guia_hospital` | sin texto | "Por favor escribe el nombre del hospital o institución..." | — permanece |
| `guia_hospital` | nombre / texto libre | "✅ Reportando para *Nombre*. Envía fotos de listas..." | `hospital_registrado` |
| `guia_hospital` | GPS compartido por WhatsApp | Igual que arriba, con lat/lng guardados | `hospital_registrado` |
| `hospital_registrado` | imagen | "📋 Lista recibida (N). Puedes seguir enviando más." | — permanece (loop) |
| `hospital_registrado` | `cambiar` | Limpia contexto, vuelve a pedir nombre | `guia_hospital` |
| `hospital_registrado` | cualquier otro texto | "Para enviar listas adjunta una foto. Escribe *cambiar*..." | — permanece |

## Parseo de ubicación

- **Texto libre**: `"Clínica El Ángel"` → se guarda como `nombre` y `ubicacion_texto`, sin coordenadas.
- **GPS compartido**: WAHA entrega coordenadas en `payload.location`. El webhook las convierte a `"Nombre (GPS: lat, lng)"` antes de llegar al handler. El handler extrae `lat`, `lng` y el nombre del prefijo.

## Datos persistidos → Supabase

### `hospitales`

| Campo | Fuente |
|---|---|
| `wa_chat_id` | Chat ID WAHA del usuario |
| `nombre` | Nombre escrito o `location.description` |
| `ubicacion_texto` | Texto completo enviado |
| `lat` / `lng` | Coordenadas GPS si se compartió ubicación |

### `hospital_listas`

| Campo | Fuente |
|---|---|
| `hospital_id` | FK a `hospitales` vía `wa_chat_id` |
| `media_url` | URL WAHA de la foto original |
| `photo_url` | Path local descargado en el servidor |

## Contexto en SQLite

| Clave | Valor |
|---|---|
| `hospital_nombre` | Nombre de la institución activa |
| `hospital_lista_count` | Contador de listas recibidas en la sesión |

Ambas claves se limpian cuando el usuario escribe `cambiar`.

## Listas interactivas

Cada respuesta en `hospital_registrado` (imagen, texto, `cambiar`) incluye una lista interactiva con dos opciones fijas:

| Opción | rowId | Efecto |
|---|---|---|
| Cambiar institución | `cambiar` | Limpia contexto → `guia_hospital` |
| Menú principal | `inicio` | Limpia contexto completo → `bienvenida` |

El payload de la lista se genera dinámicamente con `_hospital_nav_list(title, description)` en `orchestrator.py`, usando el nombre de la institución activa como título.

Ver `LISTAS-INTERACTIVAS.md` para el mecanismo de envío y extracción de `rowId`.

## Notas

- El usuario queda en `hospital_registrado` indefinidamente — loop de recepción de listas sin expiración.
- `cambiar` permite al mismo usuario reportar múltiples instituciones sin reiniciar la conversación.
- Un mismo `wa_chat_id` que reporta otra institución con `cambiar` actualiza el registro en `hospitales` (upsert por `wa_chat_id` — un número = una institución activa a la vez).
