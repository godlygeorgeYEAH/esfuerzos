# Flujo conversacional — Rescatista

Activado cuando el usuario escribe `2` en `bienvenida`.

## Nodos

| Nodo | Entrada | Respuesta | Avanza a |
|---|---|---|---|
| `guia_rescatista` | imagen sin caption | "📸 Imagen recibida (N). Puedes enviar más fotos o texto. Escribe *listo* cuando termines." | — refresca TTL |
| `guia_rescatista` | imagen con caption | Echo del caption. "Puedes agregar más texto o fotos. Escribe *listo* para terminar." | — refresca TTL |
| `guia_rescatista` | texto libre (sin fotos) | "📝 Descripción recibida. Si tienes foto, envíala. Escribe *listo* para registrar sin foto." | — |
| `guia_rescatista` | texto libre (con fotos, TTL < 60 s) | "📝 Descripción guardada. Tienes N foto(s). Escribe *listo* para finalizar." | — |
| `guia_rescatista` | texto libre (con fotos, TTL ≥ 60 s) | Auto-commit silencioso | `rescatista_guardado` |
| `guia_rescatista` | `listo` | Commit del reporte | `rescatista_guardado` |
| `guia_rescatista` | `reporte` | Limpia contexto, reenvía mensaje de `guia_rescatista` | — reinicia en el mismo nodo |
| `rescatista_guardado` | cualquier texto / `reporte` | Confirmación de caso registrado | `guia_rescatista` |

## Datos recopilados → `reunion_reports`

| Campo | Fuente |
|---|---|
| Foto(s) | Imágenes enviadas — descargadas localmente, subidas a Supabase Storage |
| Descripción | Texto libre + captions de imágenes (concatenados) |
| `kind` | `"found"` (fijo para rescatistas) |
| `reporter_wa_hash` | SHA-256 del `wa_chat_id` del rescatista |

## TTL

La ventana de 60 s se abre con la **primera imagen**. Cada imagen adicional la refresca. El auto-commit se dispara cuando el rescatista envía **texto** y han pasado ≥ 60 s desde la última foto — no por tiempo solo, sino por la combinación texto + tiempo transcurrido.

## Notas

- Imagen y texto son intercambiables: el rescatista puede empezar por cualquiera.
- Captions de imágenes se concatenan al texto libre en `intake_person_raw`.
- El flujo no requiere un orden específico; cualquier combinación de foto + texto es válida.
- `reporte` reinicia el nodo sin salir de `guia_rescatista` (limpia `pending_photos`, `last_photo_at`, `intake_person_raw`).
