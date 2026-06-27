# Flujo conversacional — Familiar

Activado cuando el usuario escribe `1` en `bienvenida`.

## Nodos

| Nodo | Entrada | Respuesta | Avanza a |
|---|---|---|---|
| `guia_familiar` | cualquier texto | Instrucciones: nombre, género, edad, última ubicación en un solo mensaje | `pedir_foto` |
| `pedir_foto` | imagen (< límite) | "📸 Imagen recibida (N/MAX). Puedes enviar más o escribe *listo*." | — permanece |
| `pedir_foto` | imagen (= límite MAX) | Auto-avance por límite alcanzado | `notas_adicionales` |
| `pedir_foto` | `listo` | Avance manual | `notas_adicionales` |
| `pedir_foto` | texto distinto a `listo` | "⏳ Tienes N/MAX foto(s). Puedes enviar más o escribe *listo*." | — permanece |
| `notas_adicionales` | texto libre | Guarda señas/ropa como notas. Commit del reporte. | `reporte_guardado` |
| `notas_adicionales` | `reporte` | Limpia contexto | `guia_familiar` (nuevo caso) |
| `reporte_guardado` | `reporte` / `1` | — | `guia_familiar` |
| `reporte_guardado` | `2` | — | `guia_rescatista` |
| `reporte_guardado` | `3` | — | `guia_hospital` |
| `reporte_guardado` | cualquier otro texto | — | `bienvenida` |

## Datos recopilados → `reunion_reports`

| Campo | Fuente |
|---|---|
| Datos del desaparecido | Texto libre del paso `guia_familiar` — almacenado en `intake_person_raw` |
| Foto(s) | Imágenes del paso `pedir_foto` — descargadas localmente, primera foto a Supabase Storage |
| Notas | Texto libre del paso `notas_adicionales` |
| `kind` | `"missing"` (fijo para familiares) |
| `reporter_wa_hash` | SHA-256 del `wa_chat_id` del familiar |

## Límite de fotos

Configurable via `PHOTO_MAX_COUNT` (default: 5). Al alcanzar el límite, el bot avanza automáticamente a `notas_adicionales` sin esperar `listo`.

## Notas

- El familiar puede omitir las fotos escribiendo `listo` inmediatamente.
- El reporte se guarda al salir de `notas_adicionales` (con o sin notas adicionales).
- Desde `reporte_guardado` el familiar puede iniciar otro caso sin pasar por `bienvenida`.
