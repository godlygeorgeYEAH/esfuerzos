# Flujo conversacional — Rescatista

Activado cuando el usuario escribe `2` en `bienvenida`.

## Nodos

| Nodo | Entrada | Respuesta | Avanza a |
|---|---|---|---|
| `guia_rescatista` | imagen sin caption | "📸 Imagen recibida (N). Puedes enviar más fotos o escribir texto para complementar el reporte." | — refresca TTL |
| `guia_rescatista` | imagen con caption | Repite el caption al usuario. "Puedes agregar más texto o enviar más fotos." | — refresca TTL |
| `guia_rescatista` | texto libre (sin fotos) | Guarda como descripción. "Si tienes una foto, envíala ahora. Escribe *listo* para registrar sin foto." | — |
| `guia_rescatista` | texto libre (con fotos, TTL < 60 s) | "Descripción guardada. Tienes N foto(s). Escribe *listo* para finalizar." | — |
| `guia_rescatista` | texto libre (con fotos, TTL ≥ 60 s) | Auto-commit | `rescatista_guardado` |
| `guia_rescatista` | `listo` | Commit del reporte | `rescatista_guardado` |
| `guia_rescatista` | `reporte` | Limpia contexto, muestra mensaje de bienvenida de nuevo | — reinicia |
| `rescatista_guardado` | cualquier texto / `reporte` | Confirmación de caso registrado | `guia_rescatista` |

## Datos recopilados

| Campo | Fuente |
|---|---|
| Foto(s) | Imágenes enviadas — descargadas localmente |
| Descripción | Texto libre o caption de imagen |
| `kind` | `"found"` (fijo para rescatistas) |
| `reporter_wa_hash` | SHA-256 del número del rescatista |

## TTL

La ventana de 60 segundos se abre con la primera imagen. Se refresca con cada imagen adicional. Se cierra cuando el rescatista escribe cualquier texto y han pasado ≥ 60 s desde la última foto — el reporte se guarda automáticamente.

## Notas

- Imagen y texto son intercambiables: el rescatista puede empezar por cualquiera.
- Captions de imágenes múltiples se concatenan en un solo campo de descripción.
- El flujo no requiere un orden específico; cualquier combinación de foto + texto es válida.
