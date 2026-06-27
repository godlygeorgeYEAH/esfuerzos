# Navegación entre flujos

## Comandos

| Comando | Disponible en | Efecto |
|---|---|---|
| `inicio` | cualquier nodo | Limpia contexto completo → `bienvenida` |
| `1` / `2` / `3` | `reporte_guardado`, `rescatista_guardado` | Navega directo al flujo sin pasar por `bienvenida` |
| `cambiar` | `hospital_registrado` | Limpia institución activa → `guia_hospital` |

## Menú proactivo

`reporte_guardado` y `rescatista_guardado` incluyen el menú 1/2/3 + `inicio` en su mensaje de cierre. El usuario no necesita recordar comandos.

## Implementación

- `orchestrator.py` — bloque `inicio` global antes del Paso 6 (después del check de `escalated`)
- `flow_seeder.py` — mensajes de `reporte_guardado` y `rescatista_guardado` actualizados; `next_node_map` de `rescatista_guardado` con `1`, `2`, `3`
