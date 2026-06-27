# Listas interactivas WAHA (sendList)

## Por qué listas

Los nodos con opciones fijas (bienvenida, reporte_guardado, rescatista_guardado, hospital_registrado) presentan las opciones como lista interactiva de WhatsApp en lugar de texto plano. El usuario toca una opción en lugar de escribir un número.

Requiere **WAHA NOWEB** (`devlikeapro/waha-plus`). WEBJS devuelve 501.

---

## Arquitectura

### `_pending_list` en Orchestrator

`Orchestrator` expone `self._pending_list: Optional[dict]` (inicializado a `None` en cada `process_message`). Cuando el FSM decide que corresponde enviar una lista, asigna el payload ahí. El webhook lo lee _después_ de `process_message` y decide si llamar `sendList` o `sendText`.

```
process_message()
  └─ FSM navega → nodo terminal / fallback
       └─ self._pending_list = _LIST_BIENVENIDA  ← Orchestrator
webhook
  └─ orchestrator._pending_list?
       ├─ sí → waha.send_list()  (fallback a sendText si falla)
       └─ no → waha.send_message()
```

### Constantes de lista

Definidas en `orchestrator.py`. Las `rowId` coinciden exactamente con las claves de `next_node_map` del FSM.

| Constante | Nodo que la dispara | rowIds |
|---|---|---|
| `_LIST_BIENVENIDA` | `bienvenida`, `fallback`, `inicio` | `1`, `2`, `3` |
| `_LIST_REPORTE_GUARDADO` | `reporte_guardado` | `1`, `2`, `3`, `inicio` |
| `_LIST_RESCATISTA_GUARDADO` | `rescatista_guardado` | `reporte`, `1`, `3`, `inicio` |
| `_hospital_nav_list(title, desc)` | `hospital_registrado` (cualquier respuesta) | `cambiar`, `inicio` |

### Cuándo se asigna `_pending_list`

- **`inicio` global** → `_LIST_BIENVENIDA`
- **Fallback** → `_LIST_BIENVENIDA`
- **Paso 13 del FSM** (nodo terminal) → según `_list_map` en `orchestrator.py`
- **`_handle_hospital_location`** → `_hospital_nav_list`
- **`_handle_hospital_lista`** (imagen y texto) → `_hospital_nav_list`
- **`_advance_rescatista`** → `_LIST_RESCATISTA_GUARDADO`

---

## Extracción del rowId (problema NOWEB)

WAHA NOWEB entrega la selección del usuario con el **título de la fila en `body`** (`"Hospital o refugio"`), no el `rowId` (`"3"`). El FSM necesita el `rowId` para navegar.

El webhook implementa tres estrategias en orden:

### Estrategia 1 — Baileys directo
```
payload_data._data.listResponseMessage.singleSelectReply.selectedRowId
```

### Estrategia 2 — Baileys envuelto en `message`
```
payload_data._data.message.listResponseMessage.singleSelectReply.selectedRowId
```

### Estrategia 3 — match por título (fallback robusto)

La selección incluye `replyTo._data.listMessage.sections` con el mapa completo `título → rowId` de la lista original. Se busca el título que coincida con `body`.

```python
for section in replyTo._data.listMessage.sections:
    for row in section.rows:
        if row.title == body:
            row_id = row.rowId
```

Esta estrategia es la que actualmente resuelve el problema en producción, ya que NOWEB no expone `listResponseMessage` en el payload normalizado.

---

## Fallback de envío

Si `sendList` falla (ej. engine no compatible), el webhook cae a `sendText` con la respuesta en texto plano si existe. El usuario no queda sin respuesta.
