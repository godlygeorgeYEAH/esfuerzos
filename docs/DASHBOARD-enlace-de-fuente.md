# Dashboard · Enlace de fuente

Cada lado de una tarjeta (Buscado / Encontrado) muestra un enlace **Fuente** que
abre el origen real del registro en una pestaña nueva.

## Problema que resuelve

`reports.source_url` cumple doble función: es también la **clave de
deduplicación** (`ON CONFLICT (source, source_url)`). Por eso, en muchas fuentes,
no es una URL sino un token sintético (`venezuelatebusca:{uuid}`,
`gdrive:{doc_id}:{nombre}`, `hospital_consolidado:{tab}:{cédula}`). Antes el
dashboard lo renderizaba tal cual en un `<a href>`, produciendo enlaces muertos.

## Cómo funciona

- Backend: `resolve_source_url(source, source_url)` en `waha_intake.py` (compartido
  por el bot de WhatsApp y el dashboard) traduce el valor almacenado a una URL
  abrible real:
  - URL `http(s)` → se usa tal cual.
  - Clave sintética conocida → se reconstruye (ej. `gdrive:{id}` → el Google Doc).
  - Fuentes "encontrado" sin página por registro → su documento/sitio de origen
    (xlsx de `hospital_consolidado`, sitio de `pacientes_terremoto`, etc.).
  - Desconocida → `None`.
- `GET /admin/matches` adjunta `source_link` a cada lado.
- Frontend (`admin_dashboard.html`): solo genera `<a>` cuando `source_link` es una
  URL `http(s)` válida; si no, muestra el nombre de la fuente como **texto plano**
  (nunca un enlace roto).

## Agregar una fuente nueva

Edita `resolve_source_url` en `waha_intake.py` y añade el caso para ese `source`
(reconstrucción del enlace real o, en su defecto, el sitio de origen).

## Archivos

- `waha_intake.py` — `resolve_source_url`
- `main.py` — `/admin/matches` (campo `source_link`)
- `admin_dashboard.html` — render del enlace
