# Dashboard · Marcar "Encontrada (sin avisar)"

Tercer botón de revisión (amarillo), entre **Aprobar** y **Rechazar**. Marca a la
persona buscada como **localizada sin notificar a la familia**.

## Cuándo usarlo

Cuando confirmas que la persona ya apareció pero **no** quieres disparar el aviso
automático a la familia (p. ej. el contacto ya se hizo por otra vía, o el aviso lo
dará una persona). Si quieres que el sistema notifique, usa **Aprobar**.

## Diferencia con las otras acciones

| Acción | `matches.status` | `reports.person_state` | ¿Notifica a la familia? |
|--------|------------------|------------------------|-------------------------|
| Aprobar | `confirmed` | — | ✅ Sí (lo toma el notificador) |
| **Encontrada (sin avisar)** | `found` | `found` (lado buscado) | ❌ No |
| Rechazar | `rejected` | — | ❌ No |

## Cómo funciona

- `POST /admin/match-review` con `decision='found'`:
  - Resuelve el `missing_id` desde el match en el servidor (no confía en el
    cliente) y pone ese `report` en `person_state='found'` → deja de aparecer
    como perdida.
  - Marca el match como `status='found'`.
- El notificador (`notify_pipeline.py`) solo procesa `status='confirmed'`, así que
  `found` **nunca** dispara una notificación.
- `GET /admin/matches` pide `status='pending'` y además descarta candidatos cuyo
  lado buscado ya esté `person_state='found'`. Resultado: al marcar a una persona,
  **todas** sus coincidencias salen de la cola.

## Por qué no se reutiliza `confirmed`

`confirmed` es justamente el disparador del notificador. Reutilizarlo avisaría a la
familia. Por eso se introdujo el estado `found`, distinto y sin notificación.

## Despliegue (requerido la primera vez)

1. Aplicar la migración en Supabase:
   ```sql
   ALTER TYPE match_status ADD VALUE IF NOT EXISTS 'found';
   ```
   (Aditiva e idempotente; no toca datos existentes.)
2. Reiniciar la API: `docker compose up -d --build reune-api`.

> Si reinicias la API sin aplicar la migración, el `PATCH` a `status='found'`
> falla (valor de enum inexistente) y el botón no funciona.

## Archivos

- `migrations/015_match_status_found.sql` — agrega el valor `found` al enum
- `main.py` — `/admin/match-review` (rama `found`) y filtro en `/admin/matches`
- `admin_dashboard.html` — botón, confirmación y toast
