# Resolución de contactos @lid (NOWEB)

## El problema

WhatsApp Multi-Device asigna a cada dispositivo un identificador `@lid` (linked device ID) distinto al número de teléfono. Con NOWEB/Baileys, los mensajes entrantes llegan con `from: "24700877054119@lid"` en lugar de `"584121234567@c.us"`.

El bot usa el número de teléfono como clave primaria de conversación. Sin resolución, dos mensajes del mismo usuario quedan como conversaciones distintas.

---

## Solución: NOWEB store + `/api/contacts`

### 1. Habilitar el store en la sesión

El store de NOWEB mantiene un índice local de contactos `@lid → número`. Se configura al crear/patchear la sesión:

```python
session_config = {
    "webhooks": [...],
    "noweb": {
        "store": {
            "enabled": True,
            "full_sync": True,
        }
    },
}
```

`full_sync: True` sincroniza todos los contactos al conectar, no solo los activos.

### 2. Resolución en el webhook

Cuando `chat_id` termina en `@lid`, el webhook llama `resolve_lid_phone` antes de pasarle el número al Orchestrator:

```python
if chat_id.endswith("@lid"):
    resolved = await resolve_lid_phone(chat_id, session_name)
    if resolved:
        client_phone = resolved
```

`resolve_lid_phone` hace `GET /api/contacts?contactId=<lid>&session=<session>` y retorna el campo `number` de la respuesta.

---

## Limitaciones conocidas

- Si el store no ha sincronizado todavía el contacto (p.ej. primer mensaje tras un re-login), `/api/contacts` devuelve el ID numérico del `@lid` en lugar del número real. El bot opera con ese ID hasta la próxima sincronización.
- El store tarda unos minutos en sincronizar completamente tras el primer QR scan.
- PATCH de sesión devuelve 404 en NOWEB (NOWEB no implementa ese endpoint igual que WEBJS); la sesión funciona correctamente de todas formas porque WAHA re-aplica la config al restart.

---

## Archivos relevantes

| Archivo | Qué hace |
|---|---|
| `app/services/waha.py` → `ensure_default_session` | Incluye config `noweb.store` al crear/patchear sesión |
| `app/services/waha.py` → `resolve_lid_phone` | Llama `/api/contacts` y retorna el número |
| `app/routers/webhook.py` | Llama `resolve_lid_phone` si `chat_id.endswith("@lid")` |
| `docker-compose.yml` | `image: devlikeapro/waha-plus`, `WHATSAPP_DEFAULT_ENGINE: NOWEB` |
