# ProX — Problemáticas de Migración

**Origen:** `godlygeorgeYEAH/foob_v2` (módulo `backend/app/bot/`)  
**Destino:** `godlygeorgeYEAH/esfuerzos`  
**Preparado:** Junio 2026

---

## Índice

1. [Visión general del paquete](#1-visión-general-del-paquete)
2. [La entidad Negocio (tenant base)](#2-la-entidad-negocio-tenant-base)
3. [Dependencia de Orden y Pago](#3-dependencia-de-orden-y-pago)
4. [Relación Cliente → Orden en SQLAlchemy](#4-relación-cliente--orden-en-sqlalchemy)
5. [Sistema de conductores en el webhook](#5-sistema-de-conductores-en-el-webhook)
6. [Sistema de notificaciones](#6-sistema-de-notificaciones)
7. [Modelo Articulo en flow_engine (nodo legacy)](#7-modelo-articulo-en-flow_engine-nodo-legacy)
8. [BotConfig no tiene seeder automático](#8-botconfig-no-tiene-seeder-automático)
9. [Notificaciones proactivas NO incluidas en el módulo](#9-notificaciones-proactivas-no-incluidas-en-el-módulo)
10. [WAHA_FREE_TIER vs multi-tenant](#10-waha_free_tier-vs-multi-tenant)
11. [Storage de comprobantes](#11-storage-de-comprobantes)
12. [Rate limiting (slowapi)](#12-rate-limiting-slowapi)
13. [BotConfig.enable_intent_detection — doble nivel de control](#13-botconfigenable_intent_detection--doble-nivel-de-control)
14. [Merge conflict sin resolver en la documentación fuente](#14-merge-conflict-sin-resolver-en-la-documentación-fuente)
15. [doble import de get_settings en webhook.py](#15-doble-import-de-get_settings-en-webhookpy)
16. [Variables de entorno completas](#16-variables-de-entorno-completas)
17. [Guía de integración paso a paso](#17-guía-de-integración-paso-a-paso)

---

## 1. Visión general del paquete

El paquete de migración contiene los siguientes archivos, organizados como deben
quedar en el nuevo repo. Los archivos marcados con `[ADAPTADO]` fueron modificados
respecto al original; los demás son copias exactas.

```
migration_prox/
├── requirements.txt
└── app/
    ├── config.py            [ADAPTADO] Solo settings del bot; sin auth, sin mapbox
    ├── database.py          [COPIA]
    ├── main.py              [NUEVO]    FastAPI mínimo solo con webhook + media
    ├── bot/                 [COPIA]    Todo el directorio, sin modificaciones
    │   ├── orchestrator.py
    │   ├── flow_engine.py
    │   ├── flow_seeder.py
    │   ├── intent_detector.py
    │   ├── decision_engine.py
    │   ├── context_manager.py
    │   ├── response_generator.py
    │   ├── abc_layer.py
    │   ├── analytics_logger.py
    │   ├── message_parser.py
    │   ├── faq_matcher.py
    │   ├── template_renderer.py
    │   └── dev_logger.py
    ├── routers/
    │   └── webhook.py       [COPIA]
    ├── core/
    │   ├── clientes.py      [COPIA]
    │   ├── conductores.py   [COPIA]   ver §5 si no hay sistema de conductores
    │   ├── phone.py         [COPIA]
    │   ├── waha_resolver.py [COPIA]
    │   └── notificaciones.py [COPIA]  ver §6 si no hay sistema de notificaciones
    ├── models/
    │   ├── bot.py           [COPIA]
    │   ├── cliente.py       [ADAPTADO] ver §4
    │   ├── conductor.py     [COPIA]   ver §5
    │   ├── negocio.py       [NUEVO]   stub mínimo; ver §2
    │   ├── notificacion.py  [COPIA]   ver §6
    │   └── orden.py         [NUEVO]   stub mínimo; ver §3
    └── services/
        ├── waha.py          [COPIA]
        ├── storage.py       [COPIA]
        └── deepseek.py      [COPIA]
```

---

## 2. La entidad Negocio (tenant base)

**Severidad: BLOQUEANTE**

Todo el sistema opera bajo el concepto de multi-tenant: cada instancia de bot,
configuración y conversación está vinculada a un `negocio_id`. Los modelos
`BotConfig`, `BlockedClient`, `Conversacion`, `NegocioFlow`, `PreguntaFrecuente`,
`Cliente`, `Conductor` y `Notificacion` tienen FK a `negocios.id`.

### Campos que el bot lee activamente de `Negocio`:

| Campo | Leído en | Para qué |
|---|---|---|
| `id` | todos | FK base de todos los modelos |
| `nombre` | `flow_engine._generate_response()` | `{business_name}`, `{bot_name}` en templates |
| `slug` | `flow_engine._generate_response()` | Construye `{webapp_link}` = `{WEBAPP_BASE_URL}/menu/{slug}` |
| `is_active` | `waha_resolver.resolve_negocio()` | Solo procesa mensajes de negocios activos |
| `waha_session` | `waha_resolver.resolve_negocio()` | Multi-tenant: enruta mensaje al negocio por nombre de sesión |
| `metodos_pago` | `flow_engine._generate_response()`, `_build_payment_method_messages()` | JSON `["efectivo","zelle"]` — lista de métodos activos |
| `datos_pago` | `flow_engine._generate_response()`, `_build_payment_method_messages()` | JSON `{"zelle":"correo@x.com"}` — datos por método |
| `delivery_enabled` | `flow_engine._generate_response()` | Nodo `location` legacy |
| `retiro_enabled` | `flow_engine._generate_response()` | Nodo `location` legacy |
| `negocio_lat` | `flow_engine._generate_response()` | Nodos `orden_lista_retiro` e `info_negocio` |
| `negocio_lng` | `flow_engine._generate_response()` | Nodos `orden_lista_retiro` e `info_negocio` |
| `direccion` | `flow_engine._generate_response()` | Fallback cuando no hay coords GPS |

### Acción requerida:

**Opción A (recomendada):** Si el nuevo repo ya tiene una entidad equivalente
(`Tenant`, `Business`, etc.), renombrar la tabla en `models/negocio.py` y ajustar
los imports en:
- `app/bot/flow_engine.py` (línea `from app.models.negocio import Negocio`)
- `app/core/waha_resolver.py` (línea `from app.models.negocio import Negocio`)

**Opción B:** Usar el stub provisto en `models/negocio.py` tal cual y agregar
los campos que falten en la tabla del nuevo repo vía migración Alembic.

---

## 3. Dependencia de Orden y Pago

**Severidad: ALTA — afecta flujo principal (comprobante de pago)**

`orchestrator._handle_comprobante()` crea o actualiza un registro `Pago` vinculado
a la `Orden` cuando el cliente envía el comprobante. Sin esto el comprobante
se descarga pero no queda registrado en DB.

### Imports activos en `orchestrator.py`:

```python
# Dentro de _handle_comprobante() — imports lazy en try/except:
from app.models.orden import Pago as _Pago, Orden as _Orden    # fallback LID
from app.models.orden import Pago, Orden, EstadoPago            # guardado normal
```

### Campos de Orden que usa el bot:

| Campo | Uso |
|---|---|
| `id` | FK en `Pago.orden_id` |
| `negocio_id` | Filtro en el fallback LID (query por negocio) |
| `total` | `Pago.monto = float(orden.total)` |
| `estado` | No se lee — solo se usa `Orden.id` para la FK |

### Campos de Pago que usa el bot:

| Campo | Uso |
|---|---|
| `orden_id` | FK única; un Pago por Orden |
| `metodo` | Se puebla desde `ctx.get('metodo_pago', 'no especificado')` |
| `monto` | `float(orden.total)` |
| `comprobante_url` | URL del archivo descargado/subido al storage |
| `estado` | Siempre inicia como `EstadoPago.PENDIENTE` |

### Opciones:

**Opción A (completa):** Incluir los modelos `Orden` y `Pago` del stub.
Migración mínima: solo necesita `ordenes` y `pagos` con los campos listados.

**Opción B (desacoplada):** Si el nuevo repo no tiene sistema de órdenes,
modificar `_handle_comprobante()` en `orchestrator.py` para omitir la creación
del registro `Pago`. El comprobante seguirá descargándose al storage y la URL
quedará en `Conversacion.context["comprobante_url"]`. El bot funcionará igual
desde la perspectiva del cliente; solo se pierde el registro DB de `Pago`.

Cambio concreto para Opción B — en `orchestrator.py`, reemplazar el bloque
que empieza con `if orden_id_str:` por un simple log:
```python
if orden_id_str:
    logger.info("Comprobante recibido para orden %s (registro Pago omitido)", orden_id_str)
```

---

## 4. Relación Cliente → Orden en SQLAlchemy

**Severidad: MEDIA — impide que SQLAlchemy mapee si Orden no existe**

`models/cliente.py` declara:
```python
ordenes: Mapped[list["Orden"]] = relationship(back_populates="cliente")
```

SQLAlchemy resuelve `"Orden"` como string al inicio de la aplicación. Si el
modelo `Orden` no está importado en ningún `__init__.py` o al momento de crear
el mapper, falla silenciosamente o con un error `InvalidRequestError`.

### Acción requerida:

Si NO se porta `Orden`, eliminar la relación de `models/cliente.py`:
```python
# ELIMINAR estas líneas:
ordenes: Mapped[list["Orden"]] = relationship(back_populates="cliente")
```

Si SÍ se porta `Orden`, asegurarse de que `models/orden.py` sea importado antes
de que SQLAlchemy cree los mappers (normalmente alcanza con importarlo en `main.py`
o en `models/__init__.py`).

---

## 5. Sistema de conductores en el webhook

**Severidad: MEDIA — el webhook no arranca si falta el modelo `Conductor`**

`routers/webhook.py` importa al inicio del handler:
```python
from app.models.conductor import Conductor
from app.core.conductores import es_respuesta_conductor, procesar_respuesta_conductor
from app.core.phone import normalize_phone
```

Antes de pasar el mensaje al Orchestrator, verifica si el remitente es un conductor
activo del negocio. Si el nuevo repo no tiene sistema de conductores, el webhook
fallará al importar `Conductor`.

`core/conductores.py` también depende de:
- `app.models.orden.Orden`, `app.models.orden.EstadoOrden`
- `app.models.notificacion.TipoNotificacion`
- `app.core.notificaciones.crear_notificacion`

### Acción requerida:

**Si se mantiene conductores:** Incluir `models/conductor.py` (ya en el paquete)
y asegurarse de que la tabla `conductores` exista en la DB.

**Si NO se quieren conductores:** Modificar `routers/webhook.py` eliminando el
bloque de verificación de conductor (aproximadamente líneas 55–72 del archivo):

```python
# ELIMINAR este bloque completo:
from app.models.conductor import Conductor
from app.core.conductores import es_respuesta_conductor, procesar_respuesta_conductor
from app.core.phone import normalize_phone

conductor = db.query(Conductor).filter(
    Conductor.telefono == normalize_phone(client_phone),
    Conductor.negocio_id == negocio.id,
    Conductor.is_active == True,
).first()

if conductor and message_text and es_respuesta_conductor(message_text):
    respuesta = await procesar_respuesta_conductor(db, conductor, message_text, session_name)
    if respuesta:
        from app.services.waha import send_message as waha_send
        await waha_send(phone=chat_id, message=respuesta, session=session_name)
    return {"status": "processed_conductor"}
```

---

## 6. Sistema de notificaciones

**Severidad: BAJA — el bot funciona aunque fallen las notificaciones**

El Orchestrator crea notificaciones en dos puntos, ambos dentro de bloques
`try/except` — si la creación falla, el error se loguea pero el flujo continúa:

**Punto 1 — Escalado a humano** (`orchestrator.process_message()`, nodo `escalado_humano`):
```python
from app.core.notificaciones import crear_notificacion
from app.models.notificacion import TipoNotificacion
crear_notificacion(db, negocio_id=..., tipo=TipoNotificacion.CONVERSACION_ESCALADA, ...)
```

**Punto 2 — Comprobante recibido** (`orchestrator._handle_comprobante()`):
```python
from app.core.notificaciones import crear_notificacion
from app.models.notificacion import TipoNotificacion
crear_notificacion(db, negocio_id=..., tipo=TipoNotificacion.COMPROBANTE_RECIBIDO, ...)
```

**Punto 3 — Conductor acepta/rechaza/entrega** (`core/conductores.py`):
```python
crear_notificacion(db, tipo=TipoNotificacion.CONDUCTOR_ACEPTO, ...)
```

### Acción requerida:

**Opción A (mantener):** Incluir `models/notificacion.py` y `core/notificaciones.py`
(ya en el paquete). La tabla `notificaciones` debe existir en la DB.

**Opción B (eliminar):** Si el nuevo repo tiene su propio sistema de notificaciones,
reemplazar las llamadas `crear_notificacion(...)` por el equivalente propio.

**Opción C (deshabilitar silenciosamente):** Los bloques ya están en try/except.
Si el import de `crear_notificacion` falla, el proceso se interrumpe dentro del
try/except y el error se loguea. Para deshabilitar de forma limpia, crear un
stub vacío en `core/notificaciones.py`:

```python
def crear_notificacion(db, **kwargs):
    pass  # no-op
```

---

## 7. Modelo Articulo en flow_engine (nodo legacy)

**Severidad: BAJA — solo afecta nodos legacy que probablemente no se usen**

`bot/flow_engine.py` importa `Articulo` para el nodo `service_list`:
```python
from app.models.menu import Articulo
```

Este nodo es del sistema anterior a Phase 2 y no está en el `flow_seeder.py`
actual. Probablemente no se usará en el nuevo repo.

### Acción requerida:

Eliminar en `flow_engine.py` la línea de import y el branch completo del nodo
`service_list` dentro del método `_generate_response()`:

```python
# ELIMINAR import:
from app.models.menu import Articulo

# ELIMINAR bloque en _generate_response():
elif node.node_key == "service_list":
    articulos = self.db.query(Articulo).filter(
        Articulo.negocio_id == negocio_id,
        Articulo.is_active == True,
    ).order_by(Articulo.id).all()
    variables['articulos_list'] = render_articulo_list(articulos)
    variables['services_list'] = variables['articulos_list']
```

Si el nuevo repo sí necesita un nodo de listado de productos, puede reimplementar
este branch con su propio modelo de producto.

---

## 8. BotConfig no tiene seeder automático

**Severidad: ALTA — sin BotConfig el bot responde con silencio a todos los mensajes**

`flow_seeder.py` crea `FlowTemplate` y `FlowNode` al arrancar. Sin embargo,
`BotConfig` (configuración del bot por negocio: horario, bot activo, away message)
NO tiene seeder.

`orchestrator.process_message()` — paso 2:
```python
bot_config = self.engine._get_bot_config(negocio_id)
bot_activo = bool(bot_config and bot_config.is_bot_active)
if not bot_activo:
    return "", False   # bot no responde
```

Si no hay `BotConfig` para el negocio → `bot_config = None` → `bot_activo = False`
→ el bot ignora todos los mensajes sin error visible.

### Acción requerida:

Crear un endpoint de administración o un seeder que cree el `BotConfig` inicial
al registrar un negocio. Configuración mínima:

```python
bot_config = BotConfig(
    negocio_id=negocio_id,
    is_bot_active=True,
    enable_intent_detection=True,
)
db.add(bot_config)
db.commit()
```

---

## 9. Notificaciones proactivas NO incluidas en el módulo

**Severidad: ALTA — el flujo de pago no funciona sin estas integraciones**

Existen dos puntos donde el **backend** (no el cliente) inicia el envío de mensajes.
Estos no están en el módulo bot — viven en otros routers de `foob_v2` y deben
ser recreados en el nuevo repo.

### 9.1 — Al crear una orden: `notificar_cliente_orden`

Cuando el cliente confirma su carrito, el backend envía proactivamente el resumen
de la orden al WhatsApp del cliente y lo avanza al nodo `esperar_comprobante`.

Vive en: `foob_v2/backend/app/routers/ordenes.py` como `BackgroundTask`.

**Implementar en el nuevo repo:**

```python
async def notificar_cliente_orden(
    orden_id: int,
    negocio_id: int,
    cliente_telefono: str,
    orden_numero: str,
    subtotal: float,
    tarifa_delivery: float,
    total: float,
    items_list: str,            # texto formateado con los artículos
    modalidad_entrega: str,     # "delivery" | "retiro"
    session: str = "default",
    db: Session = None,
):
    from app.bot.flow_engine import FlowEngine
    from app.services.waha import send_message

    engine = FlowEngine(db)
    phone = cliente_telefono.lstrip("+")

    conversation = engine._get_or_create_conversation(negocio_id, phone)
    conversation.current_node_key = "pedido_recibido"
    conversation.context = json.dumps({
        "orden_numero": orden_numero,
        "subtotal": str(subtotal),
        "tarifa_delivery": str(tarifa_delivery),
        "total": str(total),
        "items_list": items_list,
        "modalidad_entrega": modalidad_entrega,
    })
    db.commit()

    # Mensaje principal (resumen de la orden)
    node = engine._get_node_by_key(negocio_id, "pedido_recibido")
    if node:
        mensaje = engine._generate_response(node, negocio_id, conversation)
        await send_message(phone=phone, message=mensaje, session=session)

    # Un mensaje por cada método de pago activo
    for msg in engine._build_payment_method_messages(negocio_id):
        await send_message(phone=phone, message=msg, session=session)

    # Avanzar al nodo de espera
    conversation.current_node_key = "esperar_comprobante"
    db.commit()
```

### 9.2 — Al cambiar estado de orden: `_notificar_resultado`

Cuando el operador confirma, rechaza o marca en camino/lista la orden desde
el Dashboard, el backend envía un mensaje proactivo al cliente.

Vive en: `foob_v2/backend/app/routers/dashboard.py` como función interna.

**Mapa de estado → nodo bot:**

```python
_NODE_KEY_MAP = {
    "confirmada":  "orden_confirmada",
    "rechazada":   "orden_rechazada",
    "en_camino":   "orden_en_camino",
    "lista":       "orden_lista_retiro",
}
```

**Implementar en el nuevo repo** (al cambiar estado de una orden):

```python
async def notificar_resultado(
    db: Session,
    negocio_id: int,
    cliente_telefono: str,
    resultado: str,          # "confirmada" | "rechazada" | "en_camino" | "lista"
    orden_numero: str,
    session: str = "default",
    cliente_referencia: str | None = None,
):
    from app.bot.flow_engine import FlowEngine
    from app.services.waha import send_message

    node_key = _NODE_KEY_MAP.get(resultado)
    if not node_key:
        return

    engine = FlowEngine(db)
    phone = cliente_telefono.lstrip("+")
    conversation = engine._get_or_create_conversation(negocio_id, phone)

    ctx = engine._get_context(conversation)
    ctx["orden_numero"] = orden_numero
    if cliente_referencia:
        ctx["cliente_referencia"] = cliente_referencia
    conversation.context = json.dumps(ctx)
    conversation.current_node_key = node_key
    db.commit()

    node = engine._get_node_by_key(negocio_id, node_key)
    if node:
        mensaje = engine._generate_response(node, negocio_id, conversation)
        await send_message(phone=phone, message=mensaje, session=session)

    if resultado in ("confirmada", "rechazada"):
        conversation.current_node_key = (
            "esperar_comprobante" if resultado == "rechazada" else "bienvenida"
        )
        if resultado == "confirmada":
            conversation.status = "converted"
        db.commit()
```

---

## 10. WAHA_FREE_TIER vs multi-tenant

**Severidad: DECISIÓN DE ARQUITECTURA**

`core/waha_resolver.py` tiene dos modos controlados por `settings.waha_free_tier`:

**`WAHA_FREE_TIER=True` (single-tenant, desarrollo):**
- WAHA tiene una sola sesión llamada `"default"`
- El resolver busca el único negocio con `is_active=True`
- Si hay 0 o más de 1 negocio activo → error, mensaje descartado

**`WAHA_FREE_TIER=False` (multi-tenant, producción):**
- Cada negocio tiene su propia sesión WAHA con nombre único
- El resolver hace `Negocio.waha_session == session_name`
- Escala a N negocios simultáneos

### Acción requerida:

Definir la arquitectura del nuevo repo. Si siempre es single-tenant, se puede
simplificar `waha_resolver.py` para que siempre retorne el único negocio activo
sin la bifurcación. Setear `WAHA_FREE_TIER=True` en `.env`.

---

## 11. Storage de comprobantes

**Severidad: MEDIA — sin esto los comprobantes no persisten entre reinicios**

`services/storage.py` soporta dos backends:

**LocalStorage (dev):**
- Guarda en `/app/media/comprobantes/{uuid}.ext`
- Requiere volumen Docker: `./media/:/app/media/`
- Requiere `StaticFiles` en FastAPI: `app.mount("/media", StaticFiles(...))`
- La URL resultante es `{MEDIA_BASE_URL}/media/comprobantes/{uuid}.ext`

**S3Storage (prod — DigitalOcean Spaces u otro S3-compatible):**
- Requiere `aioboto3` instalado: `pip install aioboto3`
- Variables requeridas: `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`,
  `S3_BUCKET_NAME`, `S3_PUBLIC_BASE_URL`
- La URL resultante es `{S3_PUBLIC_BASE_URL}/comprobantes/{uuid}.ext`
- El bucket debe tener ACL `public-read` o equivalente

### Acción requerida:

1. Para LocalStorage: agregar volume mount en `docker-compose.yml` y el mount
   de `StaticFiles` en `main.py` (ya incluido en el stub).
2. Para S3: instalar `aioboto3`, setear las variables de entorno S3, y cambiar
   `STORAGE_BACKEND=s3` en `.env`.

---

## 12. Rate limiting (slowapi)

**Severidad: BAJA — el webhook arranca sin esto pero no tiene protección**

`routers/webhook.py` decora el endpoint con:
```python
@limiter.limit("60/minute")
```

Esto requiere:
1. `slowapi` instalado (`pip install slowapi`)
2. El limiter inicializado en `main.py`:
   ```python
   from slowapi import Limiter, _rate_limit_exceeded_handler
   from slowapi.errors import RateLimitExceeded
   from slowapi.util import get_remote_address
   limiter = Limiter(key_func=get_remote_address)
   app.state.limiter = limiter
   app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
   ```

El `main.py` del paquete ya incluye esto. Si el nuevo repo integra el webhook
en su propio `main.py`, debe agregar estas líneas.

---

## 13. BotConfig.enable_intent_detection — doble nivel de control

**Severidad: INFORMATIVA**

El intent detection tiene dos niveles de control:

1. **Global (env var):** `ENABLE_INTENT_DETECTION` en `.env` — no usado directamente
   por el Orchestrator (era del sistema anterior). Existe en Settings para compatibilidad.

2. **Por negocio (DB):** `BotConfig.enable_intent_detection` — este es el que
   efectivamente controla si DeepSeek se llama.

El Orchestrator lee:
```python
intent_enabled = bool(bot_config and getattr(bot_config, 'enable_intent_detection', False))
```

Para activar el LLM, el `BotConfig` del negocio debe tener `enable_intent_detection=True`.
Si se crea el BotConfig con `enable_intent_detection=False` (o sin el campo), el bot
usará solo similarity matching (difflib) — sin costo de API, sin latencia de LLM.

---

## 14. Merge conflict sin resolver en la documentación fuente

**Severidad: INFORMATIVA — no afecta código**

El archivo `foob_v2/docs/modulos/02_bot_conversacional.md` contiene marcadores
de conflicto git sin resolver:

```
<<<<<<< HEAD
  ├─ cliente_telefono ya es E.164 con "+" ...
=======
  ├─ Normaliza teléfono: strip("+") ...
  ├─ get_or_create_cliente(db, negocio_id, telefono, nombre) ...
  ...
>>>>>>> d7fb5b1 (chore: actualizacion documentacion bot conversacional)
```

El código en sí no tiene conflictos — solo la documentación markdown. Resolver
manualmente al copiar o ignorar (no afecta funcionalidad).

---

## 15. Doble import de get_settings en webhook.py

**Severidad: COSMÉTICA**

`routers/webhook.py` importa `get_settings` dos veces:
```python
# Al inicio del módulo:
from app.config import get_settings

# Dentro de funciones:
from app.config import get_settings as _get_settings
```

El módulo importa `_settings = get_settings()` al nivel del módulo y luego
re-importa dentro de los handlers. Funciona correctamente (lru_cache retorna
la misma instancia), pero es código duplicado. Se puede limpiar usando siempre
`settings` (la instancia de nivel de módulo) en lugar de re-importar.

---

## 16. Variables de entorno completas

Archivo `.env` mínimo para arrancar el sistema:

```env
# Base de datos
DATABASE_URL=postgresql://user:pass@localhost:5432/prox

# WAHA
WAHA_URL=http://waha:3000
WAHA_API_KEY=                        # opcional, si WAHA tiene auth
WAHA_FREE_TIER=true                  # true = single tenant
# WAHA_WEBHOOK_SECRET=secreto        # opcional, para verificar tokens

# DeepSeek LLM
DEEPSEEK_API_KEY=sk-...
DEEPSEEK_BASE_URL=https://api.deepseek.com/v1
DEEPSEEK_MODEL=deepseek-chat
DEEPSEEK_TIMEOUT=15
DEEPSEEK_MAX_RETRIES=3

# Bot behavior
FORCE_LLM_RESPONSES=false            # true = LLM en todos los nodos
DEV_FLOW_LOG=false                   # true = logs verbose del pipeline
BOT_SELF_MESSAGE_TESTING=false       # true = mensajes fromMe con "/" se procesan

# URLs
WEBAPP_BASE_URL=http://localhost:4200
MEDIA_BASE_URL=http://localhost:8000

# Storage
STORAGE_BACKEND=local                # local | s3

# S3 (solo si STORAGE_BACKEND=s3)
# S3_ENDPOINT_URL=https://nyc3.digitaloceanspaces.com
# S3_ACCESS_KEY=...
# S3_SECRET_KEY=...
# S3_BUCKET_NAME=mi-bucket
# S3_PUBLIC_BASE_URL=https://mi-bucket.nyc3.cdn.digitaloceanspaces.com

# Entorno
ENVIRONMENT=development
```

---

## 17. Guía de integración paso a paso

### Paso 1 — Definir arquitectura

Antes de escribir código, responder:
- ¿El nuevo repo es single-tenant o multi-tenant? → determina `WAHA_FREE_TIER`
- ¿Hay sistema de órdenes/pagos? → determina si portar `Orden`+`Pago`
- ¿Hay sistema de conductores? → determina si portar `Conductor` + limpiar webhook
- ¿Hay sistema de notificaciones? → determina qué hacer con `crear_notificacion`

### Paso 2 — Copiar el paquete

```bash
cp -r migration_prox/app/* <nuevo_repo>/app/
```

### Paso 3 — Resolver la entidad Negocio

Si el nuevo repo ya tiene una entidad tenant:
```python
# En flow_engine.py, reemplazar:
from app.models.negocio import Negocio
# Por:
from app.models.mi_entidad import MiEntidad as Negocio
```

Si no la tiene, usar el stub de `models/negocio.py` y crear la migración Alembic.

### Paso 4 — Crear BotConfig inicial

Por cada negocio que exista, crear un `BotConfig`:
```python
bot_config = BotConfig(
    negocio_id=negocio.id,
    is_bot_active=True,
    enable_intent_detection=True,
    working_days='["monday","tuesday","wednesday","thursday","friday","saturday"]',
    working_hours_start="09:00",
    working_hours_end="21:00",
)
db.add(bot_config)
db.commit()
```

### Paso 5 — Migraciones Alembic

Tablas que el bot necesita crear:
- `bot_config`
- `blocked_clients`
- `conversaciones`
- `mensajes_conversacion`
- `eventos_conversacion`
- `flow_templates`
- `flow_nodes`
- `negocio_flows`
- `preguntas_frecuentes`
- `clientes`
- `clientes_ubicaciones`
- (opcionales) `conductores`, `notificaciones`, `ordenes`, `pagos`

### Paso 6 — Configurar el lifespan

En el `main.py` del nuevo repo, agregar al lifespan:
```python
from app.bot.flow_seeder import seed_default_flow
seed_default_flow(db)
```

### Paso 7 — Registrar el webhook

```python
from app.routers import webhook
app.include_router(webhook.router, prefix="/webhook", tags=["webhook"])
```

### Paso 8 — Storage

Para LocalStorage, agregar en `docker-compose.yml`:
```yaml
volumes:
  - ./media:/app/media
```

Y en `main.py`:
```python
app.mount("/media", StaticFiles(directory="/app/media"), name="media")
```

### Paso 9 — Implementar notificaciones proactivas

Implementar `notificar_cliente_orden` (§9.1) y `notificar_resultado` (§9.2)
en los routers de órdenes del nuevo repo.

### Paso 10 — Smoke test

1. Arrancar WAHA apuntando al nuevo backend (`WAHA_WEBHOOK_URL`)
2. Enviar "hola" desde un número en WhatsApp → bot debe responder con `bienvenida`
3. Responder "menu" → bot debe enviar link de la webapp
4. Triggear `notificar_cliente_orden` manualmente → bot debe enviar resumen de orden
5. Enviar imagen → bot debe responder con `comprobante_recibido`
6. Triggear `notificar_resultado("confirmada", ...)` → bot debe responder con `orden_confirmada`
