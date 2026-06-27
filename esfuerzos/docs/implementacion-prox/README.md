# Módulo ProX — Reúne v1

Bot conversacional WhatsApp para reportes de personas desaparecidas tras emergencias sísmicas. Recibe mensajes de familiares, recopila datos (nombre, género, edad, ubicación, fotos) y crea registros en base de datos para coordinación humana posterior.

**Stack:** FastAPI · SQLAlchemy · WAHA (WhatsApp HTTP API) · SQLite/PostgreSQL  
**Sin:** Meta Graph API · DeepSeek (deshabilitado hasta nueva instrucción) · Supabase · CompreFace

---

## Estructura

```
modulos/migration_prox/
├── app/
│   ├── bot/               # Pipeline conversacional
│   │   ├── orchestrator.py      # Coordinador principal
│   │   ├── flow_engine.py       # Motor de nodos y templates
│   │   ├── flow_seeder.py       # 8 nodos de crisis
│   │   ├── decision_engine.py   # Selección de nodo destino
│   │   ├── context_manager.py   # Estado JSON de conversación
│   │   └── response_generator.py
│   ├── core/
│   │   ├── intake.py            # Parseo y commit de Report + Photos
│   │   └── waha_resolver.py     # Resuelve Operacion desde session WAHA
│   ├── models/
│   │   ├── negocio.py           # Operacion (tenant raíz)
│   │   ├── bot.py               # Conversacion, FlowNode, OperacionFlow…
│   │   └── reporte.py           # Report, Photo
│   ├── routers/
│   │   └── webhook.py           # POST /webhook/waha
│   └── services/
│       └── waha.py              # send_message, send_match_notification…
└── scripts/
    └── seed_crisis_bot.py       # Provisiona Operacion + flujo
```

---

## Flujo de un mensaje

```
WAHA → POST /webhook/waha
  └─ resolve_operacion()          # identifica el tenant
  └─ Orchestrator.process_message()
       ├─ verificaciones (bloqueado, bot activo, horario)
       ├─ obtener/crear Conversacion
       ├─ guardar mensaje cliente
       ├─ interceptar pedir_foto   → _handle_pedir_foto (keyword "listo" + descarga)
       ├─ similarity matching      → next_node_map (sin LLM)
       ├─ Decision Engine          → nodo destino
       ├─ intake hooks             → commit_report() al llegar a reporte_guardado
       └─ Response Generator       → template (LLM deshabilitado)
```

---

## Nodos del flujo familiar

| Nodo | Tipo | Acción |
|---|---|---|
| `bienvenida` | greeting | Selección de perfil 1/2/3 |
| `guia_familiar` | intake_guide | Pide datos en un mensaje |
| `pedir_foto` | intake_photo | Acumula fotos; avanza con "listo" o al llegar al máximo (5) |
| `notas_adicionales` | intake_notes | Notas libres; cierra el reporte |
| `reporte_guardado` | intake_saved | Confirmación. Llama `commit_report` |
| `guia_rescatista` | placeholder | Pendiente |
| `guia_hospital` | placeholder | Pendiente |
| `fallback` | fallback | Retoma con 1/2/3 |

---

## Cómo usar este módulo

### 1. Requisitos

```
Python 3.11+
pip install -r requirements.txt
```

Variables mínimas en `.env`:

```env
DATABASE_URL=sqlite:///./test.db
WAHA_URL=http://localhost:3000
WAHA_API_KEY=
WAHA_WEBHOOK_URL=http://tu-servidor/webhook/waha
WAHA_FREE_TIER=true
PHOTO_STORAGE_PATH=media/photos
PHOTO_MAX_COUNT=5
```

### 2. Provisionar la base de datos

```bash
cd modulos/migration_prox
DATABASE_URL=sqlite:///./test.db python -m scripts.seed_crisis_bot
```

Salida esperada:
```
INFO  Tablas verificadas / creadas.
INFO  Operacion creado: id=1 slug=reune
INFO  BotConfig creado para operacion_id=1
INFO  FlowSeeder: FlowTemplate de crisis creado (id=1)
INFO  FlowSeeder: 8 nodo(s) creado(s) en template id=1
INFO  OperacionFlow creado: operacion_id=1 → flow_id=1
INFO  Seed completado. Sistema listo.
```

### 3. Levantar el servidor

```bash
DATABASE_URL=sqlite:///./test.db uvicorn app.main:app --reload --port 8000
```

### 4. Apuntar WAHA al webhook

En la configuración de WAHA, establecer el webhook URL a:
```
http://tu-servidor:8000/webhook/waha
```

---

## Testing completo

> **Fase 6 (notificación de coincidencias) está pendiente de implementación.** No existe motor de matching activo. Los pasos 1–5 y 7 son funcionales.

### Setup común

```bash
cd modulos/migration_prox
DATABASE_URL=sqlite:///./test.db python -m scripts.seed_crisis_bot
DATABASE_URL=sqlite:///./test.db uvicorn app.main:app --reload --port 8000
```

```bash
BASE="http://localhost:8000/webhook/waha"
PHONE="584121234567@c.us"

send() {
  curl -s -X POST $BASE \
    -H "Content-Type: application/json" \
    -d "$1" | python3 -m json.tool
}
```

---

### Paso 1 — Infraestructura base

Verifica que el webhook responde y crea conversación:

```bash
send "{\"session\":\"default\",\"event\":\"message\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"hola\",\"hasMedia\":false}}"
# Esperado: {"status": "processed", "sent": true}
```

Verifica tablas:

```bash
DATABASE_URL=sqlite:///./test.db python - <<'EOF'
from app.database import SessionLocal
from app.models.bot import Conversacion
db = SessionLocal()
conv = db.query(Conversacion).first()
print(f"Conversacion id={conv.id} nodo={conv.current_node_key}")
db.close()
EOF
```

---

### Paso 2 — Modelos Report y Photo

Verifica que las tablas existen tras el seed:

```bash
DATABASE_URL=sqlite:///./test.db python - <<'EOF'
from app.database import SessionLocal, Base, engine
from app.models.reporte import Report, Photo
Base.metadata.create_all(engine)
db = SessionLocal()
print("reports:", db.query(Report).count())
print("photos:", db.query(Photo).count())
db.close()
EOF
```

---

### Paso 3 — Flujo de intake completo (sin fotos)

```bash
# 1. Bienvenida
send "{\"session\":\"default\",\"event\":\"message\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"hola\",\"hasMedia\":false}}"

# 2. Seleccionar familiar
send "{\"session\":\"default\",\"event\":\"message\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"1\",\"hasMedia\":false}}"

# 3. Datos de la persona
send "{\"session\":\"default\",\"event\":\"message\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"María García, femenino, 34, Cumaná centro\",\"hasMedia\":false}}"
# Esperado: nodo pedir_foto

# Verificar nodo
DATABASE_URL=sqlite:///./test.db python -c "
from app.database import SessionLocal
from app.models.bot import Conversacion
db = SessionLocal()
c = db.query(Conversacion).first()
print('Nodo:', c.current_node_key)
db.close()"
```

---

### Paso 4 — Sesión multi-foto con keyword "listo"

```bash
# Foto 1
send "{\"session\":\"default\",\"event\":\"message\",\"id\":\"t4a\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"\",\"hasMedia\":true,\"mediaUrl\":\"http://example.com/foto1.jpg\"}}"
# Esperado: "📸 Imagen recibida (1/5). Escribe *listo* cuando termines."

# Foto 2
send "{\"session\":\"default\",\"event\":\"message\",\"id\":\"t4b\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"\",\"hasMedia\":true,\"mediaUrl\":\"http://example.com/foto2.jpg\"}}"
# Esperado: "📸 Imagen recibida (2/5). Escribe *listo* cuando termines."

# Texto que no es "listo" → queda en pedir_foto
send "{\"session\":\"default\",\"event\":\"message\",\"id\":\"t4c\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"ya terminé\",\"hasMedia\":false}}"
# Esperado: "⏳ Tienes 2/5 foto(s). Puedes enviar más o escribe *listo*."

# "listo" → avanza a notas_adicionales
send "{\"session\":\"default\",\"event\":\"message\",\"id\":\"t4d\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"listo\",\"hasMedia\":false}}"
# Esperado: notas_adicionales

# Variante: "listo" sin fotos también avanza
send "{\"session\":\"default\",\"event\":\"message\",\"id\":\"t4e\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"listo\",\"hasMedia\":false}}"
# Esperado: notas_adicionales (reportes sin foto son válidos)
```

---

### Paso 5 — Commit de Report + Photos en DB

Continúa desde notas_adicionales:

```bash
send "{\"session\":\"default\",\"event\":\"message\",\"payload\":{\"from\":\"$PHONE\",\"fromMe\":false,\"body\":\"Tiene un lunar en la mejilla izquierda\",\"hasMedia\":false}}"
# Esperado: reporte_guardado
```

Verificar en DB:

```bash
DATABASE_URL=sqlite:///./test.db python - <<'EOF'
from app.database import SessionLocal
from app.models.reporte import Report, Photo
db = SessionLocal()
r = db.query(Report).order_by(Report.id.desc()).first()
if r:
    print(f"Report #{r.id} — {r.full_name}, {r.gender}, {r.age}, {r.last_seen_location}")
    print(f"  Notas: {r.distinguishing_marks}")
    print(f"  Hash:  {r.reporter_wa_hash}")
    for p in db.query(Photo).filter(Photo.report_id == r.id).all():
        print(f"  Foto: {p.media_url} → {p.local_path}")
else:
    print("Sin reportes")
db.close()
EOF
```

---

---

## Pendientes globales

| # | Área | Descripción |
|---|---|---|
| 1 | Flujo rescatista | `guia_rescatista` es placeholder. Nodos internos y lógica de reporte `kind="found"` sin definir. |
| 2 | Flujo hospital/refugio | `guia_hospital` es placeholder. Mismo estado que rescatista. |
| 3 | Motor de matching | `send_match_notification` existe pero ningún servicio detecta coincidencias entre reportes `missing` y `found`. |
| 4 | `kind="found"` | `commit_report` siempre crea `kind="missing"`. Sin flujo `found` activo, los reportes de personas encontradas no se registran. |
| 5 | Parseo robusto | `parse_person_data` espera CSV estricto (`nombre, género, edad, ubicación`). No tolera variantes como "34 años" o separadores alternativos. Evaluar LLM o regex más flexible. |
| 6 | Descarga WAHA con API key | `_download_photo` no envía `X-Api-Key` al descargar media. Requerido en producción si WAHA tiene auth habilitado. |
| 7 | Conversaciones varadas | Si el usuario llega a `pedir_foto` y nunca escribe "listo" ni envía más fotos, la conversación queda en ese nodo indefinidamente. Para producción: tarea periódica que archive conversaciones inactivas. |
| 8 | Respuesta a CONFIRMAR | Tras recibir una notificación de coincidencia, el usuario puede responder `CONFIRMAR`. No hay nodo que maneje ese keyword — cae en fallback. |
| 9 | Hash vs teléfono en notificaciones | `reporter_wa_hash` es SHA-256 irreversible. El servicio de matching debe obtener el teléfono desde `Conversacion.client_phone` cruzando por hash, o almacenar el `waha_chat_id` en el reporte. |
| 10 | `.env.example` | Las variables de entorno requeridas no están documentadas en un archivo de ejemplo. Crear antes del primer deploy. |
| 11 | Multi-operacion | El seed asume un solo tenant. Para coordinaciones regionales múltiples, extender con `WAHA_FREE_TIER=false` y seeds por operación. |
| 12 | Migraciones DB | No hay sistema de migraciones (Alembic). Cambios de esquema en producción requieren SQL manual. |

---

### Paso 6 — Notificación de coincidencias ⚠️ PENDIENTE

**Esta fase no está implementada.** El motor de matching entre reportes `missing` y `found` no existe. `send_match_notification` está disponible en `app/services/waha.py` pero no se invoca desde ningún flujo activo.

---

### Paso 7 — Seed idempotente

Verifica que correr el seed dos veces no duplica datos:

```bash
DATABASE_URL=sqlite:///./test.db python -m scripts.seed_crisis_bot
DATABASE_URL=sqlite:///./test.db python -m scripts.seed_crisis_bot
# Esperado: segunda ejecución no crea registros nuevos (solo "existente"/"actualizado")

DATABASE_URL=sqlite:///./test.db python - <<'EOF'
from app.database import SessionLocal
from app.models.negocio import Operacion
from app.models.bot import OperacionFlow
db = SessionLocal()
print("Operaciones:", db.query(Operacion).count())       # debe ser 1
print("OperacionFlows:", db.query(OperacionFlow).count()) # debe ser 1
db.close()
EOF
```
