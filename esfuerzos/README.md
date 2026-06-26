# Reúne v1 — Sistema de Reunificación Familiar

Sistema de intake por WhatsApp para conectar familias con personas rescatadas tras el sismo M7.2/M7.5 del 24 de junio de 2026 en Venezuela.

---

## Canal de comunicación: WAHA (no Meta Cloud API)

Este proyecto **no utiliza** la API oficial de WhatsApp Business de Meta (WhatsApp Cloud API / Graph API).

### Por qué WAHA y no Meta Cloud API

| Criterio | Meta Cloud API | WAHA |
|---|---|---|
| Aprobación requerida | Sí — proceso de revisión de días/semanas | No — operativo en minutos |
| Restricciones de templates | Los primeros mensajes deben ser templates aprobados por Meta | Mensajes libres desde el inicio |
| Ventana de 24 h | Solo se puede responder dentro de las 24 h de recibido el mensaje | Sin restricción de ventana |
| Costo por mensaje | Sí (tarifa Meta) | Sin tarifa por mensaje |
| Número de teléfono | Requiere número verificado en Business Manager | Cualquier número con WhatsApp activo |
| Infraestructura | Webhook HTTPS con verificación `hub.challenge` | Webhook HTTP simple, sin verificación de challenge |
| Tiempo de despliegue en crisis | Inviable en las primeras 72 h | Inmediato |

En un contexto de emergencia, la aprobación de Meta, la configuración del Business Manager y los templates de mensajes representan una barrera operacional inaceptable. WAHA permite levantar el canal de comunicación en minutos con cualquier dispositivo Android o número existente.

### Cómo funciona WAHA

WAHA (WhatsApp HTTP API) es un servidor auto-hospedado que expone una API REST sobre WhatsApp Web. Los mensajes entrantes se entregan a nuestra aplicación vía webhook (POST HTTP). No se requiere ningún `hub.challenge`, token de verificación de Meta ni configuración en el Meta Developer Portal.

```
Usuario WhatsApp
       │
       ▼
  WAHA Server ──webhook POST──▶ /webhook/waha  (este proyecto)
       ▲
       │
   send_message()
```

Variables de entorno requeridas para WAHA:

```env
WAHA_URL=http://localhost:3000        # URL del servidor WAHA
WAHA_SESSION=default                  # Nombre de sesión WAHA
WAHA_WEBHOOK_SECRET=                  # Opcional: token para validar origen del webhook
```

No existen variables `GRAPH_API_TOKEN`, `PHONE_NUMBER_ID`, `WHATSAPP_BUSINESS_ACCOUNT_ID` ni similares de Meta en este proyecto.

---

## Arquitectura general

```
WhatsApp ──▶ WAHA ──▶ FastAPI (ProX module)
                           │
                 ┌─────────┼─────────┐
                 ▼         ▼         ▼
            PostgreSQL  DeepSeek   SQLite DBs
            (intake)    (LLM NLU)  (fuentes externas)
```

### Módulos

| Directorio | Descripción |
|---|---|
| `modulos/migration_prox/` | Bot conversacional: orquestador, motor de flujo, intake de reportes |
| `scraper/` | Scrapers de fuentes externas (SOS Venezuela, reconexion, PNP, VenezReporta) |
| `scripts/` | Utilidades: exportación unificada, bot WAHA simple |
| `db/` | Modelos y repositorios SQLAlchemy para las 4 DBs SQLite externas |
| `docs/` | Arquitectura de datos, fuentes de información |

### Bases de datos

- **PostgreSQL** (`modulos/migration_prox/`) — reportes de intake, conversaciones, flujo del bot
- **`sos_personas.db`** — directorio SOS Venezuela
- **`reconexion.db`** — API desaparecidos-terremoto
- **`pnp_cedulas.db`** — cédulas venezolanas (PNP/CNE)
- **`venezreporta.db`** — VenezReporta.org

---

## Flujos de intake por WhatsApp

### Flujo A — Familiar reporta desaparecido

1. Usuario escribe al número de Reúne
2. Bot pregunta tipo de reporte (busco a alguien / encontré a alguien)
3. Bot recopila: nombre, edad, última ubicación, señas particulares, ropa, foto(s)
4. Bot confirma datos y guarda el reporte

### Flujo B — Rescatista/hospital reporta persona encontrada

1. Usuario selecciona "encontré a alguien"
2. Bot recopila: nombre o descripción, ubicación actual (albergue/hospital), condición, foto(s)
3. Bot confirma y guarda

El cruce de reportes y la notificación a familias requieren **revisión humana** antes de comunicarse. El bot no notifica matches de forma automática.

---

## Quick start

```bash
# 1. Instalar dependencias del módulo bot
cd modulos/migration_prox
pip install -r requirements.txt

# 2. Configurar variables de entorno
cp .env.example .env
# Editar: DATABASE_URL, WAHA_URL, DEEPSEEK_API_KEY

# 3. Crear tablas
alembic upgrade head

# 4. Seed del negocio y flujo de crisis
python scripts/seed_crisis_bot.py

# 5. Arrancar
uvicorn app.main:app --reload --port 8000
```

El endpoint que WAHA debe apuntar como webhook es:

```
POST http://<tu-servidor>:8000/webhook/waha
```

---

## Variables de entorno

```env
# Base de datos
DATABASE_URL=postgresql://user:pass@localhost:5432/reune

# WAHA
WAHA_URL=http://localhost:3000
WAHA_SESSION=default
WAHA_WEBHOOK_SECRET=           # Dejar vacío en dev

# DeepSeek (NLU del bot)
DEEPSEEK_API_KEY=sk-...
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-chat

# Entorno
ENVIRONMENT=development
BOT_SELF_MESSAGE_TESTING=true  # Permite testear enviando /comandos desde el mismo número
```

---

## Privacidad

Los números de teléfono de los reportantes **nunca se almacenan en texto plano**. Se guarda un hash SHA-256 del número (`reporter_wa_hash`) para permitir re-contacto sin exponer el número en la base de datos.

El consentimiento explícito del reportante se registra en el campo `consent` del reporte antes de guardar cualquier dato personal.
