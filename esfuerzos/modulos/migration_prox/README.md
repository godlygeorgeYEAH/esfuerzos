# ProX — Módulo de Bot WhatsApp para Reúne

Bot conversacional de intake de crisis, adaptado de `godlygeorgeYEAH/foob_v2` para el sistema de reunificación familiar Reúne v1.

Comunicación exclusivamente vía **WAHA** (WhatsApp HTTP API). No usa Meta Cloud API ni Graph API.

## Estructura

```
app/
├── bot/
│   ├── orchestrator.py       Pipeline principal: recibe mensaje → devuelve respuesta
│   ├── flow_engine.py        Motor de flujo conversacional (nodos y transiciones)
│   ├── flow_seeder.py        Seed de nodos de crisis al arrancar
│   ├── intent_detector.py    Detección de intención con DeepSeek
│   ├── decision_engine.py    Decide el próximo nodo según intención + similitud
│   ├── context_manager.py    Gestión del contexto de conversación (JSON en DB)
│   ├── response_generator.py Elige entre template LLM o template estático
│   ├── faq_matcher.py        Matching semántico de preguntas frecuentes
│   ├── analytics_logger.py   Registro de eventos del pipeline en DB
│   ├── template_renderer.py  Renderizado de templates con variables
│   ├── message_parser.py     Parsing de mensajes entrantes
│   └── dev_logger.py         Logger de desarrollo (dlog)
├── routers/
│   └── webhook.py            POST /webhook/waha — entrada de mensajes WAHA
├── core/
│   └── waha_resolver.py      Resuelve negocio a partir del nombre de sesión WAHA
├── models/
│   ├── bot.py                BotConfig, Conversacion, FlowTemplate, FlowNode, etc.
│   └── negocio.py            Negocio (tenant del bot)
├── services/
│   ├── waha.py               Cliente HTTP para WAHA (send_message, resolve_lid)
│   └── deepseek.py           Cliente DeepSeek (LLM)
├── config.py                 Settings con pydantic-settings
├── database.py               Setup SQLAlchemy + SessionLocal
└── main.py                   FastAPI: lifespan (seed) + router webhook
```

## Quick start

```bash
pip install -r requirements.txt
cp .env.example .env          # configurar DATABASE_URL, WAHA_URL, DEEPSEEK_API_KEY
alembic upgrade head
uvicorn app.main:app --reload
```

## Variables de entorno

```env
DATABASE_URL=postgresql://user:pass@localhost:5432/reune
WAHA_URL=http://localhost:3000
WAHA_SESSION=default
WAHA_WEBHOOK_SECRET=
DEEPSEEK_API_KEY=sk-...
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-chat
ENVIRONMENT=development
BOT_SELF_MESSAGE_TESTING=true
```

## Webhook WAHA

WAHA debe configurarse para enviar eventos al endpoint:

```
POST http://<servidor>:8000/webhook/waha
```

No se requiere verificación de `hub.challenge`. El campo `X-WAHA-Token` es opcional
(se valida solo si `WAHA_WEBHOOK_SECRET` está definido en el `.env`).

## Flujo de un mensaje entrante

```
POST /webhook/waha
  → resolve_negocio(session)
  → Orchestrator.process_message()
      1. Parsear mensaje
      2. Obtener/crear conversación
      3. Detectar intención (DeepSeek)
      4. FAQ match (similitud semántica)
      5. DecisionEngine → próximo nodo
      6. ResponseGenerator → respuesta (LLM o template)
      7. Persistir cambios
  → waha.send_message(phone, response)
```
