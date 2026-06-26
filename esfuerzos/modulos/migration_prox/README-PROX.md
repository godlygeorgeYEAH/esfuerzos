# ProX — Paquete de Migración

Bot conversacional WhatsApp, portado de `godlygeorgeYEAH/foob_v2`.

## Estructura

```
app/
├── bot/            Orquestador + motores de intención, decisión, contexto y respuesta
├── routers/        Webhook WAHA (entrada de mensajes)
├── core/           Utilidades: clientes, conductores, teléfonos, notificaciones
├── models/         Modelos SQLAlchemy (bot, cliente, negocio, orden, conductor, notificación)
├── services/       Clientes externos: WAHA, DeepSeek, Storage
├── config.py       Settings con pydantic-settings
├── database.py     Setup de SQLAlchemy
└── main.py         FastAPI mínimo (solo webhook + media)
```

## Antes de integrar

Leer `docs/PROBLEMATICAS.md` — contiene todas las problemáticas de acoplamiento
y la guía de integración paso a paso.

## Quick start (single-tenant, dev)

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Configurar .env (ver docs/PROBLEMATICAS.md §16)
cp .env.example .env

# 3. Crear tablas (Alembic)
alembic upgrade head

# 4. Crear BotConfig para el negocio (ver docs/PROBLEMATICAS.md §8)
# python scripts/seed_bot_config.py

# 5. Arrancar
uvicorn app.main:app --reload
```

## Variables de entorno mínimas

```env
DATABASE_URL=postgresql://user:pass@localhost:5432/prox
WAHA_URL=http://localhost:3000
DEEPSEEK_API_KEY=sk-...
WEBAPP_BASE_URL=http://localhost:4200
MEDIA_BASE_URL=http://localhost:8000
```
