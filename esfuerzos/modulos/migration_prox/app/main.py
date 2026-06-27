"""
Punto de entrada mínimo de FastAPI para el módulo ProX.

Este archivo arranca:
  1. El seeder de flujos (crea FlowTemplate + FlowNodes al iniciar)
  2. El webhook WAHA en POST /webhook/waha
  3. El mount de StaticFiles para servir comprobantes en GET /media/...

El nuevo repo debe integrar estos routers y el lifespan a su propio main.py
si ya tiene una aplicación FastAPI existente.
"""
import logging
import sys
from contextlib import asynccontextmanager

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stdout,
)
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from app.config import get_settings
from app.routers import webhook

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Seed del flujo conversacional y configuración de sesión WAHA al arrancar."""
    try:
        from app.database import SessionLocal, Base, engine
        from app.models import negocio, bot, reporte, cliente  # noqa: F401 — registra en Base
        from app.bot.flow_seeder import seed_default_flow
        from app.models.negocio import Operacion
        from app.models.bot import BotConfig, OperacionFlow, FlowTemplate
        Base.metadata.create_all(engine)
        db = SessionLocal()
        try:
            # 1. Flujo
            seed_default_flow(db)
            logger.info("FlowSeeder: flujo por defecto verificado/creado.")

            # 2. Operacion por defecto (idempotente)
            op = db.query(Operacion).filter_by(slug="reune").first()
            if not op:
                op = Operacion(nombre="Reúne", slug="reune", waha_session=settings.waha_session, is_active=True)
                db.add(op)
                db.flush()
                logger.info("Operacion 'reune' creada (id=%d).", op.id)

            # 3. BotConfig (idempotente)
            if not db.query(BotConfig).filter_by(operacion_id=op.id).first():
                db.add(BotConfig(operacion_id=op.id, is_bot_active=True, enable_intent_detection=False))
                logger.info("BotConfig creado para operacion_id=%d.", op.id)

            # 4. OperacionFlow (idempotente)
            if not db.query(OperacionFlow).filter_by(operacion_id=op.id).first():
                flow = db.query(FlowTemplate).filter_by(is_system_default=True).first()
                if flow:
                    db.add(OperacionFlow(operacion_id=op.id, flow_template_id=flow.id, is_active=True))
                    logger.info("OperacionFlow vinculado (operacion=%d, flow=%d).", op.id, flow.id)

            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.warning("FlowSeeder falló al arrancar (no crítico): %s", e)

    try:
        from app.services.waha import ensure_default_session
        await ensure_default_session()
    except Exception as e:
        logger.warning("WAHA session setup falló al arrancar (no crítico): %s", e)

    yield


limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="ProX Bot API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if not settings.is_production else None,
    redoc_url="/redoc" if not settings.is_production else None,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Webhook WAHA — entrada de mensajes
app.include_router(webhook.router, prefix="/webhook", tags=["webhook"])
