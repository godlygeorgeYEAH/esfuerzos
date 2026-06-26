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
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from app.config import get_settings
from app.routers import webhook

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Seed del flujo conversacional al arrancar la aplicación."""
    try:
        from app.database import SessionLocal
        from app.bot.flow_seeder import seed_default_flow
        db = SessionLocal()
        try:
            seed_default_flow(db)
            logger.info("FlowSeeder: flujo por defecto verificado/creado.")
        finally:
            db.close()
    except Exception as e:
        logger.warning("FlowSeeder falló al arrancar (no crítico): %s", e)
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
    allow_origins=["*"] if not settings.is_production else [],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Storage local: crear subdirectorios y montar en /media
for _subdir in ["comprobantes", "imagenes"]:
    Path(f"/app/media/{_subdir}").mkdir(parents=True, exist_ok=True)
app.mount("/media", StaticFiles(directory="/app/media"), name="media")

# Webhook WAHA — entrada de mensajes
app.include_router(webhook.router, prefix="/webhook", tags=["webhook"])
