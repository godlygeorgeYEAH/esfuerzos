"""
Seed de Reúne v1 — crea Negocio, BotConfig, FlowTemplate y NegocioFlow.

Idempotente: si el Negocio con slug 'reune' ya existe, solo actualiza
BotConfig y re-ejecuta el seeder de nodos. Seguro de correr en cada deploy.

Uso:
    cd modulos/migration_prox
    DATABASE_URL=sqlite:///./test.db python -m scripts.seed_crisis_bot

    # Con PostgreSQL:
    DATABASE_URL=postgresql+psycopg2://user:pass@localhost/reune \
        python -m scripts.seed_crisis_bot
"""
import logging
import os
import sys

# Permite ejecutar como módulo desde la raíz del paquete
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def run() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL no está definida.")
        sys.exit(1)

    engine = create_engine(database_url)

    # Importaciones después de crear el engine para que Base esté lista
    from app.database import Base
    from app.models.negocio import Negocio
    from app.models.bot import BotConfig, NegocioFlow
    from app.models.reporte import Report, Photo  # noqa: F401 — registra en Base
    from app.bot.flow_seeder import seed_default_flow

    Base.metadata.create_all(engine)
    logger.info("Tablas verificadas / creadas.")

    Session = sessionmaker(bind=engine)
    db = Session()

    try:
        # --- Negocio ---
        negocio = db.query(Negocio).filter(Negocio.slug == "reune").first()
        if not negocio:
            negocio = Negocio(
                nombre="Reúne",
                slug="reune",
                waha_session="default",
                is_active=True,
            )
            db.add(negocio)
            db.flush()
            logger.info("Negocio creado: id=%d slug=reune", negocio.id)
        else:
            logger.info("Negocio existente: id=%d slug=reune", negocio.id)

        # --- BotConfig ---
        bot_config = db.query(BotConfig).filter(BotConfig.negocio_id == negocio.id).first()
        if not bot_config:
            bot_config = BotConfig(
                negocio_id=negocio.id,
                is_bot_active=True,
                enable_intent_detection=False,   # DeepSeek deshabilitado
                delivery_enabled=False,
                retiro_enabled=False,
                working_hours_start=None,        # 24/7 — emergencia
                working_hours_end=None,
            )
            db.add(bot_config)
            logger.info("BotConfig creado para negocio_id=%d", negocio.id)
        else:
            bot_config.is_bot_active = True
            bot_config.enable_intent_detection = False
            logger.info("BotConfig actualizado para negocio_id=%d", negocio.id)

        db.commit()

        # --- FlowTemplate + nodos ---
        template = seed_default_flow(db)
        logger.info("FlowTemplate activo: id=%d '%s'", template.id, template.name)

        # --- NegocioFlow ---
        neg_flow = db.query(NegocioFlow).filter(NegocioFlow.negocio_id == negocio.id).first()
        if not neg_flow:
            neg_flow = NegocioFlow(
                negocio_id=negocio.id,
                flow_template_id=template.id,
                is_active=True,
            )
            db.add(neg_flow)
            db.commit()
            logger.info("NegocioFlow creado: negocio_id=%d → flow_id=%d", negocio.id, template.id)
        else:
            neg_flow.flow_template_id = template.id
            neg_flow.is_active = True
            db.commit()
            logger.info("NegocioFlow actualizado: negocio_id=%d → flow_id=%d", negocio.id, template.id)

        logger.info("Seed completado. Sistema listo.")

    except Exception:
        db.rollback()
        logger.exception("Error durante el seed — rollback ejecutado.")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    run()
