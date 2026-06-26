import difflib
from sqlalchemy.orm import Session

from app.models.bot import PreguntaFrecuente

# Umbral de similitud definido en la propuesta faq_en_db.md
_THRESHOLD = 0.65


def match_faq(db: Session, operacion_id: int, mensaje: str) -> str | None:
    """
    Busca si el mensaje del cliente hace match con alguna pregunta frecuente
    activa del negocio. Retorna la respuesta del operador o None si no hay match.
    """
    preguntas = (
        db.query(PreguntaFrecuente)
        .filter_by(operacion_id=operacion_id, activa=True)
        .all()
    )
    if not preguntas:
        return None

    canonicas = [p.pregunta for p in preguntas]
    matches = difflib.get_close_matches(mensaje, canonicas, n=1, cutoff=_THRESHOLD)
    if not matches:
        return None

    matched = next(p for p in preguntas if p.pregunta == matches[0])
    return matched.respuesta
