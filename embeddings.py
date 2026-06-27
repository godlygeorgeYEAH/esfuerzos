import asyncio
import html
import re

import cv2
import numpy as np

# Patterns that indicate shelter/location info inside distinguishing_marks.
# These come from the OBSERVACIONES column of the hospital Excel:
# e.g. "CI: 30114918; Piso Cirugía Cama #10 – La Guaira: Playa Grande"
#      "Refugio/Damnificados – Campo de Golf Caribe"
# Stripping these prevents location from contaminating embeddings when only
# one side of a potential match shares the same geographic zone.
_SHELTER_RE = re.compile(
    r'(?:'
    r'Refugio[^;–]*'
    r'|Damnificados[^;–]*'
    r'|Campo\s+de\s+Golf\b[^;–]*'
    r'|Centro\s+de\s+Acopio\b[^;–]*'
    r'|Piso\s+\S+.*?Cama\s*#?\d+[^;–]*'
    r'|[–\-]\s*(?:La\s+Guaira|Vargas|Carab[a-záéíóú]{2,}|Catia|Playa\s+Grande|Tanaguarena)[^;]*'
    r')',
    re.IGNORECASE,
)


def _clean_text(text: str) -> str:
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _clean_marks_for_embedding(marks: str | None) -> str:
    """Strip location/shelter content from marks, keep cedula + medical terms."""
    if not marks:
        return ""
    cleaned = _SHELTER_RE.sub('', marks)
    # Collapse leftover semicolons/whitespace
    cleaned = re.sub(r';\s*;', ';', cleaned)
    cleaned = re.sub(r'^[;\s]+|[;\s]+$', '', cleaned)
    return cleaned.strip()


async def get_text_embedding(text: str, model) -> list[float]:
    # C5: asyncio.to_thread wraps blocking model.encode call
    embedding = await asyncio.to_thread(model.encode, text)
    norm = np.linalg.norm(embedding)
    if norm > 0:
        embedding = embedding / norm
    return embedding.tolist()


def get_face_embedding(image_bytes: bytes, face_model) -> tuple[list[float] | None, bool]:
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None, False
    faces = face_model.get(img)
    if not faces:
        return None, False
    face = faces[0]
    if face.det_score < 0.5:
        return None, False
    return face.embedding.tolist(), True


def build_text_for_embedding(report: dict) -> str:
    # name + age + marks (marks scrubbed of location/shelter text).
    # Location excluded at every level: disaster victims from the same
    # geographic zone would otherwise produce false-positive matches.
    marks = _clean_marks_for_embedding(report.get("distinguishing_marks"))
    parts = [
        report.get("full_name"),
        str(report.get("age")) if report.get("age") is not None else None,
        marks or None,
    ]
    text = " ".join(p for p in parts if p).strip()
    return _clean_text(text)
