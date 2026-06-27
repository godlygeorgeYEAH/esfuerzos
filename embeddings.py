import asyncio
import html
import re

import cv2
import numpy as np


def _clean_text(text: str) -> str:
    """Decode HTML entities and normalize whitespace."""
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


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
    # Only name + age + distinguishing_marks (may contain CI/cedula).
    # Location excluded: disaster victims from same zone share location,
    # causing false positives between unrelated people.
    parts = [
        report.get("full_name"),
        str(report.get("age")) if report.get("age") is not None else None,
        report.get("distinguishing_marks"),
    ]
    text = " ".join(p for p in parts if p).strip()
    return _clean_text(text)
