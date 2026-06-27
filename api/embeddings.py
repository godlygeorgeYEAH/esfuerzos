import asyncio

import cv2
import numpy as np


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
    parts = [
        report.get("full_name"),
        str(report.get("age")) if report.get("age") is not None else None,
        report.get("last_seen_location"),
        report.get("distinguishing_marks"),
        report.get("clothing"),
    ]
    return " ".join(p for p in parts if p).strip()
