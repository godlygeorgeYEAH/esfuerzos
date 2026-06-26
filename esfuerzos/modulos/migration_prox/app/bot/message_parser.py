"""
Utilidades para parsear y normalizar mensajes de clientes.
Incluye normalización de texto, extracción de keywords y matching fuzzy.
"""
import re
import unicodedata
from typing import List, Optional
from difflib import SequenceMatcher


def normalize_message(text: str) -> str:
    """Normaliza un mensaje: minúsculas, sin acentos, sin especiales, espacios normalizados."""
    if not text:
        return ""
    text = text.lower()
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()



def similarity_ratio(a: str, b: str) -> float:
    """Ratio de similitud entre dos strings usando SequenceMatcher (0.0 - 1.0)."""
    return SequenceMatcher(None, a, b).ratio()


def match_expected_response(
    client_message: str,
    expected_responses: List[str],
    threshold: float = 0.6
) -> Optional[str]:
    """
    Intenta hacer fuzzy match del mensaje del cliente con una lista de respuestas esperadas.

    Returns:
        La respuesta esperada que hizo match, o None si no hay match suficiente.
    """
    if not client_message or not expected_responses:
        return None

    normalized_message = normalize_message(client_message)
    best_match = None
    best_ratio = 0.0

    for expected in expected_responses:
        normalized_expected = normalize_message(expected)
        ratio = similarity_ratio(normalized_message, normalized_expected)

        client_words = normalized_message.split()
        if normalized_expected in client_words:
            ratio = 1.0
        if normalized_expected in normalized_message:
            ratio = max(ratio, 0.8)

        if ratio > best_ratio and ratio >= threshold:
            best_ratio = ratio
            best_match = expected

    return best_match


