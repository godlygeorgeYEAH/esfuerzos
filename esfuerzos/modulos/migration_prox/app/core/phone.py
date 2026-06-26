import re


def normalize_phone(raw: str) -> str:
    """
    Normaliza un número de teléfono venezolano a E.164: +58XXXXXXXXXX.

    Formatos de entrada soportados:
      04241234567    → +584241234567
      0424-123-4567  → +584241234567
      584241234567   → +584241234567   (formato que devuelve WAHA)
      +584241234567  → +584241234567
    """
    digits = re.sub(r"\D", "", raw)

    if digits.startswith("0"):
        digits = "58" + digits[1:]

    if not digits.startswith("58"):
        digits = "58" + digits

    return "+" + digits
