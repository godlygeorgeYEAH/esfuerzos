"""
Dev Logger - Logging detallado del pipeline de conversación para desarrollo.

Activar con DEV_FLOW_LOG=True en .env

Output de ejemplo:
  [DEV] -------------------------------------------------------
  [DEV] ORCHESTRATOR | Paso 2: Bot activo
  [DEV]   is_bot_active : True
  [DEV]   resultado     : ACTIVO -> continuar
  [DEV] -------------------------------------------------------
"""
import sys


def dlog(module: str, step: str, **data) -> None:
    """
    Imprime un paso del pipeline de conversación si DEV_FLOW_LOG está activo.

    Importación lazy de settings para evitar circular imports.
    """
    from app.config import get_settings
    settings = get_settings()
    if not settings.dev_flow_log:
        return

    SEP = "-" * 55
    lines = [
        f"\n[DEV] {SEP}",
        f"[DEV] {module.upper()} | {step}",
    ]

    for key, value in data.items():
        if isinstance(value, str) and len(value) > 120:
            value = value[:120] + "..."
        label = f"{key:<14}"
        lines.append(f"[DEV]   {label}: {value}")

    output = "\n".join(lines)

    try:
        print(output)
    except UnicodeEncodeError:
        print(output.encode("ascii", "replace").decode("ascii"))

    sys.stdout.flush()
