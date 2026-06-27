"""
Configuración del sistema ProX (bot conversacional WhatsApp).

Basado en foob_v2/backend/app/config.py — reducido a los settings
estrictamente necesarios para el bot. El nuevo repo puede extender
esta clase con sus propios campos.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ----------------------------------------------------------------
    # Base de datos
    # ----------------------------------------------------------------
    database_url: str

    # ----------------------------------------------------------------
    # WAHA — gateway WhatsApp
    # ----------------------------------------------------------------
    waha_url: str = "http://localhost:3000"
    waha_api_key: str = ""
    waha_session: str = "default"
    waha_webhook_url: str = "http://localhost:8000/webhook/waha"
    waha_free_tier: bool = True          # True = un solo negocio; False = multi-tenant por waha_session
    waha_webhook_secret: str | None = None  # Si se setea, WAHA debe enviar X-WAHA-Token

    # ----------------------------------------------------------------
    # DeepSeek (LLM — compatible OpenAI)
    # ----------------------------------------------------------------
    deepseek_api_key: str = ""
    deepseek_base_url: str = "https://api.deepseek.com/v1"
    deepseek_model: str = "deepseek-chat"
    deepseek_timeout: int = 15
    deepseek_max_retries: int = 3

    # ----------------------------------------------------------------
    # Bot conversacional
    # ----------------------------------------------------------------
    force_llm_responses: bool = False       # True = todos los nodos vía DeepSeek
    dev_flow_log: bool = False              # True = logs verbose del pipeline en consola
    dev_whitelist: str = ""                 # Teléfonos permitidos en dev (sin +, separados por coma)
    bot_self_message_testing: bool = False  # True = mensajes fromMe con "/" se procesan

    # Umbrales de confianza del Decision Engine
    intent_high_confidence: float = 0.80
    intent_medium_confidence: float = 0.70
    intent_low_confidence: float = 0.65

    # ----------------------------------------------------------------
    # Fotos de intake
    # ----------------------------------------------------------------
    photo_max_count: int = 5          # máximo de fotos por reporte
    photo_ttl_seconds: int = 60       # segundos sin nueva foto para considerar la sesión cerrada
    photo_storage_path: str = "media/photos"  # directorio local de descarga

    # ----------------------------------------------------------------
    # Supabase — almacenamiento de reportes y fotos
    # ----------------------------------------------------------------
    supabase_url: str = ""
    supabase_service_role_key: str = ""

    # ----------------------------------------------------------------
    # Entorno
    # ----------------------------------------------------------------
    environment: str = "development"

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


@lru_cache
def get_settings() -> Settings:
    return Settings()
