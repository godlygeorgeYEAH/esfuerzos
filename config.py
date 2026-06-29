from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    supabase_url: str
    supabase_service_role_key: str
    waha_url: str = "http://waha:3000"
    waha_api_key: str = ""
    waha_webhook_secret: str = ""
    waha_session: str = "default"
    # Matching thresholds
    face_match_threshold: float = 0.50
    text_match_threshold: float = 0.75
    combined_match_threshold: float = 0.65
    face_weight: float = 0.35
    text_weight: float = 0.65
    photo_retention_days: int = 30
    embeddings_model: str = "paraphrase-multilingual-mpnet-base-v2"
    # External scraper keys (leave blank to disable)
    hospitales_anon_key: str = ""
    redayuda_anon_key: str = ""
    # C1: Admin endpoint protection
    admin_key: str = ""
    # C3: CORS origins (comma-separated, or * for open)
    allowed_origins: str = "*"
    # LLM (Groq, OpenAI-compatible) — used for WAHA intake bot
    llm_api_key: str = ""
    llm_base_url: str = "https://api.groq.com/openai/v1"
    llm_model: str = "llama-3.3-70b-versatile"
    # Fallback provider chain (tried in order after the primary Groq above).
    # JSON list in env LLM_FALLBACKS, each item OpenAI-compatible:
    #   [{"name":"openrouter-llama","base_url":"https://openrouter.ai/api/v1",
    #     "api_key":"sk-or-...","model":"meta-llama/llama-3.3-70b-instruct:free",
    #     "headers":{"HTTP-Referer":"https://reune.ve","X-Title":"Reune VE"}}]
    llm_fallbacks: list[dict] = []

    class Config:
        env_file = ".env"
        # El .env ahora también lleva vars del bot prox (DATABASE_URL, POSTGRES_*)
        # y del servicio db. Este Settings solo declara las del ecosistema raíz;
        # ignorar las demás evita ValidationError por "extra_forbidden".
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
