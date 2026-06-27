from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    supabase_url: str
    supabase_service_role_key: str
    waha_url: str = "http://waha:3000"
    waha_api_key: str = ""
    waha_webhook_secret: str = ""
    waha_session: str = "default"
    # Base44 Superagent
    base44_webhook_secret: str = ""
    base44_agent_id: str = ""
    base44_api_key: str = ""
    vps_public_url: str = ""
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

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()
