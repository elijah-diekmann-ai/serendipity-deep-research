from pydantic_settings import BaseSettings
from pydantic import AnyUrl
from functools import lru_cache


class Settings(BaseSettings):
    # core
    ENV: str = "dev"
    API_PREFIX: str = "/api"

    # database & redis
    DATABASE_URL: AnyUrl
    # Keep this as a plain string so redis:// URLs are always accepted
    REDIS_URL: str

    # external APIs
    OPENROUTER_API_KEY: str | None = None
    OPENAI_API_KEY: str | None = None  # Required if using openai_web connector
    OPENAI_WEB_MODEL: str | None = None  # Optional override for web search model
    EXA_API_KEY: str
    COMPANIES_HOUSE_API_KEY: str | None = None
    OPENCORPORATES_API_TOKEN: str | None = None
    OPENCORPORATES_BASE_URL: str = "https://api.opencorporates.com/v0.4"
    OPENCORPORATES_TIMEOUT_SECONDS: int = 20
    OPENCORPORATES_MAX_RESULTS: int = 5
    OPENCORPORATES_API_KEY: str | None = None # kept for backward compatibility if needed, but preferring TOKEN per plan
    
    GLEIF_ENABLED: bool = True
    GLEIF_BASE_URL: str = "https://api.gleif.org/api/v1"
    GLEIF_TIMEOUT_SECONDS: int = 20
    GLEIF_MAX_RESULTS: int = 3
    
    # PitchBook Direct Data / API
    PITCHBOOK_API_KEY: str | None = None
    PITCHBOOK_BASE_URL: str = "https://api.pitchbook.com"
    PITCHBOOK_TIMEOUT_SECONDS: int = 30
    PITCHBOOK_MAX_RESULTS: int = 10
    
    APOLLO_API_KEY: str | None = None
    PDL_API_KEY: str | None = None

    # auth / security
    API_AUTH_KEY: str | None = None
    FRONTEND_ORIGIN: str | None = None
    # Explicit debug-only switch for wide-open CORS in non-prod envs
    CORS_ALLOW_ALL_ORIGINS: bool = False

    # llm
    LLM_PROVIDER: str = "openai"  # or "claude", "openrouter"
    LLM_MODEL: str = "openai/gpt-5.1"
    # Hard cap on concurrent LLM calls per process
    LLM_MAX_CONCURRENCY: int = 4
    LLM_PRICEBOOK_JSON: str | None = None
    WEB_SEARCH_PER_CALL_USD: float = 0.01

    # data retention (in days)
    RESEARCH_RETENTION_DAYS: int = 90

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
