from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .core.config import get_settings
from .core.logging import configure_logging
from .api.routes_research import router as research_router
from .api.routes_archive import router as archive_router

configure_logging()
settings = get_settings()

app = FastAPI(title="Serendipity Deep Research API")

# CORS:
# - In prod, FRONTEND_ORIGIN is required and we never fall back to "*".
# - In non-prod, wide-open CORS is only enabled if CORS_ALLOW_ALL_ORIGINS=True.
if settings.ENV.lower() == "prod":
    if not settings.FRONTEND_ORIGIN:
        raise RuntimeError(
            "FRONTEND_ORIGIN must be set in production – refusing to start with wide-open CORS."
        )
    origins = [
        o.strip()
        for o in settings.FRONTEND_ORIGIN.split(",")
        if o.strip()
    ]
else:
    if settings.CORS_ALLOW_ALL_ORIGINS:
        origins = ["*"]
    elif settings.FRONTEND_ORIGIN:
        origins = [
            o.strip()
            for o in settings.FRONTEND_ORIGIN.split(",")
            if o.strip()
        ]
    else:
        origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=False,
)

app.include_router(research_router, prefix=settings.API_PREFIX)
app.include_router(archive_router, prefix=settings.API_PREFIX)
