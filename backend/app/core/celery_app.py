from celery import Celery
from celery.schedules import crontab

from .config import get_settings
from .logging import configure_logging

settings = get_settings()
configure_logging()

celery_app = Celery(
    "deep_research",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

celery_app.conf.update(
    task_routes={"app.services.orchestrator.run_research_job": {"queue": "research"}},
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    imports=("app.services.orchestrator", "app.services.retention"),
    beat_schedule={
        # Daily cleanup of old research data based on RESEARCH_RETENTION_DAYS
        "cleanup-expired-research-data": {
            "task": "app.services.retention.cleanup_expired",
            "schedule": crontab(hour=3, minute=0),
        },
    },
)
