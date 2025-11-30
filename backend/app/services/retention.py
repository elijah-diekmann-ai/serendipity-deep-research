from __future__ import annotations

from datetime import datetime, timedelta
import logging

from sqlalchemy.orm import Session

from ..core.celery_app import celery_app
from ..core.config import get_settings
from ..core.db import SessionLocal
from ..models.research_job import ResearchJob
from ..models.brief import Brief
from ..models.source import Source

logger = logging.getLogger(__name__)
settings = get_settings()


@celery_app.task(name="app.services.retention.cleanup_expired")
def cleanup_expired() -> int:
    """
    Periodic task to enforce data retention policy.

    Current policy:
    - Delete ResearchJob / Brief / Source records older than RESEARCH_RETENTION_DAYS
      based on ResearchJob.created_at.

    Company and Person records are retained indefinitely for now as an internal
    knowledge base.
    """
    db: Session = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=settings.RESEARCH_RETENTION_DAYS)
        old_jobs = db.query(ResearchJob).filter(ResearchJob.created_at < cutoff).all()
        job_ids = [j.id for j in old_jobs]

        if not job_ids:
            logger.info(
                "No expired research jobs found for cleanup",
                extra={"step": "retention"},
            )
            return 0

        db.query(Brief).filter(Brief.job_id.in_(job_ids)).delete(
            synchronize_session=False
        )
        db.query(Source).filter(Source.job_id.in_(job_ids)).delete(
            synchronize_session=False
        )
        deleted_jobs = (
            db.query(ResearchJob)
            .filter(ResearchJob.id.in_(job_ids))
            .delete(synchronize_session=False)
        )
        db.commit()

        logger.info(
            "Deleted expired research jobs",
            extra={"step": "retention", "deleted_jobs": deleted_jobs},
        )
        return deleted_jobs
    except Exception:
        db.rollback()
        logger.exception(
            "Error during cleanup_expired",
            extra={"step": "retention"},
        )
        raise
    finally:
        db.close()

