# backend/app/services/tracing.py
from __future__ import annotations

from typing import Any
from uuid import UUID
import logging
from datetime import datetime

from ..core.db import SessionLocal
from ..models.research_trace_event import ResearchTraceEvent

logger = logging.getLogger(__name__)

def trace_job_step(
    job_id: UUID,
    *,
    phase: str,
    step: str | None = None,
    label: str,
    detail: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """
    Best-effort, fire-and-forget trace writer.
    Failure must NEVER break the main research job.
    """
    db = SessionLocal()
    try:
        evt = ResearchTraceEvent(
            job_id=job_id,
            phase=phase,
            step=step,
            label=label,
            detail=detail,
            meta=meta or {},
            created_at=datetime.utcnow(),
        )
        db.add(evt)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Failed to write research trace event", extra={"job_id": str(job_id)})
    finally:
        db.close()

