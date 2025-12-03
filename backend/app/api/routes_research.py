from uuid import UUID, uuid4
import logging

from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.orm import Session

from ..core.db import get_db
from ..schemas.research import (
    ResearchRequest,
    ResearchJobOut,
    ResearchTraceEventOut,
    ResearchQARequest,
    ResearchQAOut,
)
from ..models.research_job import ResearchJob, JobStatus
from ..models.brief import Brief
from ..models.research_trace_event import ResearchTraceEvent
from ..models.research_qa import ResearchQA
from ..models.source import Source
from ..services.qa import answer_research_question
from ..core.celery_app import celery_app
from ..core.config import get_settings

router = APIRouter(tags=["research"])

settings = get_settings()
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
logger = logging.getLogger(__name__)


def verify_api_key(api_key: str | None = Security(api_key_header)) -> None:
    """
    Simple header-based API key authentication.

    - In dev, if API_AUTH_KEY is not set, auth is skipped.
    - Otherwise, require X-API-Key == API_AUTH_KEY.
    """
    expected = settings.API_AUTH_KEY

    # In dev with no configured key, skip auth for convenience
    if settings.ENV == "dev" and not expected:
        return

    if not expected:
        # In non-dev environments, missing config is treated as misconfiguration
        raise HTTPException(status_code=401, detail="API key not configured")

    if api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


@router.post("/research", response_model=ResearchJobOut, status_code=202)
def create_research_job(
    payload: ResearchRequest,
    db: Session = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    # Convert HttpUrl to string if present in payload (handled by schema now but kept for safety)
    payload_dict = payload.model_dump()
    if payload_dict.get("website"):
        payload_dict["website"] = str(payload_dict["website"])

    # Generate a correlation ID so we can trace this job end-to-end
    request_id = str(uuid4())
    payload_dict["request_id"] = request_id

    logger.info(
        "Creating research job",
        extra={
            "job_id": None,
            "request_id": request_id,
            "company_name": payload_dict.get("company_name"),
            "website": payload_dict.get("website"),
            "step": "create_research_job",
        },
    )

    job = ResearchJob(
        target_input=payload_dict,
        status=JobStatus.PENDING,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    logger.info(
        "Research job created",
        extra={
            "job_id": str(job.id),
            "request_id": request_id,
            "step": "job_created",
        },
    )

    celery_app.send_task(
        "app.services.orchestrator.run_research_job",
        args=[str(job.id)],
        queue="research",
    )

    return job


@router.get("/research/{job_id}")
def get_research_job(
    job_id: UUID,
    db: Session = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    job = db.query(ResearchJob).filter(ResearchJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    brief = db.query(Brief).filter(Brief.job_id == job.id).first()
    brief_content = brief.content_json if brief else None

    trace_events = (
        db.query(ResearchTraceEvent)
        .filter(ResearchTraceEvent.job_id == job_id)
        .order_by(ResearchTraceEvent.created_at.asc(), ResearchTraceEvent.id.asc())
        .all()
    )

    return {
        "job": ResearchJobOut.model_validate(job).model_dump(),
        "brief": brief_content,
        "trace": [ResearchTraceEventOut.model_validate(e).model_dump() for e in trace_events],
    }


@router.post("/research/{job_id}/qa", response_model=ResearchQAOut, status_code=201)
def create_research_qa(
    job_id: UUID,
    payload: ResearchQARequest,
    db: Session = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    job = db.query(ResearchJob).filter(ResearchJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail="Q&A is only available once the research job has completed.",
        )

    has_sources = db.query(Source.id).filter(Source.job_id == job.id).first()
    if not has_sources:
        raise HTTPException(
            status_code=400,
            detail="No sources were captured for this job; cannot answer questions.",
        )

    request_id = str(uuid4())

    try:
        qa_row = answer_research_question(
            db=db,
            job=job,
            question=payload.question,
            request_id=request_id,
        )
    except ValueError as e:
        # For predictable validation errors in the service
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(
            "Q&A failed for job %s: %s", job_id, e,
            extra={"job_id": str(job_id), "request_id": request_id},
        )
        raise HTTPException(status_code=500, detail="Failed to answer question")

    return ResearchQAOut.model_validate(qa_row)


@router.get("/research/{job_id}/qa", response_model=list[ResearchQAOut])
def list_research_qa(
    job_id: UUID,
    db: Session = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    job = db.query(ResearchJob).filter(ResearchJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    rows = (
        db.query(ResearchQA)
        .filter(ResearchQA.job_id == job_id)
        .order_by(ResearchQA.created_at.asc(), ResearchQA.id.asc())
        .all()
    )

    return [ResearchQAOut.model_validate(row) for row in rows]
