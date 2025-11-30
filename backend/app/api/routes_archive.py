from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..core.db import get_db
from ..models.research_job import ResearchJob
from ..models.brief import Brief
from .routes_research import verify_api_key

router = APIRouter(tags=["archive"])


@router.get("/archive")
def list_jobs(
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    """
    Protected listing endpoint for recent research jobs.

    - Requires the same API key protection as /api/research.
    - Supports basic pagination via limit/offset.
    """
    # Hard cap to avoid unbounded scans
    safe_limit = max(1, min(limit, 100))

    jobs = (
        db.query(ResearchJob)
        .order_by(ResearchJob.created_at.desc())
        .offset(offset)
        .limit(safe_limit)
        .all()
    )

    return [
        {
            "job": {
                "id": str(j.id),
                "status": j.status.value,
                "created_at": j.created_at,
                "completed_at": j.completed_at,
                "target_input": j.target_input,
            },
            "has_brief": db.query(Brief)
            .filter(Brief.job_id == j.id)
            .first()
            is not None,
        }
        for j in jobs
    ]
