# backend/app/schemas/research.py
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, field_validator

from ..models.research_job import JobStatus

MAX_COMPANY_NAME_LEN = 200
MAX_CONTEXT_LEN = 4000
MAX_WEBSITE_LEN = 2048


class ResearchRequest(BaseModel):
    company_name: str
    website: str | None = None
    context: str

    @field_validator("company_name")
    @classmethod
    def validate_company_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("company_name must not be empty")
        if len(v) > MAX_COMPANY_NAME_LEN:
            raise ValueError(
                f"company_name must be at most {MAX_COMPANY_NAME_LEN} characters"
            )
        return v

    @field_validator("context")
    @classmethod
    def validate_context(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("context must not be empty")
        if len(v) > MAX_CONTEXT_LEN:
            raise ValueError(
                f"context is too long; maximum length is {MAX_CONTEXT_LEN} characters"
            )
        return v

    @field_validator("website")
    @classmethod
    def validate_website(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if len(str(v)) > MAX_WEBSITE_LEN:
            raise ValueError("website URL is too long")
        return v


class ResearchJobOut(BaseModel):
    id: UUID
    status: JobStatus
    created_at: datetime
    completed_at: datetime | None = None

    class Config:
        from_attributes = True


class ResearchTraceEventOut(BaseModel):
    id: int
    created_at: datetime
    phase: str
    step: str | None = None
    label: str
    detail: str | None = None
    meta: dict | None = None

    class Config:
        from_attributes = True


class BriefContent(BaseModel):
    executive_summary: str | None = None
    founding_details: str | None = None
    founders_and_leadership: str | None = None
    fundraising: str | None = None
    product: str | None = None
    technology: str | None = None
    competitors: str | None = None
    recent_news: str | None = None
    sources: str | None = None
    # Split citations into "used" (in prompt context) and "all" (all persisted sources)
    used_citations: list[dict] = []
    all_citations: list[dict] = []
    # Backwards-compatible alias for used_citations
    citations: list[dict] = []

