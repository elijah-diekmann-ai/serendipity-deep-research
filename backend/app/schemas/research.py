# backend/app/schemas/research.py
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, field_validator, model_validator

from ..models.research_job import JobStatus

MAX_COMPANY_NAME_LEN = 200
MAX_PERSON_NAME_LEN = 200
MAX_CONTEXT_LEN = 4000
MAX_WEBSITE_LEN = 2048
MAX_LOCATION_LEN = 200


class ResearchRequest(BaseModel):
    target_type: Literal["company", "person"] = "company"
    company_name: str | None = None
    person_name: str | None = None
    website: str | None = None
    context: str
    location: str | None = None

    @field_validator("company_name", "person_name", "location", "website", mode="before")
    @classmethod
    def _blank_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            stripped = v.strip()
            return stripped or None
        return v

    @field_validator("company_name")
    @classmethod
    def validate_company_name(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if len(v) > MAX_COMPANY_NAME_LEN:
            raise ValueError(
                f"company_name must be at most {MAX_COMPANY_NAME_LEN} characters"
            )
        return v

    @field_validator("person_name")
    @classmethod
    def validate_person_name(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if len(v) > MAX_PERSON_NAME_LEN:
            raise ValueError(
                f"person_name must be at most {MAX_PERSON_NAME_LEN} characters"
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

    @field_validator("location")
    @classmethod
    def validate_location(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if len(v) > MAX_LOCATION_LEN:
            raise ValueError(
                f"location must be at most {MAX_LOCATION_LEN} characters"
            )
        return v

    @model_validator(mode="after")
    def validate_target_fields(self):
        target_type = self.target_type or "company"
        if target_type == "company" and not self.company_name:
            raise ValueError("company_name must be provided for company research")
        if target_type == "person" and not self.person_name:
            raise ValueError("person_name must be provided for person research")
        return self


class ResearchJobOut(BaseModel):
    id: UUID
    status: JobStatus
    created_at: datetime
    completed_at: datetime | None = None
    total_cost_usd: float | None = None
    llm_usage: dict | None = None

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

