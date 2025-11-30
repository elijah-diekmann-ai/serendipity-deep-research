from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime

from ..core.db import Base

class ResearchTraceEvent(Base):
    __tablename__ = "research_trace_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(UUID(as_uuid=True),
                    ForeignKey("research_jobs.id"),
                    index=True,
                    nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    phase = Column(String, nullable=False)   # "PLANNING", "COLLECTION", "WRITING", …
    step = Column(String, nullable=True)     # "search_exa_site", "executive_summary", …
    label = Column(String, nullable=False)   # short human-readable summary
    detail = Column(String, nullable=True)   # one-paragraph explanation
    meta = Column(JSON, nullable=True)       # small, structured extras for UI

