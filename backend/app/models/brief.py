from sqlalchemy import Column, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID
from ..core.db import Base

class Brief(Base):
    __tablename__ = "briefs"

    job_id = Column(UUID(as_uuid=True), ForeignKey("research_jobs.id"), primary_key=True)
    content_json = Column(JSON, nullable=False)

