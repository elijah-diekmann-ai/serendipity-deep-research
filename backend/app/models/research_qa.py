from sqlalchemy import Column, Integer, Text, DateTime, ForeignKey, JSON, Numeric
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime

from ..core.db import Base

class ResearchQA(Base):
    __tablename__ = "research_qa"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey("research_jobs.id"), index=True, nullable=False)
    question = Column(Text, nullable=False)
    answer_markdown = Column(Text, nullable=False)
    used_source_ids = Column(JSON, nullable=True)  # List[int]
    llm_usage = Column(JSON, nullable=True)
    total_cost_usd = Column(Numeric(14, 6), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

