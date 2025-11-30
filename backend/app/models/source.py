from sqlalchemy import Column, Integer, String, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from ..core.db import Base

class Source(Base):
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey("research_jobs.id"), nullable=False)
    url = Column(String, nullable=True)
    title = Column(String, nullable=True)
    snippet = Column(Text, nullable=False)       # text given to LLM
    provider = Column(String, nullable=False)    # 'exa', 'companies_house', 'apollo', etc.
    published_date = Column(String, nullable=True)  # ISO date/time from connectors, if available

