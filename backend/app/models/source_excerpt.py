"""
SourceExcerpt model for storing novel text excerpts from micro-research.

When micro-research finds a URL that already exists in the sources table,
instead of discarding it, we store the new text content as an "excerpt".
This solves the "zero novelty" problem where duplicate URLs prevented
any new information from being added to the knowledge base.
"""
from datetime import datetime

from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from ..core.db import Base


class SourceExcerpt(Base):
    """
    Stores novel text excerpts from micro-research for existing sources.
    
    When micro-research finds content from a URL that already exists,
    we store the new excerpt text here (deduplicated by content hash)
    rather than discarding the result entirely.
    """
    __tablename__ = "source_excerpts"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys
    job_id = Column(UUID(as_uuid=True), ForeignKey("research_jobs.id"), index=True, nullable=False)
    source_id = Column(Integer, ForeignKey("sources.id"), index=True, nullable=False)
    plan_id = Column(UUID(as_uuid=True), ForeignKey("research_qa_plans.id"), index=True, nullable=True)
    
    # Excerpt content
    excerpt_text = Column(Text, nullable=False)  # The actual excerpt text
    excerpt_type = Column(String(32), nullable=False)  # exa_highlight, openai_extract, etc.
    content_hash = Column(String(64), nullable=False)  # SHA256 for dedupe (lowercase, normalized)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Constraints
    __table_args__ = (
        # Prevent duplicate content for the same source
        UniqueConstraint('source_id', 'content_hash', name='uq_source_excerpt_content'),
        # Index for efficient lookup by job
        Index('ix_source_excerpts_job_id', 'job_id'),
    )

