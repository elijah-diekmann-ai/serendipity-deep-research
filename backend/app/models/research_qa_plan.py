"""
ResearchQAPlan model for storing micro-research plan proposals.

This model tracks plans for additional targeted research that can be proposed
when the Q&A system detects gaps in available sources.
"""
from uuid import uuid4
from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, JSON, Numeric
from sqlalchemy.dialects.postgresql import UUID

from ..core.db import Base


class PlanStatus:
    """Status values for ResearchQAPlan."""
    PROPOSED = "PROPOSED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    NO_CHANGE = "NO_CHANGE"  # Completed but no new evidence found
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ResearchQAPlan(Base):
    """
    Stores micro-research plan proposals for Q&A follow-up research.
    
    Lifecycle:
    1. PROPOSED - Plan generated after gap detection, awaiting user confirmation
    2. RUNNING - User confirmed, connectors executing
    3. COMPLETED - Research finished, new sources ingested
    4. FAILED - Execution encountered an error
    5. CANCELLED - User or system cancelled the plan
    """
    __tablename__ = "research_qa_plans"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("research_jobs.id"), index=True, nullable=False)
    qa_id = Column(Integer, ForeignKey("research_qa.id"), nullable=True)  # The Q&A that triggered this plan
    
    # Question and gap analysis
    question = Column(Text, nullable=False)
    gap_statement = Column(Text, nullable=False)  # Human-readable description of what's missing
    intent = Column(String(64), nullable=True)  # funding_investors, patents, litigation, etc.
    
    # Plan details
    plan_steps_json = Column(JSON, nullable=False)  # List[PlanStep] - connector execution plan
    plan_markdown = Column(Text, nullable=True)  # Human-readable plan summary for UI
    
    # Status tracking
    status = Column(String(32), default=PlanStatus.PROPOSED, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    confirmed_at = Column(DateTime, nullable=True)  # When user clicked "Run"
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Results
    created_source_ids = Column(JSON, nullable=True)  # List[int] - IDs of newly created Source rows
    result_qa_id = Column(Integer, ForeignKey("research_qa.id"), nullable=True)  # The re-answer Q&A row
    
    # Cost tracking
    estimated_cost_label = Column(String(32), nullable=True)  # "small" | "moderate" | "large"
    llm_usage = Column(JSON, nullable=True)
    total_cost_usd = Column(Numeric(14, 6), nullable=True)

