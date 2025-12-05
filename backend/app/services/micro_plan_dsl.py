"""
Pydantic DSL Schema for Micro-Research Plans

Provides validated, type-safe models for micro-research task specification
with per-type field requirements and automatic repair logic.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, model_validator


class MicroTaskType(str, Enum):
    """Allowed task types for micro-research."""
    
    # Exa neural search tasks
    exa_news_search = "exa_news_search"
    exa_site_search = "exa_site_search"
    exa_funding_search = "exa_funding_search"
    exa_patent_search = "exa_patent_search"
    exa_general_search = "exa_general_search"
    exa_similar_search = "exa_similar_search"
    exa_research_paper = "exa_research_paper"
    exa_historical_search = "exa_historical_search"
    
    # OpenAI web search
    openai_web_search = "openai_web_search"
    
    # PDL tasks - explicit separation of person vs company modes
    pdl_person_enrich = "pdl_person_enrich"  # Requires person_name
    pdl_company_leadership = "pdl_company_leadership"  # Company leadership search (no person_name needed)
    pdl_company_search = "pdl_company_search"
    
    # Registry lookups
    gleif_lei_lookup = "gleif_lei_lookup"


class OpenAIMode(str, Enum):
    """OpenAI web search modes."""
    
    competitors = "competitors"
    founding = "founding"
    leadership = "leadership"
    person = "person"
    news = "news"


class MicroTask(BaseModel):
    """
    A single task in the micro-research DSL.
    
    Validates that required fields are present based on task type:
    - openai_web_search: requires openai_mode
    - openai_mode="person": requires person_name  
    - pdl_person_enrich: requires person_name
    """
    
    type: MicroTaskType
    priority: Literal["high", "medium", "low"] = "medium"
    query_hint: Optional[str] = None
    
    # OpenAI-specific
    openai_mode: Optional[OpenAIMode] = None
    
    # Person-related
    person_name: Optional[str] = None
    
    # Exa-specific params
    subpage_targets: Optional[List[str]] = None
    highlights_query: Optional[str] = None
    
    # Date range overrides
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    
    @model_validator(mode='after')
    def validate_required_fields(self) -> "MicroTask":
        """Validate that required fields are present based on task type."""
        
        # OpenAI requires mode
        if self.type == MicroTaskType.openai_web_search and not self.openai_mode:
            raise ValueError("openai_web_search requires openai_mode")
        
        # Person mode requires person_name
        if self.openai_mode == OpenAIMode.person and not self.person_name:
            raise ValueError("openai_mode='person' requires person_name")
        
        # PDL person enrich requires person_name
        if self.type == MicroTaskType.pdl_person_enrich and not self.person_name:
            raise ValueError("pdl_person_enrich requires person_name")
        
        return self
    
    class Config:
        use_enum_values = True


class MicroPlanDSL(BaseModel):
    """
    The complete micro-research plan in DSL format.
    
    This is what the LLM generates, before translation to PlanStep format.
    """
    
    gap: str
    intent: str
    tasks: List[MicroTask]
    slot_hints: Dict[str, Any] = {}
    
    class Config:
        use_enum_values = True


def parse_task_with_repair(
    task_dict: Dict[str, Any],
    default_slots: Optional[Dict[str, Any]] = None,
) -> Optional[MicroTask]:
    """
    Parse a task dictionary with repair logic for common issues.
    
    Repair strategies:
    1. Missing openai_mode for openai_web_search -> infer from query_hint or drop
    2. Missing person_name for person mode -> convert to leadership mode
    3. Missing person_name for pdl_person_enrich -> drop task
    
    Args:
        task_dict: Raw task dictionary from LLM
        default_slots: Slot hints to use for repair (e.g., person_name)
        
    Returns:
        Validated MicroTask or None if task should be dropped
    """
    default_slots = default_slots or {}
    task_type = task_dict.get("type", "")
    
    # Legacy support: convert pdl_person_search to explicit types BEFORE enum validation
    if task_type == "pdl_person_search":
        if task_dict.get("person_name") or default_slots.get("person_name"):
            task_dict["type"] = MicroTaskType.pdl_person_enrich.value
            task_type = task_dict["type"]
            if not task_dict.get("person_name"):
                task_dict["person_name"] = default_slots.get("person_name")
        else:
            # No person_name -> this is actually a leadership search
            task_dict["type"] = MicroTaskType.pdl_company_leadership.value
            task_type = task_dict["type"]
    
    # Normalize type string to enum value
    try:
        task_type_enum = MicroTaskType(task_type)
    except ValueError:
        return None  # Unknown task type
    
    # Repair: openai_web_search without mode
    if task_type_enum == MicroTaskType.openai_web_search:
        if not task_dict.get("openai_mode"):
            # Try to infer mode from query_hint
            query_hint = (task_dict.get("query_hint") or "").lower()
            inferred_mode = _infer_openai_mode(query_hint)
            if inferred_mode:
                task_dict["openai_mode"] = inferred_mode
            else:
                # Cannot infer mode, drop task
                return None
    
    # Repair: person mode without person_name
    openai_mode = task_dict.get("openai_mode")
    if openai_mode == "person":
        if not task_dict.get("person_name"):
            # Check if we have person_name in slots
            if default_slots.get("person_name"):
                task_dict["person_name"] = default_slots["person_name"]
            else:
                # Convert to leadership mode instead of dropping
                task_dict["openai_mode"] = "leadership"
    
    # Repair: pdl_person_enrich without person_name
    if task_type_enum == MicroTaskType.pdl_person_enrich:
        if not task_dict.get("person_name"):
            if default_slots.get("person_name"):
                task_dict["person_name"] = default_slots["person_name"]
            else:
                # Cannot proceed without person_name, drop task
                return None
    
    # Repair: Fill missing query_hint from slot_hints
    if not task_dict.get("query_hint") and default_slots.get("query_hint"):
        task_dict["query_hint"] = default_slots["query_hint"]
    
    try:
        return MicroTask(**task_dict)
    except ValueError:
        return None


def _infer_openai_mode(query_hint: str) -> Optional[str]:
    """Infer OpenAI mode from query hint text."""
    hint_lower = query_hint.lower()
    
    mode_keywords = {
        "competitors": ["competitor", "alternative", "rival", "vs", "versus", "market"],
        "founding": ["founding", "founded", "incorporation", "registered", "legal entity", "sec"],
        "leadership": ["founder", "executive", "ceo", "leadership", "team", "board"],
        "person": ["biography", "career", "background", "education"],
        "news": ["news", "announcement", "press", "recent"],
    }
    
    for mode, keywords in mode_keywords.items():
        for keyword in keywords:
            if keyword in hint_lower:
                return mode
    
    return None


# Mapping from DSL task types to connector names
TASK_TO_CONNECTOR: Dict[str, str] = {
    "exa_news_search": "exa",
    "exa_site_search": "exa",
    "exa_funding_search": "exa",
    "exa_patent_search": "exa",
    "exa_general_search": "exa",
    "exa_similar_search": "exa",
    "exa_research_paper": "exa",
    "exa_historical_search": "exa",
    "openai_web_search": "openai_web",
    "pdl_person_enrich": "pdl",
    "pdl_company_leadership": "pdl",
    "pdl_company_search": "pdl_company",
    "gleif_lei_lookup": "gleif",
    # Legacy support
    "pdl_person_search": "pdl",
}


def get_connector_for_task(task_type: str) -> Optional[str]:
    """Get the connector name for a task type."""
    return TASK_TO_CONNECTOR.get(task_type)

