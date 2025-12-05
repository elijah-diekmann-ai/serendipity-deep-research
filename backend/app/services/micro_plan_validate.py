"""
Micro-Plan Validation and Cost Estimation Module

Validates micro-research plans and provides cost estimates before execution.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..core.config import get_settings
from .planner import PlanStep
from .micro_planner import MAX_MICRO_STEPS, MAX_MICRO_EXA_QUERIES

logger = logging.getLogger(__name__)
settings = get_settings()


# ---------------------------------------------------------------------------
# Cost Estimation Constants (approximate costs per operation)
# ---------------------------------------------------------------------------

# Exa API costs (approximate)
COST_EXA_SEARCH = 0.02  # ~$0.02 per search query

# OpenAI web search costs (variable, estimate conservatively)
COST_OPENAI_WEB = 0.05  # ~$0.05 per web search call

# PDL costs
COST_PDL_PERSON = 0.10  # ~$0.10 per person lookup
COST_PDL_COMPANY = 0.05  # ~$0.05 per company lookup

# LLM re-answer cost (for the Q&A response after research)
COST_LLM_REANSWER = 0.02  # ~$0.02 for the re-answer LLM call

# Cost thresholds for labels
COST_THRESHOLD_SMALL = 0.10
COST_THRESHOLD_MODERATE = 0.30

# Runtime labels (based on typical execution times)
RUNTIME_THRESHOLD_SHORT = 2  # < 2 steps
RUNTIME_THRESHOLD_MEDIUM = 4  # < 4 steps


@dataclass
class ValidationError:
    """A single validation error."""
    field: str
    message: str
    severity: str = "error"  # "error" | "warning"


@dataclass
class ValidationResult:
    """Result of plan validation."""
    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    
    # Cost estimation
    estimated_cost_usd: float = 0.0
    cost_label: str = "small"  # "small" | "moderate" | "large"
    cost_breakdown: Dict[str, float] = field(default_factory=dict)
    
    # Runtime estimation
    estimated_runtime_seconds: int = 0
    runtime_label: str = "short"  # "short" | "medium" | "long"


def _get_available_connectors() -> set:
    """Get the set of connectors that are available (have API keys configured)."""
    available = set()
    
    # Exa is always available (assuming EXA_API_KEY is set)
    if getattr(settings, "EXA_API_KEY", None):
        available.add("exa")
    
    # OpenAI web search - only OPENAI_API_KEY works
    # Note: OPENROUTER_API_KEY does NOT enable openai_web connector
    # The OpenAIWebSearchConnector uses the openai SDK directly
    if getattr(settings, "OPENAI_API_KEY", None):
        available.add("openai_web")
    
    # PDL connectors
    if getattr(settings, "PDL_API_KEY", None):
        available.add("pdl")
        available.add("pdl_company")
    
    # GLEIF is always available (no key required)
    available.add("gleif")
    
    return available


def _validate_step(step: PlanStep, available_connectors: set) -> List[ValidationError]:
    """Validate a single plan step."""
    errors = []
    
    step_name = step.get("name", "unnamed")
    connector = step.get("connector", "")
    params = step.get("params", {})
    
    # Check connector exists and is available
    if not connector:
        errors.append(ValidationError(
            field=f"step.{step_name}.connector",
            message="Connector name is required",
        ))
    elif connector not in available_connectors:
        errors.append(ValidationError(
            field=f"step.{step_name}.connector",
            message=f"Connector '{connector}' is not available (missing API key or not registered)",
        ))
    
    # Validate params based on connector type
    if connector == "exa":
        mode = params.get("mode")
        if mode not in ("search", "similar", None):
            errors.append(ValidationError(
                field=f"step.{step_name}.params.mode",
                message=f"Invalid Exa mode: {mode}",
            ))
        
        queries = params.get("queries", [])
        if mode == "search" and not queries:
            errors.append(ValidationError(
                field=f"step.{step_name}.params.queries",
                message="Exa search requires at least one query",
            ))
    
    elif connector == "openai_web":
        mode = params.get("mode")
        # Validate OpenAI mode is explicit (not relying on keyword detection)
        if not mode:
            errors.append(ValidationError(
                field=f"step.{step_name}.params.mode",
                message="OpenAI web search requires explicit mode (competitors, founding, leadership, person, news)",
                severity="error",  # Upgraded from warning - mode is required
            ))
        elif mode not in ("competitors", "founding", "leadership", "person", "news", "general"):
            errors.append(ValidationError(
                field=f"step.{step_name}.params.mode",
                message=f"Invalid OpenAI web mode: {mode}",
                severity="error",
            ))
        
        # Validate person mode has person_name
        if mode == "person" and not params.get("person_name"):
            errors.append(ValidationError(
                field=f"step.{step_name}.params.person_name",
                message="OpenAI person mode requires person_name",
                severity="error",
            ))
    
    elif connector == "pdl":
        # Check if this is person enrichment (needs full_name) vs leadership search (empty full_name is ok)
        full_name = params.get("full_name")
        # If full_name is provided but empty string, it's a leadership search (valid)
        # If full_name key is missing entirely, we can't determine intent - warn
        if full_name is None:
            errors.append(ValidationError(
                field=f"step.{step_name}.params.full_name",
                message="PDL search should specify full_name (use empty string for leadership search)",
                severity="warning",
            ))
    
    elif connector == "pdl_company":
        # PDL company requires some identifying information
        has_identifier = any([
            params.get("company_name"),
            params.get("website"),
        ])
        if not has_identifier:
            errors.append(ValidationError(
                field=f"step.{step_name}.params",
                message="PDL company requires at least one identifier (company_name or website)",
            ))
    
    elif connector == "gleif":
        # GLEIF requires company_name
        if not params.get("company_name"):
            errors.append(ValidationError(
                field=f"step.{step_name}.params.company_name",
                message="GLEIF lookup requires company_name",
            ))
    
    return errors


def _estimate_step_cost(step: PlanStep) -> float:
    """Estimate the cost of a single step."""
    connector = step.get("connector", "")
    params = step.get("params", {})
    
    if connector == "exa":
        # Cost per query
        queries = params.get("queries", [])
        return COST_EXA_SEARCH * max(1, len(queries))
    
    elif connector == "openai_web":
        return COST_OPENAI_WEB
    
    elif connector == "pdl":
        return COST_PDL_PERSON
    
    elif connector == "pdl_company":
        return COST_PDL_COMPANY
    
    elif connector == "gleif":
        return 0.0  # Free API
    
    return 0.01  # Default minimal cost


def _estimate_step_runtime(step: PlanStep) -> int:
    """Estimate the runtime of a single step in seconds."""
    connector = step.get("connector", "")
    
    # Most connectors take 2-5 seconds
    if connector == "exa":
        return 3
    elif connector == "openai_web":
        return 8  # OpenAI web search is slower due to reasoning
    elif connector in ("pdl", "pdl_company"):
        return 2
    elif connector == "gleif":
        return 1
    
    return 3


def _get_cost_label(cost: float) -> str:
    """Convert cost to a human-readable label."""
    if cost < COST_THRESHOLD_SMALL:
        return "small"
    elif cost < COST_THRESHOLD_MODERATE:
        return "moderate"
    else:
        return "large"


def _get_runtime_label(steps: int) -> str:
    """Convert step count to a runtime label."""
    if steps < RUNTIME_THRESHOLD_SHORT:
        return "short"
    elif steps < RUNTIME_THRESHOLD_MEDIUM:
        return "medium"
    else:
        return "long"


def validate_and_estimate(
    plan_steps: List[PlanStep],
    target_input: Dict[str, Any],
) -> ValidationResult:
    """
    Validate a micro-research plan and estimate its cost.
    
    Checks:
    - Connectors are registered and have required API keys
    - Parameters match expected shapes
    - Caps are not exceeded (MAX_MICRO_STEPS, MAX_MICRO_EXA_QUERIES)
    
    Returns:
        ValidationResult with validity, errors, and cost estimates
    """
    errors: List[ValidationError] = []
    warnings: List[ValidationError] = []
    cost_breakdown: Dict[str, float] = {}
    total_cost = 0.0
    total_runtime = 0
    
    available_connectors = _get_available_connectors()
    
    # Check global caps
    if len(plan_steps) > MAX_MICRO_STEPS:
        errors.append(ValidationError(
            field="plan_steps",
            message=f"Plan exceeds maximum steps ({len(plan_steps)} > {MAX_MICRO_STEPS})",
        ))
    
    # Count Exa queries
    exa_query_count = 0
    for step in plan_steps:
        if step.get("connector") == "exa":
            queries = step.get("params", {}).get("queries", [])
            exa_query_count += max(1, len(queries))
    
    if exa_query_count > MAX_MICRO_EXA_QUERIES:
        warnings.append(ValidationError(
            field="plan_steps",
            message=f"Plan has many Exa queries ({exa_query_count}), may be costly",
            severity="warning",
        ))
    
    # Validate each step and estimate costs
    for step in plan_steps:
        step_errors = _validate_step(step, available_connectors)
        
        for err in step_errors:
            if err.severity == "warning":
                warnings.append(err)
            else:
                errors.append(err)
        
        # Estimate cost
        step_cost = _estimate_step_cost(step)
        connector = step.get("connector", "unknown")
        cost_breakdown[connector] = cost_breakdown.get(connector, 0.0) + step_cost
        total_cost += step_cost
        
        # Estimate runtime
        total_runtime += _estimate_step_runtime(step)
    
    # Add LLM re-answer cost
    total_cost += COST_LLM_REANSWER
    cost_breakdown["llm_reanswer"] = COST_LLM_REANSWER
    
    # Determine labels
    cost_label = _get_cost_label(total_cost)
    runtime_label = _get_runtime_label(len(plan_steps))
    
    is_valid = len(errors) == 0
    
    logger.info(
        "Plan validation: valid=%s, errors=%d, warnings=%d, cost=$%.3f (%s)",
        is_valid,
        len(errors),
        len(warnings),
        total_cost,
        cost_label,
        extra={
            "is_valid": is_valid,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "cost_usd": total_cost,
            "cost_label": cost_label,
        },
    )
    
    return ValidationResult(
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        estimated_cost_usd=total_cost,
        cost_label=cost_label,
        cost_breakdown=cost_breakdown,
        estimated_runtime_seconds=total_runtime,
        runtime_label=runtime_label,
    )

