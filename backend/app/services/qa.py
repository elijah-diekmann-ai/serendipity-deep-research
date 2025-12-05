from uuid import UUID
from dataclasses import dataclass
from sqlalchemy.orm import Session
import json
import logging
import re
from typing import Tuple, Set, List, Optional

from urllib.parse import urlparse

from ..models.research_job import ResearchJob, JobStatus
from ..models.source import Source
from ..models.research_qa import ResearchQA
from ..models.research_qa_plan import ResearchQAPlan, PlanStatus
from .writer import Writer
from .entity_resolution import KnowledgeGraph, CompanyNode, PersonTargetNode
from .llm_costs import LLMCostTracker
from .tracing import trace_job_step
from .qa_gap import detect_gap, GapDetectionResult
from .micro_planner import propose_micro_plan, MicroPlan
from .micro_plan_validate import validate_and_estimate, ValidationResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Q&A Result with Optional Micro-Research Plan
# ---------------------------------------------------------------------------

@dataclass
class QAResult:
    """Result of Q&A with optional micro-research plan proposal."""
    qa_row: ResearchQA
    research_plan: Optional[ResearchQAPlan] = None

# ---------------------------------------------------------------------------
# Raw Source Access Configuration
# ---------------------------------------------------------------------------
# The Q&A system reads RAW snippets (not summarized) to surface "alpha" –
# specific details the brief may have compressed away for brevity.

# Higher token budget for raw source access (vs 6000 for brief sections)
QA_RAW_MAX_TOKENS = 24000

# Per-source snippet truncation limit (truncate, don't LLM-summarize)
QA_MAX_SNIPPET_CHARS = 4000

# Legacy constant (kept for compatibility, but raw builder is preferred)
QA_MAX_SOURCE_TOKENS = 12000

# Maps question keywords to relevant brief sections for targeted source selection
# This reuses the Writer's section-specific source filtering to surface relevant sources
QUESTION_SECTION_MAP: dict[tuple[str, ...], list[str]] = {
    # Technology & IP
    ("patent", "ip", "intellectual property", "technology", "tech stack", "architecture"): [
        "technology"
    ],
    # Leadership & Team
    ("founder", "co-founder", "ceo", "team", "leadership", "management", "executive", "officer"): [
        "founders_and_leadership"
    ],
    # Funding & Investment
    ("funding", "raised", "investor", "investment", "round", "series", "valuation", "capital"): [
        "fundraising"
    ],
    # Product & Offering
    ("product", "offering", "service", "platform", "solution", "feature"): [
        "product"
    ],
    # Competition
    ("competitor", "alternative", "rival", "compete", "market share", "vs"): [
        "competitors"
    ],
    # News & Events
    ("news", "recent", "announcement", "press", "update", "event"): [
        "recent_news"
    ],
    # Corporate Structure
    ("founding", "incorporated", "registered", "abn", "acn", "jurisdiction", "headquarters", "hq"): [
        "founding_details"
    ],
    # Person-specific: Education
    ("education", "degree", "university", "college", "school", "mba", "phd"): [
        "education"
    ],
    # Person-specific: Work History
    ("career", "work history", "employment", "previous role", "experience"): [
        "work_history"
    ],
}


def _build_minimal_kg(target_input: dict) -> KnowledgeGraph:
    """
    Construct a minimal KnowledgeGraph from job.target_input for use in source filtering.
    """
    target_type = target_input.get("target_type", "company")
    
    if target_type == "person":
        person = PersonTargetNode(
            full_name=target_input.get("person_name", ""),
            normalized_name=target_input.get("person_name", "").lower(),
            linkedin_url=target_input.get("linkedin_url"),
            primary_company=target_input.get("company_context"),
        )
        return KnowledgeGraph(
            company=None,
            target_type="person",
            person=person,
        )
    else:
        company = CompanyNode(
            name=target_input.get("company_name", ""),
            domain=target_input.get("company_domain"),
        )
        return KnowledgeGraph(
            company=company,
            target_type="company",
            person=None,
        )


def _detect_relevant_sections(question: str, target_type: str) -> list[str]:
    """
    Parse the question for keywords and return a list of relevant section names.
    Falls back to a broad set if no keywords match.
    """
    question_lower = question.lower()
    matched_sections: set[str] = set()
    
    for keywords, sections in QUESTION_SECTION_MAP.items():
        for keyword in keywords:
            if keyword in question_lower:
                matched_sections.update(sections)
                break  # Found a match in this keyword group, move to next
    
    if matched_sections:
        logger.info(
            "Q&A keyword detection matched sections: %s for question: '%s'",
            list(matched_sections),
            question[:100],
        )
        return list(matched_sections)
    
    # No keyword match: return broad section list based on target type
    if target_type == "person":
        return ["education", "work_history", "additional_information"]
    else:
        return [
            "executive_summary",
            "founding_details", 
            "founders_and_leadership",
            "fundraising",
            "product",
            "technology",
            "competitors",
            "recent_news",
        ]


def _select_sources_for_question(
    question: str,
    all_sources: list[Source],
    writer: Writer,
    kg: KnowledgeGraph,
) -> list[Source]:
    """
    Select sources relevant to the Q&A question by mapping keywords to sections
    and using the Writer's section-based source filtering.
    """
    target_type = kg.target_type or "company"
    relevant_sections = _detect_relevant_sections(question, target_type)
    
    # Collect sources from all relevant sections
    combined_sources: dict[int, Source] = {}  # Use dict to dedupe by source ID
    
    for section_name in relevant_sections:
        section_sources = writer._select_sources_for_section(
            section_name, all_sources, kg
        )
        for src in section_sources:
            if src.id not in combined_sources:
                combined_sources[src.id] = src
    
    # If section filtering returned nothing, fall back to all sources
    if not combined_sources:
        logger.warning(
            "Q&A section filtering returned no sources; falling back to all %d sources",
            len(all_sources),
        )
        return all_sources
    
    logger.info(
        "Q&A source selection: %d sources from sections %s (out of %d total)",
        len(combined_sources),
        relevant_sections,
        len(all_sources),
    )
    
    return list(combined_sources.values())


# ---------------------------------------------------------------------------
# Raw Source Access Functions
# ---------------------------------------------------------------------------

def _score_source_relevance(source: Source, question: str) -> float:
    """
    Score how relevant a source is to the question.
    Higher score = more likely to contain the answer.
    
    Scoring factors:
    - Question term matches in title (high signal)
    - Question term matches in snippet
    - Presence of specific identifiers (patents, amounts, dates)
    """
    score = 0.0
    
    # Normalize question into searchable terms (remove common words)
    stop_words = {"the", "a", "an", "is", "are", "was", "were", "what", "who", 
                  "where", "when", "how", "does", "do", "did", "have", "has",
                  "their", "they", "this", "that", "for", "with", "and", "or"}
    question_terms = {
        term.lower() for term in re.findall(r'\b\w+\b', question)
        if term.lower() not in stop_words and len(term) > 2
    }
    
    if not question_terms:
        return 0.0
    
    # Title match (high signal - 3x weight)
    title = (source.title or "").lower()
    title_matches = sum(1 for term in question_terms if term in title)
    score += title_matches * 3.0
    
    # Snippet match (1x weight)
    snippet = (source.snippet or "").lower()
    snippet_matches = sum(1 for term in question_terms if term in snippet)
    score += snippet_matches * 1.0
    
    # Boost for specific identifiers that indicate detailed data
    raw_snippet = source.snippet or ""
    
    # Patent-like IDs (US, EP, WO, CN, JP followed by numbers)
    if re.search(r'\b(?:US|EP|WO|CN|JP)[A-Z]?\d{4,}', raw_snippet, re.IGNORECASE):
        score += 2.0
    
    # Dollar/currency amounts
    if re.search(r'[\$£€]\s*[\d,.]+\s*(?:million|billion|[MBK])?', raw_snippet, re.IGNORECASE):
        score += 1.5
    
    # Specific dates (ISO format or common formats)
    if re.search(r'\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', raw_snippet):
        score += 1.0
    
    # Email addresses (specific contact info)
    if re.search(r'\b[\w.-]+@[\w.-]+\.\w+\b', raw_snippet):
        score += 0.5
    
    # Registration/ID numbers (ABN, ACN, EIN, etc.)
    if re.search(r'\b(?:ABN|ACN|EIN|VAT|CRN|LEI)[\s:]*[\d\s-]+', raw_snippet, re.IGNORECASE):
        score += 1.5
    
    return score


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for English text."""
    return max(1, len(text) // 4)


def _build_raw_source_context(
    sources: list[Source],
    question: str,
    max_tokens: int = QA_RAW_MAX_TOKENS,
) -> tuple[str, set[int]]:
    """
    Build source context using RAW snippets without LLM summarization.
    
    This is the core of the Q&A "alpha" discovery system:
    - Reads original source snippets, not compressed summaries
    - Truncates long snippets (preserves start) rather than abstracting them
    - Scores sources by relevance to prioritize likely answers
    - Fits more sources within budget through smart truncation
    
    Returns:
        Tuple of (formatted source string, set of used source IDs)
    """
    if not sources:
        return "", set()
    
    # Score and sort sources by relevance to the question
    scored_sources = [
        (source, _score_source_relevance(source, question))
        for source in sources
    ]
    scored_sources.sort(key=lambda x: x[1], reverse=True)  # Highest score first
    
    def _provider_label(src: Source) -> str:
        """Extract domain from URL for cleaner source labels."""
        if src.url:
            try:
                parsed = urlparse(src.url)
                if parsed.netloc:
                    return parsed.netloc
            except Exception:
                pass
        return src.provider or "Unknown"
    
    lines: list[str] = []
    used_ids: set[int] = set()
    tokens_used = 0
    
    for source, relevance_score in scored_sources:
        raw_snippet = source.snippet or ""
        
        # Truncate long snippets (don't summarize – preserve original detail)
        if len(raw_snippet) > QA_MAX_SNIPPET_CHARS:
            truncated_chars = len(raw_snippet) - QA_MAX_SNIPPET_CHARS
            raw_snippet = (
                raw_snippet[:QA_MAX_SNIPPET_CHARS] + 
                f"\n... [truncated, {truncated_chars:,} more chars in original]"
            )
        
        # Build source block
        block = (
            f"[S{source.id}] {source.title or 'Source'} – {_provider_label(source)}\n"
            f"{raw_snippet}\n"
            f"URL: {source.url or 'N/A'}"
        )
        
        block_tokens = _estimate_tokens(block)
        
        # Check if we'd exceed budget (but always include at least one source)
        if tokens_used + block_tokens > max_tokens and used_ids:
            logger.info(
                "Q&A raw source builder: stopping at %d sources (%d tokens), "
                "%d sources remaining",
                len(used_ids),
                tokens_used,
                len(sources) - len(used_ids),
            )
            break
        
        tokens_used += block_tokens
        used_ids.add(source.id)
        lines.append(block)
    
    logger.info(
        "Q&A raw source context: %d sources, ~%d tokens, "
        "top relevance scores: %s",
        len(used_ids),
        tokens_used,
        [round(s[1], 1) for s in scored_sources[:5]],
    )
    
    return "\n\n".join(lines), used_ids


def _build_qa_instruction(question: str, target_type: str) -> str:
    """
    Build the Q&A instruction prompt.
    
    This prompt emphasizes DETAIL EXTRACTION – surfacing specific facts
    that may have been compressed in the brief for brevity.
    """
    subject_label = "company" if target_type != "person" else "person"

    return f"""
You are answering an ad-hoc research question about the target {subject_label}
for a buy-side investment partner.

QUESTION:
{question}

IMPORTANT: You have access to RAW source data, not summaries. Your job is to
surface SPECIFIC details that may not appear in the executive brief.

Rules:
- Look for SPECIFIC facts: exact names, numbers, dates, identifiers, amounts.
- Prefer precise values over ranges or approximations (e.g., "$15.2M" not "~$15M").
- If sources contain specific identifiers (patent numbers like "EP3966938B1",
  investor names, registration numbers, clinical trial IDs), include them
  even if they seem minor – this is the "alpha" the user is seeking.
- Use bullet-first formatting with bold labels (e.g., '- **Seed investors:** ...').
- Every factual claim MUST include [S<ID>] citations.
- If the specific detail requested is NOT in the sources, say so explicitly:
  "Not disclosed in available sources" – do not guess or use external knowledge.
- If multiple sources contain different details on the same topic, include all
  of them with their respective citations.
- Do NOT invent facts or pull in external knowledge.
""".strip()

def answer_research_question(
    db: Session,
    job: ResearchJob,
    question: str,
    request_id: str | None = None,
) -> ResearchQA:
    # 1. Validate job status
    if job.status != JobStatus.COMPLETED:
        raise ValueError("Q&A is only available once the research job has completed.")

    # 2. Load sources
    sources: list[Source] = (
        db.query(Source).filter(Source.job_id == job.id).all()
    )
    if not sources:
        raise ValueError("No sources available for this job.")

    # 3. Initialize components
    # Seed with existing usage so we accumulate cost correctly
    cost_tracker = LLMCostTracker(job_id=str(job.id))
    
    writer = Writer(
        db=db,
        job_id=job.id,
        request_id=request_id,
        cost_tracker=cost_tracker,
    )

    # 4. Trace start
    trace_job_step(
        job.id,
        phase="QA",
        step="qa:question",
        label="Q&A question received",
        detail=f'Question: "{question[:200]}"',
        meta={"question_length": len(question)},
    )

    # 5. Build context
    target_input = job.target_input or {}
    context_json = {
        "job_id": str(job.id),
        "target_input": target_input,
        "target_type": target_input.get("target_type", "company"),
        "question": question,
    }
    context_str = json.dumps(context_json, indent=2)

    # 6. Build minimal KnowledgeGraph for source filtering
    kg = _build_minimal_kg(target_input)

    # 7. Select sources relevant to the question using section-aware filtering
    relevant_sources = _select_sources_for_question(question, sources, writer, kg)
    
    # 8. Build RAW source context (no LLM summarization)
    # This is the key to surfacing "alpha" – specific details the brief compressed away
    sources_str, used_source_ids = _build_raw_source_context(
        relevant_sources,
        question=question,
        max_tokens=QA_RAW_MAX_TOKENS,
    )
    if not sources_str.strip():
        raise ValueError("Unable to build source context for Q&A.")

    # 9. Generate Answer
    target_type = target_input.get("target_type", "company")
    section_instruction = _build_qa_instruction(question, target_type)

    raw_answer = writer._call_llm(
        section_name="qa",
        section_instruction=section_instruction,
        context=context_str,
        sources_str=sources_str,
    )

    checked = writer._hallucination_check(
        raw_answer,
        used_source_ids,
        section_name="qa",
        section_instruction=section_instruction,
        context_str=context_str,
        sources_str=sources_str,
    )

    final_answer = writer._enforce_numeric_citation_coverage(
        checked,
        used_source_ids,
        section_name="qa",
        section_instruction=section_instruction,
        context_str=context_str,
        sources_str=sources_str,
    )

    # 10. Persist
    # Extract new usage
    summary = cost_tracker.summarize()
    
    # Create QA row
    qa_row = ResearchQA(
        job_id=job.id,
        question=question,
        answer_markdown=final_answer,
        used_source_ids=sorted(list(used_source_ids)),
        llm_usage=summary,
        total_cost_usd=summary.get("total_cost_usd"),
    )
    db.add(qa_row)

    # 11. Merge costs into main job record
    # We need to carefully merge existing usage with new usage
    # However, LLMCostTracker is local. The usage in summary is ONLY for this Q&A session.
    # So we need to merge `summary` into `job.llm_usage`.
    
    current_usage = job.llm_usage or {}
    current_providers = current_usage.get("providers") or {}
    new_providers = summary.get("providers") or {}
    
    for provider_key, new_data in new_providers.items():
        if provider_key not in current_providers:
            current_providers[provider_key] = new_data
        else:
            # Deep merge totals
            curr_p = current_providers[provider_key]
            curr_totals = curr_p.get("totals") or {}
            new_totals = new_data.get("totals") or {}
            
            for k, v in new_totals.items():
                curr_totals[k] = (curr_totals.get(k) or 0) + (v or 0)
            
            curr_p["totals"] = curr_totals
            curr_p["cost_usd"] = (curr_p.get("cost_usd") or 0.0) + (new_data.get("cost_usd") or 0.0)
            
    current_usage["providers"] = current_providers
    current_usage["total_cost_usd"] = (current_usage.get("total_cost_usd") or 0.0) + (summary.get("total_cost_usd") or 0.0)
    
    job.llm_usage = current_usage
    
    # Safe addition: handle None and different types (Decimal vs float)
    current_total = job.total_cost_usd or 0.0
    qa_total = summary.get("total_cost_usd") or 0.0
    job.total_cost_usd = float(current_total) + float(qa_total)
    
    db.commit()
    db.refresh(qa_row)

    # 12. Trace completion
    trace_job_step(
        job.id,
        phase="QA",
        step="qa:answer",
        label="Q&A answer produced",
        detail="Q&A response generated from RAW sources (no summarization).",
        meta={
            "num_sources_used": len(used_source_ids),
            "num_sources_available": len(relevant_sources),
            "answer_chars": len(final_answer or ""),
            "cost_usd": summary.get("total_cost_usd"),
            "raw_source_access": True,
        },
    )

    return qa_row


def answer_with_micro_research_proposal(
    db: Session,
    job: ResearchJob,
    question: str,
    request_id: str | None = None,
) -> QAResult:
    """
    Answer a research question with optional micro-research plan proposal.
    
    This wraps answer_research_question and adds:
    1. Gap detection on the answer
    2. If gap detected, propose a micro-research plan
    3. Return both the answer and optional plan
    
    The plan is NOT executed automatically - the user must confirm via
    the /qa/research/{plan_id}/run endpoint.
    
    Args:
        db: Database session
        job: The completed research job
        question: User's question
        request_id: Optional correlation ID
        
    Returns:
        QAResult with qa_row and optional research_plan
    """
    # 1. Generate the answer (existing flow)
    qa_row = answer_research_question(
        db=db,
        job=job,
        question=question,
        request_id=request_id,
    )
    
    # 2. Load all sources for gap detection
    sources: list[Source] = (
        db.query(Source).filter(Source.job_id == job.id).all()
    )
    used_source_ids = set(qa_row.used_source_ids or [])
    
    # 3. Detect gaps
    gap_result = detect_gap(
        question=question,
        answer_markdown=qa_row.answer_markdown,
        used_source_ids=used_source_ids,
        all_sources=sources,
    )
    
    # 4. If no gap, return answer only
    if not gap_result.should_propose:
        logger.debug("No gap detected, returning answer without plan")
        return QAResult(qa_row=qa_row, research_plan=None)
    
    # 5. Trace gap detection
    trace_job_step(
        job.id,
        phase="QA",
        step="qa_gap_detected",
        label="Gap detected in Q&A answer",
        detail=gap_result.gap_statement[:200],
        meta={
            "intent": gap_result.intent,
            "confidence": gap_result.confidence,
            "detection_method": gap_result.detection_method,
        },
    )
    
    # 6. Propose micro-research plan
    target_input = job.target_input or {}
    
    # Extract existing source context for micro-planner to avoid redundant queries
    existing_providers: set[str] = set()
    existing_domains: set[str] = set()
    for src in sources:
        if src.provider:
            existing_providers.add(src.provider.lower())
        if src.url:
            try:
                parsed = urlparse(src.url)
                if parsed.netloc:
                    existing_domains.add(parsed.netloc.lower())
            except Exception:
                pass
    
    try:
        micro_plan = propose_micro_plan(
            question=question,
            gap_result=gap_result,
            target_input=target_input,
            existing_sources=sources,
            existing_providers=existing_providers,
            existing_domains=existing_domains,
        )
    except Exception as e:
        logger.exception("Failed to propose micro-plan: %s", e)
        # Return answer without plan if planning fails
        return QAResult(qa_row=qa_row, research_plan=None)
    
    if not micro_plan.plan_steps:
        logger.warning("Micro-planner returned no steps, skipping plan proposal")
        return QAResult(qa_row=qa_row, research_plan=None)
    
    # 7. Validate and estimate cost
    validation = validate_and_estimate(
        plan_steps=micro_plan.plan_steps,
        target_input=target_input,
    )
    
    if not validation.is_valid:
        logger.warning(
            "Micro-plan validation failed: %s",
            [e.message for e in validation.errors],
        )
        # Still propose but log the issues
        for err in validation.errors:
            logger.warning("Plan validation error: %s - %s", err.field, err.message)
    
    # 8. Persist the plan
    plan = ResearchQAPlan(
        job_id=job.id,
        qa_id=qa_row.id,
        question=question,
        gap_statement=micro_plan.gap_statement,
        intent=micro_plan.intent,
        plan_steps_json=micro_plan.plan_steps,
        plan_markdown=micro_plan.plan_markdown,
        status=PlanStatus.PROPOSED,
        estimated_cost_label=validation.cost_label,
    )
    db.add(plan)
    db.commit()
    db.refresh(plan)
    
    # 9. Trace plan proposal
    trace_job_step(
        job.id,
        phase="QA",
        step="micro_plan_proposed",
        label="Micro-research plan proposed",
        detail=f"Plan with {len(micro_plan.plan_steps)} steps proposed.",
        meta={
            "plan_id": str(plan.id),
            "estimated_cost": validation.cost_label,
            "steps": len(micro_plan.plan_steps),
        },
    )
    
    logger.info(
        "Micro-research plan proposed: plan_id=%s, steps=%d, cost=%s",
        plan.id,
        len(micro_plan.plan_steps),
        validation.cost_label,
        extra={
            "plan_id": str(plan.id),
            "steps": len(micro_plan.plan_steps),
            "cost_label": validation.cost_label,
        },
    )
    
    return QAResult(qa_row=qa_row, research_plan=plan)

