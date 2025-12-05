"""
Micro-Research Execution Module

Executes micro-research plans, ingests new sources, and re-answers the question.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import UUID

from sqlalchemy.orm import Session

from ..core.config import get_settings
from ..models.research_job import ResearchJob, JobStatus
from ..models.research_qa import ResearchQA
from ..models.research_qa_plan import ResearchQAPlan, PlanStatus
from ..models.source import Source
from ..models.source_excerpt import SourceExcerpt
from .connectors import get_connectors
from .tracing import trace_job_step
from .llm_costs import LLMCostTracker

logger = logging.getLogger(__name__)
settings = get_settings()

# Max snippet length for DB storage (same as writer.py)
MAX_DB_SNIPPET_CHARS = 12000

# Mapping from step name prefixes to provider labels
# Note: More specific prefixes (pdl_company) must come before general ones (pdl)
STEP_TO_PROVIDER: Dict[str, str] = {
    "exa": "exa",
    "openai": "openai-web",
    "pdl_company": "pdl_company",  # Must come before "pdl" to match first
    "pdl": "pdl",
    "gleif": "gleif",
}


def _infer_provider_from_step(step_name: str) -> str:
    """
    Infer the provider label from a step name.
    
    Step names follow patterns like:
    - "micro_exa_news_search_0"
    - "micro_openai_web_search_0"
    - "micro_pdl_person_search_0"
    - "micro_gleif_lei_lookup_0"
    
    Returns the appropriate provider label for source tracking.
    """
    step_lower = step_name.lower()
    
    # Check for known prefixes after "micro_"
    for prefix, provider in STEP_TO_PROVIDER.items():
        if f"_{prefix}_" in step_lower or step_lower.startswith(f"{prefix}_"):
            return provider
    
    # Fallback: try to extract from step name
    if "openai" in step_lower:
        return "openai-web"
    if "exa" in step_lower:
        return "exa"
    if "pdl" in step_lower:
        if "company" in step_lower:
            return "pdl_company"
        return "pdl"
    if "gleif" in step_lower:
        return "gleif"
    
    return "unknown"


def _extract_snippets_from_results(
    raw_results: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Extract source snippets from connector results.
    
    This is similar to how entity_resolution processes web_snippets,
    but simplified for micro-research output.
    
    Provider labeling:
    - Uses item.get("provider") if present in the snippet
    - Otherwise infers from step_name using STEP_TO_PROVIDER mapping
    """
    snippets: List[Dict[str, Any]] = []
    seen_urls: Set[str] = set()
    
    for step_name, payload in raw_results.items():
        if not isinstance(payload, dict):
            continue
        
        # Infer provider from step name for items without explicit provider
        step_provider = _infer_provider_from_step(step_name)
        
        # Handle results/snippets array (Exa, OpenAI, GLEIF all use this)
        generic_results = payload.get("results") or payload.get("snippets") or []
        for item in generic_results:
            if not isinstance(item, dict):
                continue
            
            url = item.get("url") or ""
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            
            # Extract snippet text - Exa may use 'text', 'snippet', or 'highlights'
            snippet_text = (
                item.get("text") or
                item.get("snippet") or
                item.get("description") or
                ""
            )
            
            # Handle Exa highlights (array of strings)
            highlights = item.get("highlights") or []
            if not snippet_text and highlights:
                if isinstance(highlights, list):
                    snippet_text = " ... ".join(str(h) for h in highlights[:5])
            
            if not snippet_text:
                continue
            
            # Use item's provider if present, otherwise infer from step name
            provider = item.get("provider") or step_provider
            
            snippets.append({
                "provider": provider,
                "title": item.get("title") or "Web result",
                "url": url,
                "snippet": snippet_text,
                "published_date": item.get("published_date") or item.get("publishedDate"),
            })
        
        # Handle OpenAI web search results
        openai_snippets = payload.get("web_snippets") or []
        for item in openai_snippets:
            if not isinstance(item, dict):
                continue
            
            url = item.get("url") or ""
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            
            snippet_text = item.get("snippet") or item.get("text") or ""
            if not snippet_text:
                continue
            
            snippets.append({
                "provider": "openai-web",
                "title": item.get("title") or "OpenAI Web Search",
                "url": url,
                "snippet": snippet_text,
                "published_date": item.get("published_date"),
            })
        
        # Handle structured data from OpenAI (competitors, etc.)
        if "structured_output" in payload:
            structured = payload["structured_output"]
            if isinstance(structured, dict):
                # Create a snippet from structured data
                snippet_text = str(structured)[:2000]
                snippets.append({
                    "provider": "openai-web",
                    "title": f"Structured data from {step_name}",
                    "url": None,
                    "snippet": snippet_text,
                    "published_date": None,
                })
        
        # Handle PDL results
        pdl_people = payload.get("people") or []
        for person in pdl_people:
            if not isinstance(person, dict):
                continue
            
            full_name = person.get("full_name") or ""
            # PDL uses 'title' in normalized output, 'job_title' in raw API response
            job_title = person.get("title") or person.get("job_title") or ""
            # PDL uses 'company' in normalized output, 'job_company_name' in raw API response
            company = person.get("company") or person.get("job_company_name") or ""
            linkedin = person.get("linkedin_url") or ""
            
            snippet_parts = []
            if full_name:
                snippet_parts.append(f"Name: {full_name}")
            if job_title:
                snippet_parts.append(f"Title: {job_title}")
            if company:
                snippet_parts.append(f"Company: {company}")
            # Check for pdl_data which contains the full PDL response
            pdl_data = person.get("pdl_data") or person
            education = pdl_data.get("education")
            if education and isinstance(education, list) and education:
                school = education[0].get("school", {})
                school_name = school.get("name", "") if isinstance(school, dict) else ""
                if school_name:
                    snippet_parts.append(f"Education: {school_name}")
            
            if not snippet_parts:
                continue
            
            url = linkedin if linkedin else None
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            
            snippets.append({
                "provider": "pdl",
                "title": f"PDL Person: {full_name}",
                "url": url,
                "snippet": " | ".join(snippet_parts),
                "published_date": None,
            })
        
        # Handle PDL company results
        if "company" in payload:
            company_data = payload["company"]
            if isinstance(company_data, dict):
                name = company_data.get("name") or company_data.get("display_name") or ""
                founded = company_data.get("founded")
                funding = company_data.get("total_funding_raised")
                hq = company_data.get("location", {}).get("locality") if isinstance(company_data.get("location"), dict) else None
                
                snippet_parts = []
                if name:
                    snippet_parts.append(f"Company: {name}")
                if founded:
                    snippet_parts.append(f"Founded: {founded}")
                if funding:
                    snippet_parts.append(f"Total Funding: ${funding:,}" if isinstance(funding, (int, float)) else f"Total Funding: {funding}")
                if hq:
                    snippet_parts.append(f"HQ: {hq}")
                
                if snippet_parts:
                    website = company_data.get("website") or ""
                    if website and website not in seen_urls:
                        seen_urls.add(website)
                    
                    snippets.append({
                        "provider": "pdl_company",
                        "title": f"PDL Company: {name}",
                        "url": website or None,
                        "snippet": " | ".join(snippet_parts),
                        "published_date": None,
                    })
    
    return snippets


def _compute_content_hash(text: str) -> str:
    """
    Compute a SHA256 hash of normalized text for deduplication.
    
    Normalization: lowercase, collapse whitespace.
    """
    normalized = " ".join(text.lower().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _dedupe_snippets(
    new_snippets: List[Dict[str, Any]],
    existing_source_urls: Set[str],
) -> List[Dict[str, Any]]:
    """Remove snippets that already exist in the database (by URL)."""
    deduped = []
    
    for snippet in new_snippets:
        url = snippet.get("url")
        if url and url in existing_source_urls:
            logger.debug("Skipping duplicate URL: %s", url[:100])
            continue
        deduped.append(snippet)
    
    return deduped


def _dedupe_and_store_excerpts(
    db: Session,
    new_snippets: List[Dict[str, Any]],
    existing_sources: List[Source],
    job_id: UUID,
    plan_id: UUID,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Dedupe snippets and store excerpts for duplicate URLs.
    
    For URLs that already exist in the database, instead of discarding
    the snippet entirely, we store the text as an "excerpt" if it's novel.
    This solves the "zero novelty" problem.
    
    Returns:
        Tuple of (truly_new_snippets, excerpt_count)
    """
    # Build URL -> Source mapping for existing sources
    existing_url_to_source: Dict[str, Source] = {}
    for src in existing_sources:
        if src.url:
            existing_url_to_source[src.url] = src
    
    truly_new: List[Dict[str, Any]] = []
    excerpt_count = 0
    
    for snippet in new_snippets:
        url = snippet.get("url")
        text = snippet.get("snippet", "")
        
        if not text:
            continue
        
        if url and url in existing_url_to_source:
            # URL exists - try to store as excerpt
            source = existing_url_to_source[url]
            content_hash = _compute_content_hash(text)
            
            # Check if this exact content already exists as an excerpt
            existing_excerpt = db.query(SourceExcerpt).filter(
                SourceExcerpt.source_id == source.id,
                SourceExcerpt.content_hash == content_hash
            ).first()
            
            if not existing_excerpt:
                # Store as new excerpt
                excerpt = SourceExcerpt(
                    job_id=job_id,
                    source_id=source.id,
                    plan_id=plan_id,
                    excerpt_text=text[:MAX_DB_SNIPPET_CHARS],
                    excerpt_type=snippet.get("provider", "unknown"),
                    content_hash=content_hash,
                )
                db.add(excerpt)
                excerpt_count += 1
                logger.debug(
                    "Stored excerpt for existing URL: %s (hash: %s...)",
                    url[:80] if url else "N/A",
                    content_hash[:12],
                )
            else:
                logger.debug(
                    "Skipping duplicate excerpt content for URL: %s",
                    url[:80] if url else "N/A",
                )
        else:
            # Truly new URL (or no URL)
            truly_new.append(snippet)
    
    if excerpt_count > 0:
        db.commit()
    
    return truly_new, excerpt_count


def _persist_sources(
    db: Session,
    job_id: UUID,
    snippets: List[Dict[str, Any]],
) -> List[Source]:
    """Persist snippets as Source rows."""
    sources: List[Source] = []
    
    for s in snippets:
        snippet_text = s.get("snippet") or ""
        if not snippet_text:
            continue
        
        # Truncate before persisting
        if len(snippet_text) > MAX_DB_SNIPPET_CHARS:
            snippet_text = snippet_text[:MAX_DB_SNIPPET_CHARS]
        
        src = Source(
            job_id=job_id,
            url=s.get("url"),
            title=s.get("title"),
            snippet=snippet_text,
            provider=s.get("provider", "Unknown"),
            published_date=s.get("published_date"),
        )
        db.add(src)
        sources.append(src)
    
    if sources:
        db.commit()
        for src in sources:
            db.refresh(src)
    
    return sources


def execute_micro_research(
    db: Session,
    plan_id: UUID,
    request_id: Optional[str] = None,
) -> Optional[ResearchQA]:
    """
    Execute a micro-research plan and re-answer the question.
    
    1. Load stored ResearchQAPlan by ID (with row locking)
    2. Call ConnectorRunner.execute_plan()
    3. Extract snippets from results
    4. Dedupe against existing sources
    5. If no new evidence, mark NO_CHANGE and skip re-answer
    6. Otherwise, persist new Source rows and re-answer
    
    Args:
        db: Database session
        plan_id: UUID of the ResearchQAPlan to execute
        request_id: Optional correlation ID for tracing
        
    Returns:
        ResearchQA row (new or original if NO_CHANGE), or None if plan not found
    """
    # Import here to avoid circular dependency
    from .qa import answer_research_question
    
    # 1. Load the plan WITH ROW LOCKING to prevent concurrent execution
    plan = db.query(ResearchQAPlan).filter(
        ResearchQAPlan.id == plan_id
    ).with_for_update().first()
    
    if not plan:
        raise ValueError(f"Plan not found: {plan_id}")
    
    if plan.status != PlanStatus.PROPOSED:
        raise ValueError(f"Plan is not in PROPOSED status: {plan.status}")
    
    # Load the job
    job = db.query(ResearchJob).filter(ResearchJob.id == plan.job_id).first()
    if not job:
        raise ValueError(f"Job not found: {plan.job_id}")
    
    if job.status != JobStatus.COMPLETED:
        raise ValueError("Micro-research requires a completed job")
    
    # Update plan status to RUNNING
    plan.status = PlanStatus.RUNNING
    plan.confirmed_at = datetime.utcnow()
    db.commit()
    
    # Trace: plan confirmed
    trace_job_step(
        job.id,
        phase="QA_RESEARCH",
        step="micro_plan_confirmed",
        label="Micro-research plan confirmed",
        detail=f"Executing {len(plan.plan_steps_json or [])} research steps.",
        meta={"plan_id": str(plan_id)},
    )
    
    cost_tracker = LLMCostTracker(job_id=str(job.id))
    qa_row: Optional[ResearchQA] = None
    
    try:
        # 2. Execute connectors
        connectors = get_connectors()
        plan_steps = plan.plan_steps_json or []
        target_input = job.target_input or {}
        
        trace_job_step(
            job.id,
            phase="QA_RESEARCH",
            step="micro_connectors:start",
            label="Running micro-research connectors",
            detail=f"Executing {len(plan_steps)} connector steps.",
        )
        
        raw_results = connectors.execute_plan(plan_steps, target_input, job_id=job.id)
        
        trace_job_step(
            job.id,
            phase="QA_RESEARCH",
            step="micro_connectors:done",
            label="Micro-research connectors complete",
            detail="Connector execution finished.",
            meta={"steps_with_results": [k for k, v in (raw_results or {}).items() if v]},
        )
        
        # 3. Extract snippets
        new_snippets = _extract_snippets_from_results(raw_results or {})
        
        logger.info(
            "Micro-research extracted %d snippets",
            len(new_snippets),
            extra={"plan_id": str(plan_id), "snippet_count": len(new_snippets)},
        )
        
        # 4. Dedupe against existing sources AND store excerpts for duplicate URLs
        existing_sources = db.query(Source).filter(Source.job_id == job.id).all()
        
        # Use new dedupe function that also stores excerpts
        deduped_snippets, excerpt_count = _dedupe_and_store_excerpts(
            db=db,
            new_snippets=new_snippets,
            existing_sources=existing_sources,
            job_id=job.id,
            plan_id=plan.id,
        )
        
        logger.info(
            "Micro-research after dedupe: %d new snippets, %d excerpts stored (from %d total)",
            len(deduped_snippets),
            excerpt_count,
            len(new_snippets),
            extra={
                "deduped_count": len(deduped_snippets),
                "excerpt_count": excerpt_count,
                "total_extracted": len(new_snippets),
            },
        )
        
        # 5. Check if we have any new evidence (either new sources OR new excerpts)
        has_new_evidence = len(deduped_snippets) > 0 or excerpt_count > 0
        
        if not has_new_evidence:
            # NO NEW EVIDENCE - skip re-answer to save LLM costs
            plan.status = PlanStatus.NO_CHANGE
            plan.created_source_ids = []
            
            trace_job_step(
                job.id,
                phase="QA_RESEARCH",
                step="micro_no_change",
                label="No new evidence found",
                detail=f"All {len(new_snippets)} extracted snippets were exact duplicates.",
                meta={"extracted_count": len(new_snippets), "duplicate_count": len(new_snippets)},
            )
            
            # Return the original QA row if available
            if plan.qa_id:
                qa_row = db.query(ResearchQA).filter(ResearchQA.id == plan.qa_id).first()
            
            logger.info(
                "Micro-research completed with NO_CHANGE: plan_id=%s, no new evidence",
                plan_id,
                extra={"plan_id": str(plan_id), "status": "NO_CHANGE"},
            )
        else:
            # 6. Persist new sources (if any)
            new_sources = _persist_sources(db, job.id, deduped_snippets) if deduped_snippets else []
            created_source_ids = [s.id for s in new_sources]
            
            trace_job_step(
                job.id,
                phase="QA_RESEARCH",
                step="micro_sources_ingested",
                label=f"Ingested {len(new_sources)} new sources + {excerpt_count} excerpts",
                detail="New evidence added to knowledge base.",
                meta={
                    "new_source_ids": created_source_ids,
                    "excerpt_count": excerpt_count,
                },
            )
            
            # 7. Re-answer the question
            trace_job_step(
                job.id,
                phase="QA_RESEARCH",
                step="micro_reanswer:start",
                label="Re-answering with new evidence",
                detail="Generating updated answer using expanded source set.",
            )
            
            qa_row = answer_research_question(
                db=db,
                job=job,
                question=plan.question,
                request_id=request_id,
            )
            
            trace_job_step(
                job.id,
                phase="QA_RESEARCH",
                step="micro_reanswer:done",
                label="Micro-research complete",
                detail="Updated answer generated with new evidence.",
                meta={
                    "new_sources_count": len(new_sources),
                    "new_excerpt_count": excerpt_count,
                    "total_sources": len(existing_sources) + len(new_sources),
                },
            )
            
            # Update plan status
            plan.status = PlanStatus.COMPLETED
            plan.created_source_ids = created_source_ids
            plan.result_qa_id = qa_row.id
            
            logger.info(
                "Micro-research completed: plan_id=%s, new_sources=%d, qa_id=%d",
                plan_id,
                len(new_sources),
                qa_row.id,
                extra={
                    "plan_id": str(plan_id),
                    "new_sources": len(new_sources),
                    "qa_id": qa_row.id,
                },
            )
        
        # Store cost info (for both COMPLETED and NO_CHANGE)
        usage_summary = cost_tracker.summarize()
        plan.llm_usage = usage_summary
        plan.total_cost_usd = usage_summary.get("total_cost_usd")
        
        return qa_row
        
    except Exception as e:
        # Mark plan as failed
        plan.status = PlanStatus.FAILED
        plan.error_message = str(e)[:500]
        
        trace_job_step(
            job.id,
            phase="QA_RESEARCH",
            step="micro_research:failed",
            label="Micro-research failed",
            detail=str(e)[:200],
        )
        
        logger.exception("Micro-research failed: %s", e)
        raise
        
    finally:
        # ALWAYS finalize: ensure plan is not left in RUNNING state
        if plan.status == PlanStatus.RUNNING:
            plan.status = PlanStatus.FAILED
            plan.error_message = plan.error_message or "Unexpected termination"
        plan.completed_at = datetime.utcnow()
        db.commit()


def sweep_stale_plans(db: Session, timeout_minutes: int = 30) -> int:
    """
    Mark plans stuck in RUNNING status for too long as FAILED.
    
    This should be called periodically (e.g., via Celery beat) to clean up
    plans that got stuck due to worker crashes, timeouts, or other issues.
    
    Args:
        db: Database session
        timeout_minutes: How long a plan can be RUNNING before considered stale
        
    Returns:
        Number of plans marked as FAILED
    """
    cutoff = datetime.utcnow() - timedelta(minutes=timeout_minutes)
    
    stale_plans = db.query(ResearchQAPlan).filter(
        ResearchQAPlan.status == PlanStatus.RUNNING,
        ResearchQAPlan.confirmed_at < cutoff
    ).all()
    
    for plan in stale_plans:
        plan.status = PlanStatus.FAILED
        plan.completed_at = datetime.utcnow()
        plan.error_message = f"Timed out after {timeout_minutes} minutes"
        
        logger.warning(
            "Marked stale plan as FAILED: plan_id=%s, confirmed_at=%s",
            plan.id,
            plan.confirmed_at,
            extra={"plan_id": str(plan.id), "timeout_minutes": timeout_minutes},
        )
    
    if stale_plans:
        db.commit()
    
    return len(stale_plans)

