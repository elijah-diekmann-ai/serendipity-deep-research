from uuid import UUID
from sqlalchemy.orm import Session
import json
from typing import Tuple, Set

from ..models.research_job import ResearchJob, JobStatus
from ..models.source import Source
from ..models.research_qa import ResearchQA
from .writer import Writer
from .llm_costs import LLMCostTracker
from .tracing import trace_job_step

def _build_qa_instruction(question: str, target_type: str) -> str:
    subject_label = "company"
    if target_type == "person":
        subject_label = "person"

    return f"""
You are answering an ad-hoc research question about the target {subject_label}
for a buy-side investment partner.

QUESTION:
{question}

Rules:
- Answer directly and concisely, assuming the reader has the full brief.
- Use bullet-first formatting with bold labels where helpful (e.g. '- **Regulatory approvals:** ...').
- Every factual sentence MUST end with one or more [S<ID>] citations referencing the sources.
- Use only the evidence in the SOURCES block; if something is not supported, either omit it
  or explicitly say that it is not visible in the available sources.
- If the question has multiple parts, structure the answer into clear bullets or short subsections.
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

    # 6. Build source string
    sources_str, used_source_ids = writer._build_source_list(sources)
    if not sources_str.strip():
        raise ValueError("Unable to build source context for Q&A.")

    # 7. Generate Answer
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

    # 8. Persist
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

    # 9. Merge costs into main job record
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

    # 10. Trace completion
    trace_job_step(
        job.id,
        phase="QA",
        step="qa:answer",
        label="Q&A answer produced",
        detail="Q&A response generated from existing sources.",
        meta={
            "num_sources_used": len(used_source_ids),
            "answer_chars": len(final_answer or ""),
            "cost_usd": summary.get("total_cost_usd"),
        },
    )

    return qa_row

