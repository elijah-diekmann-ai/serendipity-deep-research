from __future__ import annotations

from uuid import UUID
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from decimal import Decimal

from ..core.celery_app import celery_app
from ..core.db import SessionLocal
from ..models.research_job import ResearchJob, JobStatus
from ..models.brief import Brief
from ..models.company import Company
from ..models.person import Person
from .planner import plan_research
from .connectors import get_connectors
from .entity_resolution import resolve_entities, KnowledgeGraph
from .intent import normalize_target_input
from .writer import Writer
from .tracing import trace_job_step
from .llm_costs import LLMCostTracker

logger = logging.getLogger(__name__)


def _persist_company_and_people(db: Session, kg: KnowledgeGraph) -> None:
    """
    Upsert Company and Person rows based on the resolved KnowledgeGraph.
    """
    if getattr(kg, "target_type", "company") != "company":
        # Person-target jobs should not create placeholder company records.
        return

    company_node = kg.company

    # Upsert company by domain when available; otherwise treat as a new record
    company: Company | None = None
    if company_node.domain:
        company = (
            db.query(Company)
            .filter(Company.domain == company_node.domain)
            .with_for_update()
            .first()
        )

    if not company:
        try:
            with db.begin_nested():
                company = Company(
                    name=company_node.name,
                    domain=company_node.domain,
                    domain_confidence=company_node.domain_confidence,
                    domain_source=company_node.domain_source,
                )
                db.add(company)
                db.flush()
        except IntegrityError:
            if company_node.domain:
                company = (
                    db.query(Company)
                    .filter(Company.domain == company_node.domain)
                    .with_for_update()
                    .first()
                )
            if not company:
                raise

    identifiers = company.identifiers or {}
    if company_node.companies_house_number:
        identifiers["companies_house"] = company_node.companies_house_number
    if company_node.apollo_organization_id:
        identifiers["apollo_organization"] = company_node.apollo_organization_id
    company.identifiers = identifiers or None
    company.profile_data = company_node.profile or {}
    
    # Update confidence fields if they changed (e.g. re-resolved with better confidence)
    if company_node.domain_confidence is not None:
        company.domain_confidence = company_node.domain_confidence
    if company_node.domain_source:
        company.domain_source = company_node.domain_source

    db.add(company)
    db.flush()

    existing_people = db.query(Person).filter(Person.company_id == company.id).all()

    people_by_linkedin = {p.linkedin_url: p for p in existing_people if p.linkedin_url}
    people_by_name = {p.full_name.lower().strip(): p for p in existing_people if p.full_name}

    for p in company_node.people:
        person_record = None

        if p.linkedin_url and p.linkedin_url in people_by_linkedin:
            person_record = people_by_linkedin[p.linkedin_url]
        elif p.full_name and p.full_name.lower().strip() in people_by_name:
            person_record = people_by_name[p.full_name.lower().strip()]

        if person_record:
            person_record.full_name = p.full_name
            if p.linkedin_url:
                person_record.linkedin_url = p.linkedin_url
            if p.roles:
                person_record.current_role = p.roles[0]

            if p.enrichment:
                # Merge enrichment data per provider instead of blind dict.update
                current_data = dict(person_record.enrichment_data or {})
                for provider_key, payload in p.enrichment.items():
                    existing_payload = current_data.get(provider_key) or {}
                    if isinstance(existing_payload, dict) and isinstance(payload, dict):
                        merged_payload = {**existing_payload, **payload}
                    else:
                        merged_payload = payload
                    current_data[provider_key] = merged_payload
                person_record.enrichment_data = current_data or None

            db.add(person_record)
        else:
            new_person = Person(
                full_name=p.full_name,
                linkedin_url=p.linkedin_url,
                current_role=p.roles[0] if p.roles else None,
                company_id=company.id,
                enrichment_data=p.enrichment or None,
            )
            db.add(new_person)

    db.commit()


@celery_app.task(name="app.services.orchestrator.run_research_job", bind=True, queue="research")
def run_research_job(self, job_id: str):
    db: Session = SessionLocal()
    try:
        job = db.query(ResearchJob).filter(ResearchJob.id == UUID(job_id)).first()
        if not job:
            return

        raw_target_input = job.target_input or {}
        target_input = normalize_target_input(raw_target_input)
        request_id = target_input.get("request_id")
        tracker = LLMCostTracker(job_id=str(job.id))

        trace_job_step(
            job.id,
            phase="INIT",
            step="job_received",
            label="Job received by research worker",
            detail="Queued research request has been picked up by a worker.",
            meta={"target_input": {k: target_input.get(k) for k in ("company_name", "website")}},
        )

        logger.info(
            "Starting research job",
            extra={"job_id": str(job.id), "request_id": request_id, "step": "start"},
        )

        job.status = JobStatus.PROCESSING
        db.commit()

        # Phase 1: Planning
        trace_job_step(
            job.id,
            phase="PLANNING",
            step="plan_research:start",
            label="Planning research strategy",
            detail="Designing deterministic Exa-first plan for this target.",
        )
        plan = plan_research(target_input)
        trace_job_step(
            job.id,
            phase="PLANNING",
            step="plan_research:done",
            label="Research plan created",
            detail=f"{len(plan)} steps scheduled across connectors.",
            meta={"steps": [s["name"] for s in plan]},
        )
        logger.info(
            "Plan generated",
            extra={"job_id": str(job.id), "request_id": request_id, "step": "plan"},
        )

        # Phase 2: Parallel Execution
        connectors = get_connectors()
        # Attach request_id for downstream connector logging if needed
        setattr(connectors, "request_id", request_id)

        trace_job_step(
            job.id,
            phase="COLLECTION",
            step="connectors:start",
            label="Collecting sources from the web and registries",
            detail="Running connector plan (Exa + registries) in parallel.",
        )
        raw_results = connectors.execute_plan(plan, target_input)
        trace_job_step(
            job.id,
            phase="COLLECTION",
            step="connectors:done",
            label="Finished collecting raw sources",
            detail="Connectors returned snippets for entity resolution.",
            meta={"steps_with_results": [k for k, v in (raw_results or {}).items() if v]},
        )
        logger.info(
            "Connectors executed",
            extra={"job_id": str(job.id), "request_id": request_id, "step": "connectors"},
        )

        # Record connector usage (e.g., OpenAI web search)
        for step_name, payload in (raw_results or {}).items():
            if not isinstance(payload, dict):
                continue
            usage = payload.get("usage") or {}
            if not usage:
                continue
            cost_meta = payload.get("cost") or {}
            tracker.add_record(
                provider="openai",
                model=usage.get("model"),
                kind=f"openai_web:{step_name}",
                section=None,
                input_tokens=int(usage.get("input_tokens") or 0),
                output_tokens=int(usage.get("output_tokens") or 0),
                cached_input_tokens=int(usage.get("cached_input_tokens") or 0),
                reasoning_output_tokens=int(usage.get("reasoning_output_tokens") or 0),
                web_search_calls=int(usage.get("web_search_calls") or 0),
                tool_cost_usd=float(cost_meta.get("web_search_tool_cost_usd") or 0.0),
                cost_usd=float(cost_meta.get("model_cost_usd") or 0.0),
            )

        connector_usage_preview = tracker.summarize()
        openai_totals = connector_usage_preview.get("providers", {}).get("openai")
        if openai_totals:
            trace_job_step(
                job.id,
                phase="COSTS",
                step="connectors",
                label="Accumulated OpenAI web-search usage",
                detail="Web search token usage recorded.",
                meta={
                    "openai_totals": openai_totals.get("totals", {}),
                    "openai_cost_usd": openai_totals.get("cost_usd"),
                },
            )

        # Phase 3: Entity Resolution
        trace_job_step(
            job.id,
            phase="ENTITY_RESOLUTION",
            step="resolve_entities:start",
            label="Normalising entities into a knowledge graph",
            detail="Resolving company, domain, and people from raw connector outputs.",
        )
        kg = resolve_entities(raw_results, target_input)

        trace_job_step(
            job.id,
            phase="ENTITY_RESOLUTION",
            step="resolve_entities:done",
            label="Knowledge graph built",
            detail="Resolved company profile, domain, and leadership set.",
            meta={
                "company_name": kg.company.name,
                "domain": kg.company.domain,
                "num_people": len(kg.company.people),
                "num_web_snippets": len(kg.company.web_snippets),
            },
        )

        # Phase 3b: Persist structured entities (Company/People)
        _persist_company_and_people(db, kg)

        # Phase 4 & 5: Drafting
        trace_job_step(
            job.id,
            phase="WRITING",
            step="writer:start",
            label="Drafting investment brief",
            detail="Compressing sources and drafting sections with the LLM.",
        )
        writer = Writer(
            db=db,
            job_id=job.id,
            request_id=request_id,
            cost_tracker=tracker,
        )
        brief_json = writer.generate_brief(kg)

        trace_job_step(
            job.id,
            phase="WRITING",
            step="writer:done",
            label="Brief drafted",
            detail="All sections generated; persisting to storage.",
        )

        brief = Brief(job_id=job.id, content_json=brief_json)
        db.merge(brief)
        job.status = JobStatus.COMPLETED
        job.completed_at = datetime.utcnow()
        summary_usage = tracker.summarize()
        job.llm_usage = summary_usage
        total_cost_value = summary_usage.get("total_cost_usd")
        if total_cost_value is not None:
            job.total_cost_usd = Decimal(str(total_cost_value))
        else:
            job.total_cost_usd = None
        db.commit()

        trace_job_step(
            job.id,
            phase="DONE",
            step="job:completed",
            label="Research job completed",
            detail="Brief is ready to view.",
        )
        if total_cost_value is not None:
            trace_job_step(
                job.id,
                phase="COSTS",
                step="final",
                label="LLM usage recorded",
                detail="Drafting and web-search costs captured.",
                meta={"total_cost_usd": float(total_cost_value)},
            )

        logger.info(
            "Research job completed",
            extra={"job_id": str(job.id), "request_id": request_id, "step": "completed"},
        )
    except Exception as e:
        db.rollback()
        job = db.query(ResearchJob).filter(ResearchJob.id == UUID(job_id)).first()
        if job:
            request_id = (job.target_input or {}).get("request_id")
            job.status = JobStatus.FAILED
            job.error_message = str(e)[:500]
            job.completed_at = datetime.utcnow()
            db.commit()
            logger.exception(
                "Research job failed",
                extra={"job_id": str(job.id), "request_id": request_id, "step": "failed"},
            )
        raise
    finally:
        db.close()
