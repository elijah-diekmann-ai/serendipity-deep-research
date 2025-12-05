"""
Micro-Planner Module for Q&A Additional Research

Uses an LLM to generate a restricted retrieval DSL, then translates it to
PlanStep format compatible with ConnectorRunner.execute_plan().
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

from ..core.config import get_settings
from ..models.source import Source
from .qa_gap import GapDetectionResult
from .llm import get_llm_client, limit_llm_concurrency
from .planner import PlanStep
from .micro_plan_dsl import parse_task_with_repair, MicroTaskType

logger = logging.getLogger(__name__)
settings = get_settings()


# ---------------------------------------------------------------------------
# Micro-Research Limits
# ---------------------------------------------------------------------------

MAX_MICRO_STEPS = 4
MAX_MICRO_EXA_QUERIES = 3

# Task types the LLM can output (restricted DSL)
ALLOWED_TASK_TYPES = {
    "exa_news_search",
    "exa_site_search",
    "exa_funding_search",
    "exa_patent_search",
    "exa_general_search",
    "exa_similar_search",      # Competitor/similar company discovery
    "exa_research_paper",      # Academic/technical papers
    "exa_historical_search",   # Time-bounded historical research
    "openai_web_search",
    "pdl_person_search",       # Legacy - will be converted to pdl_person_enrich or pdl_company_leadership
    "pdl_person_enrich",       # Person enrichment (requires person_name)
    "pdl_company_leadership",  # Company leadership discovery (no person_name needed)
    "pdl_company_search",
    "gleif_lei_lookup",        # Legal Entity Identifier registry lookup
}

# Map from DSL task types to connector names
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
    "pdl_person_search": "pdl",          # Legacy support
    "pdl_person_enrich": "pdl",          # Person enrichment
    "pdl_company_leadership": "pdl",     # Company leadership search
    "pdl_company_search": "pdl_company",
    "gleif_lei_lookup": "gleif",
}

# Domains to exclude for cleaner primary-source results
EXCLUDE_AGGREGATOR_DOMAINS = [
    "crunchbase.com",
    "pitchbook.com",
    "linkedin.com",
    "bloomberg.com",
    "wikipedia.org",
    "glassdoor.com",
    "zoominfo.com",
    "apollo.io",
    "golden.com",
    "tracxn.com",
    "owler.com",
]

# ---------------------------------------------------------------------------
# Query Hint Derivation
# ---------------------------------------------------------------------------

# Stopwords for query hint extraction
_QUERY_HINT_STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "what", "who", "where",
    "when", "how", "does", "do", "did", "have", "has", "their", "they",
    "this", "that", "for", "with", "and", "or", "can", "you", "look",
    "up", "search", "find", "dig", "deeper", "more", "about", "any",
}

# Customer-related terms for synonym expansion
_CUSTOMER_TERMS = {"customer", "customers", "client", "clients", "commercial"}
_CUSTOMER_SYNONYMS = "customers clients commercial partner partnership case study deployment contract"

# Common acronyms to EXCLUDE from must-include (too generic or stopwords)
_GENERIC_ACRONYMS = {
    "US", "UK", "EU", "UK", "CEO", "CFO", "CTO", "COO", "VP", "HR",
    "LLC", "INC", "LTD", "PTY", "CO", "OR", "AND", "THE", "FOR",
}


def _extract_must_include_terms(question: str) -> List[str]:
    """
    Extract terms that MUST appear in search queries.
    
    These are high-value specific entities that should be preserved verbatim:
    - Double-quoted strings: "SYSTEM AND METHOD FOR CONTROLLING..."
    - Single-quoted keywords: 'control', 'readout'
    - All-caps acronyms: DARPA, QBI, IQMP, DOE (2-6 chars)
    - Title Case multi-word spans: "Quantum Benchmarking Initiative"
    
    Returns:
        List of must-include terms (deduplicated)
    """
    terms: List[str] = []
    
    # 1. Double-quoted strings (e.g., "SYSTEM AND METHOD FOR...")
    double_quoted = re.findall(r'"([^"]+)"', question)
    terms.extend(double_quoted)
    
    # 2. Single-quoted keywords (e.g., 'control', 'readout')
    single_quoted = re.findall(r"'([^']+)'", question)
    terms.extend(single_quoted)
    
    # 3. All-caps acronyms (2-6 chars, excluding generic ones)
    acronyms = re.findall(r'\b([A-Z]{2,6})\b', question)
    for acr in acronyms:
        if acr not in _GENERIC_ACRONYMS:
            terms.append(acr)
    
    # 4. Title Case multi-word spans (potential program/initiative names)
    # Match 3+ consecutive Title Case words: "Quantum Benchmarking Initiative"
    title_case_spans = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){2,})\b', question)
    terms.extend(title_case_spans)
    
    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: List[str] = []
    for term in terms:
        term_lower = term.lower()
        if term_lower not in seen and len(term) > 1:
            seen.add(term_lower)
            unique.append(term)
    
    return unique


def _derive_query_hint(question: str, company_name: str | None = None) -> str:
    """
    Extract a compact, search-optimized hint from the user's question.
    Returns a short string suitable for Exa query_hint.
    """
    q_lower = question.lower()
    
    # If question contains customer terms, return curated synonym pack
    if any(term in q_lower for term in _CUSTOMER_TERMS):
        return _CUSTOMER_SYNONYMS
    
    # Otherwise, extract keywords from question
    words = re.findall(r'\b\w+\b', q_lower)
    
    # Remove stopwords, company name tokens, and short words
    company_tokens: set[str] = set()
    if company_name:
        company_tokens = {t.lower() for t in re.findall(r'\b\w+\b', company_name.lower())}
    
    keywords = [
        w for w in words
        if w not in _QUERY_HINT_STOPWORDS
        and w not in company_tokens
        and len(w) > 2
    ]
    
    # Return up to 8 unique keywords
    seen: set[str] = set()
    unique: list[str] = []
    for kw in keywords:
        if kw not in seen:
            seen.add(kw)
            unique.append(kw)
        if len(unique) >= 8:
            break
    
    return " ".join(unique)


@dataclass
class MicroPlanTask:
    """A single task in the micro-research DSL."""
    type: str
    priority: str = "medium"  # high, medium, low
    query_hint: Optional[str] = None
    # Explicit OpenAI mode (competitors, founding, leadership, person, news)
    openai_mode: Optional[str] = None
    # Person name for PDL/OpenAI person lookups
    person_name: Optional[str] = None
    # Exa-specific params
    subpage_targets: Optional[List[str]] = None
    highlights_query: Optional[str] = None
    # Date range overrides
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@dataclass
class MicroPlanDSL:
    """The LLM-generated retrieval DSL."""
    gap: str
    intent: str
    tasks: List[MicroPlanTask] = field(default_factory=list)
    slot_hints: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MicroPlan:
    """The final micro-research plan with PlanStep format."""
    gap_statement: str
    intent: Optional[str]
    plan_steps: List[PlanStep]
    plan_markdown: str  # Human-readable summary for UI
    estimated_queries: int  # For cost estimation


def _extract_domain(website: Optional[str]) -> Optional[str]:
    """Extract domain from website URL, stripping www. prefix for consistency."""
    if not website:
        return None
    try:
        if "://" not in website:
            website = "https://" + website
        parsed = urlparse(website)
        domain = parsed.netloc or None
        # Strip www. prefix for consistent matching
        if domain and domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        raw = website.split("://")[-1].split("/")[0]
        # Strip www. prefix
        if raw.startswith("www."):
            raw = raw[4:]
        return raw


# Jurisdiction to ISO country code mapping
JURISDICTION_TO_COUNTRY_CODE: Dict[str, str] = {
    "us": "US",
    "usa": "US",
    "united states": "US",
    "uk": "GB",
    "united kingdom": "GB",
    "gb": "GB",
    "eu": "EU",
    "australia": "AU",
    "au": "AU",
    "canada": "CA",
    "ca": "CA",
    "germany": "DE",
    "de": "DE",
    "france": "FR",
    "fr": "FR",
    "japan": "JP",
    "jp": "JP",
    "china": "CN",
    "cn": "CN",
    "india": "IN",
    "in": "IN",
    "singapore": "SG",
    "sg": "SG",
    "hong kong": "HK",
    "hk": "HK",
    "ireland": "IE",
    "ie": "IE",
    "netherlands": "NL",
    "nl": "NL",
    "switzerland": "CH",
    "ch": "CH",
}


def _normalize_jurisdiction_to_country_code(jurisdiction: Optional[str]) -> Optional[str]:
    """Convert jurisdiction string to ISO country code."""
    if not jurisdiction:
        return None
    normalized = jurisdiction.lower().strip()
    return JURISDICTION_TO_COUNTRY_CODE.get(normalized, jurisdiction.upper()[:2])


def _build_planner_prompt(
    question: str,
    gap_result: GapDetectionResult,
    target_input: dict,
    existing_source_count: int,
    existing_providers: Optional[Set[str]] = None,
    existing_domains: Optional[Set[str]] = None,
    missing_slots: Optional[Dict[str, Any]] = None,
) -> str:
    """Build the prompt for the LLM micro-planner."""
    company_name = target_input.get("company_name", "")
    website = target_input.get("website", "")
    context = target_input.get("context", "")
    domain = _extract_domain(website) or ""
    
    # Build existing research context
    existing_info = ""
    if existing_providers:
        existing_info += f"\n- Already queried providers: {', '.join(sorted(existing_providers))}"
    if existing_domains:
        domains_list = sorted(existing_domains)[:10]
        existing_info += f"\n- Already crawled domains: {', '.join(domains_list)}"
        if len(existing_domains) > 10:
            existing_info += f" (and {len(existing_domains) - 10} more)"
    
    # Build detected slots section
    slots_info = ""
    if missing_slots:
        slots_parts = []
        if missing_slots.get("years"):
            slots_parts.append(f"Years mentioned: {', '.join(str(y) for y in missing_slots['years'])}")
        if missing_slots.get("round"):
            slots_parts.append(f"Funding round: {missing_slots['round']}")
        if missing_slots.get("country_code"):
            slots_parts.append(f"Country/Jurisdiction: {missing_slots['country_code']}")
        if missing_slots.get("person_name"):
            slots_parts.append(f"Person name: {missing_slots['person_name']}")
        if missing_slots.get("query_hint"):
            slots_parts.append(f"Query hint: {missing_slots['query_hint']}")
        if slots_parts:
            slots_info = "\n## DETECTED SLOTS (use these in slot_hints)\n- " + "\n- ".join(slots_parts)
    
    return f"""You are a research planning assistant. A user asked a question about a company,
but the existing sources did not fully answer it. Your job is to propose a minimal
set of targeted searches to fill the gap.

## TARGET COMPANY
- Name: {company_name}
- Website: {website}
- Domain: {domain}
- Context: {context}

## USER QUESTION
{question}

## GAP ANALYSIS
- Gap: {gap_result.gap_statement}
- Detected Intent: {gap_result.intent or "general"}{slots_info}

## EXISTING RESEARCH
- Sources collected: {existing_source_count}{existing_info}
- Avoid re-querying the same providers/domains unless you believe different parameters will yield new results.

## PROVIDER NAME MAPPING
Source database uses these provider labels:
- "exa" -> Exa connector
- "openai-web" -> OpenAI web search connector (note: hyphen, not underscore)
- "pdl" -> PDL person connector  
- "pdl_company" -> PDL company connector
- "gleif" -> GLEIF LEI registry

## AVAILABLE CONNECTORS

### 1. EXA (Neural Search)
Best for: Raw web content, date-filtered news, deep site crawling, similar company discovery.
Cost: Medium (~$0.01/query)

| Task Type | Use Case | Key Params |
|-----------|----------|------------|
| exa_site_search | Content ON company website | subpage_targets, highlights_query |
| exa_news_search | Press coverage, announcements | start_date, end_date |
| exa_funding_search | Investor names, round details | start_date (default: 5 years) |
| exa_patent_search | Patent numbers, IP filings | highlights_query |
| exa_general_search | Broad primary-source search | exclude aggregators |
| exa_similar_search | Find competitor/similar companies | url required |
| exa_research_paper | Academic papers | category: research paper |
| exa_historical_search | Time-bounded events | start_date, end_date REQUIRED |

### 2. OPENAI WEB SEARCH (AI-Powered Reasoning)
Best for: Complex reasoning, structured extraction, ambiguous questions.
Cost: High (~$0.05-0.10/call)

| Mode | Use Case |
|------|----------|
| competitors | Discover and categorize competitors |
| founding | Legal entity, incorporation, registration numbers |
| leadership | Founders, executives, board members |
| person | Individual biography and career history (requires person_name) |
| news | Categorized recent news events |

### 3. PDL (Structured LinkedIn Data)
Best for: Verified professional data, company firmographics.
Cost: Low (~$0.01/call)

| Task Type | Returns |
|-----------|---------|
| pdl_person_search | Work history, education, LinkedIn (requires person_name) |
| pdl_company_search | Founded year, HQ, headcount, total_funding_raised, funding_details |

### 4. GLEIF (Legal Entity Registry)
Best for: LEI, legal entity name, jurisdiction, registration authority IDs.
Cost: Free

| Task Type | Returns |
|-----------|---------|
| gleif_lei_lookup | LEI, legal_name, jurisdiction, registration_authority_entity_id |

## OUTPUT FORMAT
Respond with valid JSON:
{{
  "gap": "Brief description of what's missing",
  "intent": "funding_investors|patents|litigation|founder_background|competitors|technology|regulatory|legal_entity|acquisitions|customers|general",
  "tasks": [
    {{
      "type": "openai_web_search",
      "openai_mode": "founding",
      "priority": "high",
      "query_hint": "legal entity registration SEC filings"
    }},
    {{
      "type": "pdl_person_search",
      "person_name": "John Smith",
      "priority": "medium"
    }},
    {{
      "type": "exa_site_search",
      "subpage_targets": ["api", "developers", "docs"],
      "highlights_query": "API endpoints authentication SDK",
      "priority": "medium"
    }}
  ],
  "slot_hints": {{"years": ["2023", "2024"], "country_code": "US"}}
}}

## RULES
1. Propose 1-3 tasks maximum
2. For openai_web_search, ALWAYS include "openai_mode"
3. For pdl_person_search or openai_mode="person", ALWAYS include "person_name"
4. For exa_site_search with specific needs, include "subpage_targets" and "highlights_query"
5. Prefer PDL for structured data (funding totals, headcount) over Exa press releases
6. Prefer GLEIF for LEI/legal entity questions over OpenAI
7. Avoid re-querying providers/domains already searched
8. If the question contains specific topic terms (customers, patents, etc.), `query_hint` MUST include those terms

## CUSTOMER/COMMERCIAL QUESTIONS
For questions about customers, clients, commercial deals, or partnerships:
- Use `exa_site_search` with subpage_targets: ["customers", "case-studies", "success-stories", "partners"]
- Use `exa_news_search` with 5-year date window for partnership announcements
- `query_hint` MUST include customer synonyms: "customers clients commercial partner case study deployment contract"
- `highlights_query` should target: "customer client partner agreement deployment contract procurement"
"""


def _parse_llm_response(
    response_text: str,
    default_slots: Optional[Dict[str, Any]] = None,
) -> Optional[MicroPlanDSL]:
    """
    Parse the LLM response into MicroPlanDSL.
    
    Uses Pydantic validation with repair logic for common issues:
    - Missing openai_mode for openai_web_search
    - Missing person_name for person mode
    - Legacy pdl_person_search conversion
    """
    try:
        # Try to extract JSON from the response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if not json_match:
            logger.warning("No JSON found in LLM response")
            return None
        
        data = json.loads(json_match.group())
        default_slots = default_slots or {}
        
        # Merge LLM slot_hints with default_slots for repair context
        slot_hints = {**default_slots, **data.get("slot_hints", {})}
        
        tasks = []
        for t in data.get("tasks", []):
            task_type = t.get("type", "")
            if task_type not in ALLOWED_TASK_TYPES:
                logger.warning("Skipping unknown task type: %s", task_type)
                continue
            
            # Try Pydantic validation with repair
            validated_task = parse_task_with_repair(t, slot_hints)
            if validated_task:
                # Convert back to MicroPlanTask (dataclass) for compatibility
                tasks.append(MicroPlanTask(
                    type=validated_task.type,
                    priority=validated_task.priority,
                    query_hint=validated_task.query_hint,
                    openai_mode=validated_task.openai_mode,
                    person_name=validated_task.person_name,
                    subpage_targets=validated_task.subpage_targets,
                    highlights_query=validated_task.highlights_query,
                    start_date=validated_task.start_date,
                    end_date=validated_task.end_date,
                ))
            else:
                logger.warning(
                    "Task failed validation and repair, dropping: %s",
                    task_type,
                )
        
        return MicroPlanDSL(
            gap=data.get("gap", ""),
            intent=data.get("intent", "general"),
            tasks=tasks,
            slot_hints=slot_hints,
        )
    except json.JSONDecodeError as e:
        logger.error("Failed to parse LLM response as JSON: %s", e)
        return None
    except Exception as e:
        logger.error("Error parsing LLM response: %s", e)
        return None


def _translate_task_to_plan_step(
    task: MicroPlanTask,
    target_input: dict,
    slot_hints: dict,
    step_index: int,
) -> Optional[PlanStep]:
    """Translate a DSL task to a PlanStep for ConnectorRunner."""
    company_name = target_input.get("company_name", "")
    website = target_input.get("website", "")
    domain = _extract_domain(website)
    context = target_input.get("context", "")
    
    # Build query based on task type and hints
    query_hint = task.query_hint or ""
    subject = company_name or domain or "target company"
    
    # Get must-include terms from slot_hints for query enrichment
    must_include_terms: List[str] = slot_hints.get("must_include_terms", [])
    # Build a short string from must-include terms (limit to 3 for query length)
    must_include_str = " ".join(must_include_terms[:3]) if must_include_terms else ""
    
    # Time windows
    today = datetime.utcnow().date()
    news_start = (today - timedelta(days=365)).isoformat()  # 1 year
    funding_start = (today - timedelta(days=365 * 5)).isoformat()  # 5 years
    
    connector = TASK_TO_CONNECTOR.get(task.type)
    if not connector:
        return None
    
    params: Dict[str, Any] = {}
    step_name = f"micro_{task.type}_{step_index}"
    
    # Default subpage targets for deep site crawling (expanded to match full Exa capability)
    default_site_subpage_targets = [
        "about", "company", "team", "leadership", "customers", "partners",
        "case-studies", "success-stories", "solutions", "products", "news", "press",
        "portfolio", "investments", "technology", "api", "docs", "developers",
    ]
    
    if task.type == "exa_news_search":
        # Build query with must-include terms if available
        base_query = f"{subject} {query_hint}".strip() if query_hint else f"{subject} news announcement"
        query = f"{base_query} {must_include_str}".strip() if must_include_str else base_query
        # Use task-specified dates or default to 1 year
        start_date = task.start_date or news_start
        end_date = task.end_date  # None means no end limit
        # Build highlights_query with must-include terms for better relevance
        highlights = task.highlights_query or must_include_str or query_hint or f"{subject} customer partner announcement deal contract"
        params = {
            "mode": "search",
            "queries": [query],
            "category": "news",
            "start_published_date": start_date,
            "num_results": 10,
            "highlights_query": highlights,
            "exclude_domains": EXCLUDE_AGGREGATOR_DOMAINS,  # Exclude aggregators for primary sources
        }
        if end_date:
            params["end_published_date"] = end_date
    
    elif task.type == "exa_site_search":
        # Deep site crawl with subpages - same approach as main planner
        query = f"{subject} {query_hint}".strip() if query_hint else f"{subject} about team company"
        # Use task-specified targets or defaults
        subpage_targets = task.subpage_targets or default_site_subpage_targets
        params = {
            "mode": "search",
            "queries": [query],
            "category": "company",
            "num_results": 10,
            "subpages": 3,  # Crawl up to 3 subpages per result
            "subpage_targets": subpage_targets,
            "highlights_query": task.highlights_query or query_hint or f"{subject} customers partners case study deployment",
        }
        if domain:
            params["include_domains"] = [domain]
    
    elif task.type == "exa_funding_search":
        round_hint = slot_hints.get("round", "")
        query = f"{subject} funding {round_hint} investors raised".strip()
        # Use task-specified dates or default to 5 years
        start_date = task.start_date or funding_start
        end_date = task.end_date
        params = {
            "mode": "search",
            "queries": [query],
            "category": "news",
            "start_published_date": start_date,
            "num_results": 12,
            "highlights_query": task.highlights_query or "funding round investors lead investor amount raised valuation post-money",
            "exclude_domains": EXCLUDE_AGGREGATOR_DOMAINS,  # Exclude aggregators for primary sources
        }
        if end_date:
            params["end_published_date"] = end_date
    
    elif task.type == "exa_patent_search":
        # For patents, must-include terms are especially important (e.g., patent titles)
        base_query = f"{subject} patent filing IP intellectual property {query_hint}".strip()
        query = f"{base_query} {must_include_str}".strip() if must_include_str else base_query
        # Use must-include terms in highlights for better matching
        highlights = task.highlights_query or must_include_str or "patent number US EP WO filing date inventor assignee claims granted"
        params = {
            "mode": "search",
            "queries": [query],
            "category": "company",
            "num_results": 10,
            "highlights_query": highlights,
        }
    
    elif task.type == "exa_general_search":
        base_query = f"{subject} {query_hint}".strip() if query_hint else subject
        query = f"{base_query} {must_include_str}".strip() if must_include_str else base_query
        highlights = task.highlights_query or must_include_str or query_hint or subject
        params = {
            "mode": "search",
            "queries": [query],
            "num_results": 10,
            "highlights_query": highlights,
            # Exclude aggregators to get primary sources
            "exclude_domains": EXCLUDE_AGGREGATOR_DOMAINS,
        }
    
    elif task.type == "exa_similar_search":
        # /findSimilar mode for competitor discovery
        # Requires the company's website URL
        if not domain:
            return None
        params = {
            "mode": "similar",
            "url": f"https://{domain}",
            "num_results": 10,
            "exclude_domains": EXCLUDE_AGGREGATOR_DOMAINS + ([domain] if domain else []),
            "highlights_query": task.highlights_query or query_hint or "product offering business model customers competitors positioning",
        }
    
    elif task.type == "exa_research_paper":
        # Search for academic/technical papers
        query = f"{subject} {query_hint}".strip() if query_hint else f"{subject} research paper study"
        params = {
            "mode": "search",
            "queries": [query],
            "category": "research paper",
            "num_results": 8,
            "highlights_query": task.highlights_query or query_hint or "methodology results findings conclusions data",
        }
    
    elif task.type == "exa_historical_search":
        # Time-bounded historical search
        # Use task dates first, then slot_hints years, then default
        if task.start_date or task.end_date:
            start_date = task.start_date or (today - timedelta(days=365 * 5)).isoformat()
            end_date = task.end_date or (today - timedelta(days=365)).isoformat()
        else:
            # Extract year hints from slot_hints
            years = slot_hints.get("years", [])
            if years:
                start_year = min(years)
                end_year = max(years)
                start_date = f"{start_year}-01-01"
                end_date = f"{end_year}-12-31"
            else:
                # Default: 2-5 years ago
                start_date = (today - timedelta(days=365 * 5)).isoformat()
                end_date = (today - timedelta(days=365)).isoformat()
        
        query = f"{subject} {query_hint}".strip() if query_hint else subject
        params = {
            "mode": "search",
            "queries": [query],
            "start_published_date": start_date,
            "end_published_date": end_date,
            "num_results": 10,
            "highlights_query": task.highlights_query or query_hint or subject,
            "exclude_domains": EXCLUDE_AGGREGATOR_DOMAINS,
        }
    
    elif task.type == "openai_web_search":
        # Use explicit mode from task, fall back to slot_hints, then default
        openai_mode = task.openai_mode or slot_hints.get("openai_mode") or "competitors"
        
        params = {
            "mode": openai_mode,
            "company_name": company_name,
            "website": website,
            "context": f"{context} {query_hint}".strip() if query_hint else context,
        }
        
        # Pass person_name and company for person mode
        if openai_mode == "person":
            params["person_name"] = task.person_name or slot_hints.get("person_name") or ""
            params["company"] = company_name  # OpenAI person mode uses "company" for context
    
    elif task.type == "pdl_person_search" or task.type == "pdl_person_enrich":
        # pdl_person_enrich: Person enrichment (requires person_name)
        # pdl_person_search: Legacy, kept for compatibility
        person_name = task.person_name or slot_hints.get("person_name") or ""
        if not person_name:
            # Cannot do person enrichment without a name, skip this step
            return None
        params = {
            "full_name": person_name,
            "company_name": company_name,  # PDL connector expects "company_name", not "company"
            "company_domain": domain,
        }
        # Pass additional hints if available
        if slot_hints.get("linkedin_url"):
            params["linkedin_url"] = slot_hints["linkedin_url"]
        if slot_hints.get("location"):
            params["location"] = slot_hints["location"]
    
    elif task.type == "pdl_company_leadership":
        # Company leadership search - no person_name needed
        # PDL will return company executives when full_name is empty
        params = {
            "full_name": "",  # Empty triggers leadership search mode
            "company_name": company_name,
            "company_domain": domain,
        }
    
    elif task.type == "pdl_company_search":
        params = {
            "company_name": company_name,
            "website": domain,
        }
    
    elif task.type == "gleif_lei_lookup":
        params = {
            "company_name": company_name,
        }
        # Add domain for better matching
        if domain:
            params["company_domain"] = domain
        # Optional: country_code from slot_hints
        if slot_hints.get("country_code"):
            params["country_code"] = slot_hints["country_code"]
    
    else:
        return None
    
    return {
        "name": step_name,
        "connector": connector,
        "params": params,
    }


def _generate_plan_markdown(tasks: List[MicroPlanTask], gap_statement: str) -> str:
    """Generate a human-readable markdown summary of the plan."""
    if not tasks:
        return "No additional research tasks proposed."
    
    task_descriptions = {
        "exa_news_search": "Search recent news and press releases",
        "exa_site_search": "Deep crawl the company's website (with subpages)",
        "exa_funding_search": "Search for funding announcements and investors",
        "exa_patent_search": "Search patent databases and IP filings",
        "exa_general_search": "Search primary web sources (excluding aggregators)",
        "exa_similar_search": "Find similar/competitor companies",
        "exa_research_paper": "Search academic and technical papers",
        "exa_historical_search": "Search historical records (time-bounded)",
        "openai_web_search": "AI-powered web research with reasoning",
        "pdl_person_search": "Look up person background and work history",
        "pdl_person_enrich": "Enrich person profile with LinkedIn data",
        "pdl_company_leadership": "Discover company leadership and executives",
        "pdl_company_search": "Look up company firmographics and stats",
        "gleif_lei_lookup": "Look up Legal Entity Identifier (LEI) from GLEIF registry",
    }
    
    lines = [f"**Gap:** {gap_statement}", "", "**Proposed research:**"]
    
    for i, task in enumerate(tasks, 1):
        desc = task_descriptions.get(task.type, task.type)
        priority_badge = f"[{task.priority}]" if task.priority != "medium" else ""
        hint = f" - _{task.query_hint}_" if task.query_hint else ""
        lines.append(f"{i}. {desc}{hint} {priority_badge}")
    
    return "\n".join(lines)


def _create_fallback_plan(
    question: str,
    gap_result: GapDetectionResult,
    target_input: dict,
    slot_hints: Optional[Dict[str, Any]] = None,
) -> MicroPlan:
    """Create a simple fallback plan based on detected intent."""
    intent = gap_result.intent or "general"
    
    # Map intents to sensible default tasks
    intent_to_tasks: Dict[str, List[MicroPlanTask]] = {
        "funding_investors": [
            MicroPlanTask(type="exa_funding_search", priority="high"),
            MicroPlanTask(type="pdl_company_search", priority="medium"),
            MicroPlanTask(type="exa_news_search", priority="low", query_hint="funding round investors lead"),
        ],
        # NEW: research_papers intent - use exa_research_paper, NOT exa_patent_search
        "research_papers": [
            MicroPlanTask(type="exa_research_paper", priority="high", query_hint="paper publication DOI journal"),
            MicroPlanTask(type="exa_general_search", priority="medium", query_hint="research paper academic publication"),
        ],
        "patents": [
            MicroPlanTask(type="exa_patent_search", priority="high"),
            MicroPlanTask(type="exa_research_paper", priority="medium", query_hint="patent technology innovation"),
        ],
        "founder_background": [
            # First: discover founders via OpenAI leadership mode (not person mode)
            # Person mode requires person_name which we don't have yet
            MicroPlanTask(
                type="openai_web_search",
                openai_mode="leadership",  # Use leadership mode to discover founders
                priority="high",
                query_hint="founders executives biography career history"
            ),
            # Second: site search for team pages
            MicroPlanTask(type="exa_site_search", priority="medium", query_hint="team founders leadership bio"),
        ],
        "competitors": [
            # OpenAI doesn't require domain, use first when domain may be unknown
            MicroPlanTask(
                type="openai_web_search",
                openai_mode="competitors",
                priority="high",
                query_hint="competitors alternatives market"
            ),
            # exa_similar_search only works if domain is known - handled in translator
            MicroPlanTask(type="exa_similar_search", priority="medium"),
        ],
        "technology": [
            MicroPlanTask(
                type="exa_site_search",
                priority="high",
                query_hint="technology platform architecture API",
                subpage_targets=["technology", "api", "docs", "developers", "platform", "solutions"],
            ),
            MicroPlanTask(type="exa_research_paper", priority="medium"),
        ],
        "regulatory": [
            MicroPlanTask(type="exa_news_search", priority="high", query_hint="regulatory compliance approval FDA SEC"),
            MicroPlanTask(type="exa_general_search", priority="medium", query_hint="filing certification license"),
        ],
        "revenue_arr": [
            MicroPlanTask(type="exa_news_search", priority="high", query_hint="revenue growth ARR financials earnings"),
            MicroPlanTask(type="pdl_company_search", priority="medium"),
        ],
        "litigation": [
            MicroPlanTask(type="exa_news_search", priority="high", query_hint="lawsuit litigation legal dispute court"),
            MicroPlanTask(type="exa_general_search", priority="medium", query_hint="settlement judgment ruling"),
        ],
        "acquisitions": [
            MicroPlanTask(type="exa_news_search", priority="high", query_hint="acquisition merger M&A deal buy"),
            MicroPlanTask(type="exa_historical_search", priority="medium", query_hint="acquired merged"),
        ],
        "legal_entity": [
            MicroPlanTask(type="gleif_lei_lookup", priority="high"),
            MicroPlanTask(
                type="openai_web_search",
                openai_mode="founding",
                priority="medium",
                query_hint="legal entity registration SEC incorporation"
            ),
        ],
        # NEW: programs_contracts intent - for government programs, grants, consortiums
        "programs_contracts": [
            MicroPlanTask(
                type="exa_general_search",
                priority="high",
                query_hint="program project initiative consortium grant award",
            ),
            MicroPlanTask(
                type="exa_news_search",
                priority="medium",
                query_hint="government contract award announcement grant program",
            ),
        ],
        "customers": [
            MicroPlanTask(
                type="exa_site_search",
                priority="high",
                query_hint="customers clients commercial partners case study",
                subpage_targets=["customers", "case-studies", "success-stories", "partners", "news", "press"],
                highlights_query="customer client case study partner deployment contract procurement pilot",
            ),
            MicroPlanTask(
                type="exa_news_search",
                priority="high",
                query_hint="commercial customer client partner collaboration deployment contract pilot",
                start_date=(datetime.utcnow().date() - timedelta(days=365 * 5)).isoformat(),
                highlights_query="customer client partner agreement strategic announces deployment contract",
            ),
            MicroPlanTask(
                type="exa_general_search",
                priority="low",
                query_hint="commercial customers clients partners case study deployment",
            ),
        ],
    }
    
    effective_slots = slot_hints or {}
    
    tasks = intent_to_tasks.get(intent, [
        MicroPlanTask(
            type="exa_general_search",
            priority="medium",
            query_hint=effective_slots.get("query_hint") or "",
        ),
    ])
    
    # Translate to plan steps
    plan_steps: List[PlanStep] = []
    for i, task in enumerate(tasks[:MAX_MICRO_STEPS]):
        step = _translate_task_to_plan_step(task, target_input, effective_slots, i)
        if step:
            plan_steps.append(step)
    
    plan_markdown = _generate_plan_markdown(tasks, gap_result.gap_statement)
    
    # Apply quality gate repair
    plan_steps = _repair_low_quality_plan(
        plan_steps,
        question_hint=effective_slots.get("query_hint", ""),
        target_input=target_input,
        intent=intent,
    )
    
    return MicroPlan(
        gap_statement=gap_result.gap_statement,
        intent=intent,
        plan_steps=plan_steps,
        plan_markdown=plan_markdown,
        estimated_queries=len([t for t in tasks if t.type.startswith("exa_")]),
    )


def _repair_low_quality_plan(
    plan_steps: List[PlanStep],
    question_hint: str,
    target_input: dict,
    intent: Optional[str],
) -> List[PlanStep]:
    """
    Ensures plan steps are question-aligned and not trivially generic.
    Repairs Exa queries that only contain the company name.
    """
    if not plan_steps or not question_hint:
        return plan_steps
    
    company_name = (target_input.get("company_name") or "").strip().lower()
    domain = _extract_domain(target_input.get("website"))
    
    # Patterns that indicate a "company-only" query
    company_only_patterns: set[str] = set()
    if company_name:
        company_only_patterns.add(company_name)
    if domain:
        company_only_patterns.add(domain.lower())
    company_only_patterns.discard("")
    
    repaired: List[PlanStep] = []
    for step in plan_steps:
        step = dict(step)  # Make mutable copy
        params = dict(step.get("params", {}))
        
        # Only repair Exa steps
        if step.get("connector") != "exa":
            repaired.append(step)
            continue
        
        # Check if queries are company-only
        queries = params.get("queries", [])
        needs_repair = False
        
        for q in queries:
            q_lower = (q or "").strip().lower()
            # Query is company-only if it equals company name or domain
            if q_lower in company_only_patterns:
                needs_repair = True
                break
        
        if needs_repair and question_hint:
            # Expand queries with question hint
            expanded_queries = [
                f"{q} {question_hint}".strip() for q in queries
            ] if queries else [f"{company_name} {question_hint}".strip()]
            params["queries"] = expanded_queries
            
            # Also set highlights_query if empty
            if not params.get("highlights_query"):
                params["highlights_query"] = question_hint
        
        step["params"] = params
        repaired.append(step)
    
    # For customers intent, ensure we have site + news coverage
    if intent == "customers":
        has_site = any(
            s.get("connector") == "exa" and 
            (s.get("params", {}).get("include_domains") or "site" in s.get("name", ""))
            for s in repaired
        )
        has_news = any(
            s.get("params", {}).get("category") == "news"
            for s in repaired
        )
        
        exa_count = sum(1 for s in repaired if s.get("connector") == "exa")
        
        # Add missing coverage if within caps
        if not has_site and exa_count < MAX_MICRO_EXA_QUERIES and len(repaired) < MAX_MICRO_STEPS:
            repaired.append({
                "name": f"micro_exa_site_search_{len(repaired)}",
                "connector": "exa",
                "params": {
                    "mode": "search",
                    "queries": [f"{company_name} {question_hint}".strip()],
                    "category": "company",
                    "subpage_targets": ["customers", "case-studies", "partners"],
                    "highlights_query": question_hint,
                },
            })
            exa_count += 1
        
        if not has_news and exa_count < MAX_MICRO_EXA_QUERIES and len(repaired) < MAX_MICRO_STEPS:
            repaired.append({
                "name": f"micro_exa_news_search_{len(repaired)}",
                "connector": "exa",
                "params": {
                    "mode": "search",
                    "queries": [f"{company_name} {question_hint}".strip()],
                    "category": "news",
                    "start_published_date": (datetime.utcnow().date() - timedelta(days=365 * 5)).isoformat(),
                    "highlights_query": question_hint,
                },
            })
    
    return repaired


def propose_micro_plan(
    question: str,
    gap_result: GapDetectionResult,
    target_input: dict,
    existing_sources: List[Source],
    existing_providers: Optional[Set[str]] = None,
    existing_domains: Optional[Set[str]] = None,
) -> MicroPlan:
    """
    Generate a micro-research plan using LLM with fallback to heuristics.
    
    The LLM outputs a restricted DSL which is then translated to PlanStep format.
    
    Args:
        question: The user's question
        gap_result: Result from gap detection
        target_input: Job's target_input (company_name, website, etc.)
        existing_sources: Already collected sources
        existing_providers: Set of provider names already queried
        existing_domains: Set of domains already crawled
        
    Returns:
        MicroPlan with plan_steps ready for ConnectorRunner
    """
    # Normalize missing_slots from gap detection
    # - Align key names: jurisdiction -> country_code, round_type -> round
    missing_slots = dict(gap_result.missing_slots) if gap_result.missing_slots else {}
    
    # Normalize jurisdiction to ISO country code
    if "jurisdiction" in missing_slots:
        country_code = _normalize_jurisdiction_to_country_code(missing_slots.pop("jurisdiction"))
        if country_code:
            missing_slots["country_code"] = country_code
    
    # Ensure round key (qa_gap.py now uses "round" but handle legacy "round_type")
    if "round_type" in missing_slots and "round" not in missing_slots:
        missing_slots["round"] = missing_slots.pop("round_type")
    
    # Derive query hint from question for fallback/repair
    company_name = target_input.get("company_name", "")
    derived_hint = _derive_query_hint(question, company_name)
    if derived_hint:
        missing_slots["query_hint"] = derived_hint
    
    try:
        # Build and send prompt to LLM
        prompt = _build_planner_prompt(
            question=question,
            gap_result=gap_result,
            target_input=target_input,
            existing_source_count=len(existing_sources),
            existing_providers=existing_providers,
            existing_domains=existing_domains,
            missing_slots=missing_slots,
        )
        
        client = get_llm_client()
        
        with limit_llm_concurrency():
            response = client.chat.completions.create(
                model=settings.LLM_MODEL,
                messages=[
                    {"role": "system", "content": "You are a research planning assistant that outputs JSON."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=500,
            )
        
        response_text = response.choices[0].message.content or ""
        
        # Parse the DSL with missing_slots for repair context
        dsl = _parse_llm_response(response_text, missing_slots)
        
        if not dsl or not dsl.tasks:
            logger.warning("LLM did not produce valid plan, using fallback")
            return _create_fallback_plan(question, gap_result, target_input, missing_slots)
        
        # dsl.slot_hints already includes merged missing_slots from _parse_llm_response
        
        # Translate DSL to PlanSteps
        plan_steps: List[PlanStep] = []
        exa_query_count = 0
        
        for i, task in enumerate(dsl.tasks):
            # Enforce limits
            if len(plan_steps) >= MAX_MICRO_STEPS:
                break
            if task.type.startswith("exa_") and exa_query_count >= MAX_MICRO_EXA_QUERIES:
                continue
            
            step = _translate_task_to_plan_step(task, target_input, dsl.slot_hints, i)
            if step:
                plan_steps.append(step)
                if task.type.startswith("exa_"):
                    exa_query_count += 1
        
        if not plan_steps:
            logger.warning("No valid plan steps generated, using fallback")
            return _create_fallback_plan(question, gap_result, target_input, missing_slots)
        
        # Apply quality gate repair
        plan_steps = _repair_low_quality_plan(
            plan_steps,
            question_hint=missing_slots.get("query_hint", ""),
            target_input=target_input,
            intent=dsl.intent or gap_result.intent,
        )
        
        plan_markdown = _generate_plan_markdown(dsl.tasks[:len(plan_steps)], dsl.gap or gap_result.gap_statement)
        
        logger.info(
            "Micro-plan generated: %d steps, %d exa queries",
            len(plan_steps),
            exa_query_count,
            extra={"steps": len(plan_steps), "exa_queries": exa_query_count},
        )
        
        return MicroPlan(
            gap_statement=dsl.gap or gap_result.gap_statement,
            intent=dsl.intent or gap_result.intent,
            plan_steps=plan_steps,
            plan_markdown=plan_markdown,
            estimated_queries=exa_query_count,
        )
        
    except Exception as e:
        logger.exception("Error generating micro-plan: %s", e)
        return _create_fallback_plan(question, gap_result, target_input, missing_slots)

