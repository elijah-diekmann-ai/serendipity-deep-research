# backend/app/services/planner.py

from __future__ import annotations

from typing import TypedDict, List, Dict, Any, Optional
from urllib.parse import urlparse
from datetime import datetime, timedelta

import logging

from ..core.config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

# Allowed connectors for the deterministic planner
ALLOWED_CONNECTORS = {"exa", "companies_house", "apollo", "openai_web", "pdl", "open_corporates", "gleif", "pitchbook"}

MAX_PLANNER_STEPS = 10
# Global cap on total Exa query strings across all Exa steps
MAX_EXA_QUERIES = 8


class PlanStep(TypedDict):
    name: str
    connector: str
    params: Dict[str, Any]


# Subpage targets we want Exa to prioritise when crawling the company site
SITE_SUBPAGE_TARGETS: List[str] = [
    "about",
    "company",
    "team",
    "leadership",
    "management",
    "people",
    "founders",
    "board",
    "partners",
    "portfolio",
    "investments",
    "companies",
    "product",
    "products",
    "solutions",
    "platform",
    "technology",
    "engineering",
    "tech",
    "docs",
    "documentation",
    "developers",
    "api",
    "blog",
    "news",
    "press",
    "careers",
]


def _extract_domain(website: Optional[str]) -> Optional[str]:
    if not website:
        return None
    try:
        if "://" not in website:
            website = "https://" + website
        parsed = urlparse(website)
        return parsed.netloc or None
    except Exception:
        return website.split("://")[-1].split("/")[0]


def _default_plan(target_input: dict) -> List[PlanStep]:
    """
    Deterministic hybrid plan aligned to the high-density brief structure.

    High-level strategy under the MAX_EXA_QUERIES budget:

    - Use Exa where it is strongest: high-recall similarity + date-filtered web
      search for factual evidence:
        * Deep crawl of the company's own site (overview, founding, HQ,
          identifiers, about, team, product, technology) using Exa /search with
          type="deep" + subpages.
        * Deep facts/evidence (patents, regulatory filings, technical benchmarks,
          clinical/industrial capacity).
        * Recent news in the last ~12–24 months.

    - Use OpenAI web search where broad world knowledge and reasoning are needed:
        * Competitor discovery and qualification for the Competitors section is
          routed to the `openai_web` connector, which uses OpenAI's web_search
          tool + reasoning to propose a curated competitor set.

    - Maintain optional registry / enrichment connectors (Companies House,
      Apollo) for structured identifiers and leadership data.

    NOTE: We explicitly do NOT use Exa for competitor discovery anymore.
    """
    company_name = (target_input.get("company_name") or "").strip()
    website = target_input.get("website") or ""
    context = (target_input.get("context") or "").strip()
    domain = _extract_domain(website)

    subject_parts = [p for p in [company_name, domain] if p]
    subject = " ".join(dict.fromkeys(subject_parts)) or company_name or domain or "target company"

    # Shortened context hint to bias Exa without blowing up queries
    context_hint = ""
    if context:
        context_hint = " " + " ".join(context.split()[:20])

    # Time windows for Exa publishedDate filters
    today = datetime.utcnow().date()
    # Funding history: look back ~10 years
    funding_start_date = (today - timedelta(days=365 * 10)).isoformat()
    # "Recent news": roughly last 18 months
    recent_news_start_date = (today - timedelta(days=540)).isoformat()

    # -------------------------------------------------------------------------
    # Exa queries by section
    # -------------------------------------------------------------------------

    # Company-site focused queries (Founding Details, Founders & Leadership,
    # Product, Technology, identifiers) – constrained to the company domain.
    site_queries: List[str] = [
        # Founding, HQ, legal entity & identifiers
        (
            f"{subject} company overview legal entity name incorporation date "
            f"registration number ABN ACN EIN VAT company number headquarters "
            f"jurisdiction spin-out origin founding story corporate history{context_hint}"
        ),
        # Founders & leadership
        (
            f"{subject} founders leadership team executives board of directors "
            f"biographies backgrounds prior companies track record{context_hint}"
        ),
        # Product & technology
        (
            f"{subject} products services solutions platform technology architecture "
            f"technical specifications performance benchmarks pricing model target "
            f"customers industries use cases integrations roadmap{context_hint}"
        ),
    ]

    # Fundraising (history of rounds, investors, valuations, plus non-dilutive)
    funding_queries: List[str] = [
        (
            f"{subject} funding history funding rounds seed series A series B "
            f"venture capital equity financing grants government programs non-dilutive "
            f"capital revenue ARR MRR headcount valuation{context_hint}"
        ),
    ]

    # Deep evidence: patents, regulatory filings, technical benchmarks, capacity
    deep_evidence_queries: List[str] = [
        (
            f"{subject} patent filings patents EP US WO PCT regulatory filings "
            f"SEC filing 10-K S-1 prospectus clinical trial phase manufacturing "
            f"capacity throughput technical benchmark performance paper standard "
            f"specification{context_hint}"
        ),
    ]

    # Recent news (product launches, partnerships, layoffs, regulatory, exits)
    news_queries: List[str] = [
        (
            f"{subject} recent news announcements product launches partnerships "
            f"major customers strategic deals layoffs acquisitions IPO regulatory "
            f"actions investigations{context_hint}"
        ),
        (
            f"{subject} press release funding round grant contract government program "
            f"clinical trial milestone manufacturing plant opening capacity expansion{context_hint}"
        ),
    ]

    # Enforce global query cap while prioritising core coverage:
    # 1) company site  2) funding  3) deep evidence  4) news
    total_queries = site_queries + funding_queries + deep_evidence_queries + news_queries

    if len(total_queries) > MAX_EXA_QUERIES:
        remaining = MAX_EXA_QUERIES

        # Site queries are highest priority
        if len(site_queries) > remaining:
            site_queries = site_queries[:remaining]
            funding_queries = []
            deep_evidence_queries = []
            news_queries = []
        else:
            remaining -= len(site_queries)

            # Funding next
            if len(funding_queries) > remaining:
                funding_queries = funding_queries[:remaining]
                deep_evidence_queries = []
                news_queries = []
            else:
                remaining -= len(funding_queries)

                # Deep evidence next
                if len(deep_evidence_queries) > remaining:
                    deep_evidence_queries = deep_evidence_queries[:remaining]
                    news_queries = []
                else:
                    remaining -= len(deep_evidence_queries)

                    # Remaining budget goes to news queries
                    if len(news_queries) > remaining:
                        news_queries = news_queries[:remaining]

    steps: List[PlanStep] = []

    # --- Step 1: Deep crawl of the company website (Exa) ---
    if site_queries:
        exa_params_site: Dict[str, Any] = {
            "mode": "search",
            "queries": site_queries,
            "category": "company",
            # Encourage Exa to pull key subpages for founding/team/product/tech
            "subpages": 3,
            "subpage_targets": SITE_SUBPAGE_TARGETS,
            "highlights_query": (
                "Legal entity name, incorporation/registration date, jurisdiction, "
                "headquarters address, registration numbers and identifiers "
                "(ABN, ACN, EIN, VAT, company number, stock ticker), founding story "
                "or spin-out origin, leadership team and board, products and "
                "services, target customers, pricing model, and high-level "
                "description of the technology stack or platform."
            ),
        }
        if domain:
            exa_params_site["include_domains"] = [domain]

        steps.append(
            {
                "name": "search_exa_site",
                "connector": "exa",
                "params": exa_params_site,
            }
        )

    # --- Step 2: Funding / fundraising history (Exa) ---
    if funding_queries:
        exa_params_funding: Dict[str, Any] = {
            "mode": "search",
            "queries": funding_queries,
            "category": "news",
            "start_published_date": funding_start_date,
            "highlights_query": (
                "Funding rounds (seed, Series A/B/C, IPO), dates, amounts raised, "
                "lead and notable investors, valuation signals, grants and "
                "non-dilutive funding, and any disclosed revenue/ARR, growth, "
                "headcount, or profitability metrics."
            ),
        }
        steps.append(
            {
                "name": "search_exa_fundraising",
                "connector": "exa",
                "params": exa_params_funding,
            }
        )

    # --- Step 3: Deep evidence (patents, regulatory filings, benchmarks) (Exa) ---
    if deep_evidence_queries:
        exa_params_deep: Dict[str, Any] = {
            "mode": "search",
            "queries": deep_evidence_queries,
            "category": "company",
            "highlights_query": (
                "Patent identifiers (EP, US, WO, PCT codes), regulatory filings "
                "(SEC, 10-K, 20-F, S-1, clinical trial IDs), technical "
                "specifications, architectures, benchmarks, capacity or throughput "
                "figures, clinical trial phases, and other hard technical or "
                "regulatory evidence."
            ),
        }
        steps.append(
            {
                "name": "search_exa_deep_evidence",
                "connector": "exa",
                "params": exa_params_deep,
            }
        )

    # --- Step 4: Recent news (last ~18 months) (Exa) ---
    if news_queries:
        exa_params_news: Dict[str, Any] = {
            "mode": "search",
            "queries": news_queries,
            "category": "news",
            "start_published_date": recent_news_start_date,
            "highlights_query": (
                "Recent news in roughly the last 12–24 months including product "
                "launches, partnerships, major customer wins, regulatory events, "
                "funding announcements, grants or contracts, layoffs, and M&A."
            ),
        }
        steps.append(
            {
                "name": "search_exa_news",
                "connector": "exa",
                "params": exa_params_news,
            }
        )

    # --- Step 5: Reasoning-first competitor discovery (OpenAI web_search) ---
    if company_name or website:
        steps.append(
            {
                "name": "openai_competitors",
                "connector": "openai_web",
                "params": {
                    "mode": "competitors",
                    "company_name": company_name,
                    "website": website,
                    "context": context,
                },
            }
        )

    # -------------------------------------------------------------------------
    # Supplemental Connectors
    # -------------------------------------------------------------------------

    if getattr(settings, "COMPANIES_HOUSE_API_KEY", None) and company_name:
        steps.append(
            {
                "name": "companies_house_lookup",
                "connector": "companies_house",
                "params": {"query": company_name},
            }
        )

    # OpenCorporates registry lookup – used primarily for Founding Details section
    if getattr(settings, "OPENCORPORATES_API_TOKEN", None) and company_name:
        oc_params: Dict[str, Any] = {"company_name": company_name}

        # Optional: pass through hints if target_input is extended later
        jurisdiction_hint = target_input.get("jurisdiction_code")
        country_hint = target_input.get("country_code")
        if jurisdiction_hint:
            oc_params["jurisdiction_code"] = jurisdiction_hint
        if country_hint:
            oc_params["country_code"] = country_hint

        steps.append(
            {
                "name": "open_corporates_lookup",
                "connector": "open_corporates",
                "params": oc_params,
            }
        )

    # GLEIF LEI / legal-entity registry lookup – used for Founding Details
    # Only requires company_name (and optionally country/jurisdiction hints).
    gleif_enabled = getattr(settings, "GLEIF_ENABLED", True)

    if gleif_enabled and company_name:
        gleif_params: Dict[str, Any] = {"company_name": company_name}

        # Optionally propagate hints from target_input
        country_hint = target_input.get("country_code")
        if country_hint:
            gleif_params["country_code"] = country_hint

        # Optional future fields in target_input:
        # - "lei": explicit LEI if user has it
        # - "bic": BIC if user has it
        if "lei" in target_input:
            gleif_params["lei"] = target_input["lei"]
        if "bic" in target_input:
            gleif_params["bic"] = target_input["bic"]

        steps.append(
            {
                "name": "gleif_lookup",
                "connector": "gleif",
                "params": gleif_params,
            }
        )

    # -------------------------------------------------------------------------
    # PitchBook fundraising data (optional, if configured)
    # -------------------------------------------------------------------------

    pitchbook_key = getattr(settings, "PITCHBOOK_API_KEY", None)

    if pitchbook_key and (company_name or domain):
        steps.append(
            {
                "name": "pitchbook_fundraising",
                "connector": "pitchbook",
                "params": {
                    "company_name": company_name,
                    "company_domain": domain,
                },
            }
        )

    # -------------------------------------------------------------------------
    # People Discovery: Apollo and/or PDL
    # -------------------------------------------------------------------------
    # We prioritize PDL if configured (works on free tier + strong LinkedIn data).
    # If PDL is missing, we fall back to Apollo if available.

    people_params: Dict[str, Any] = {}
    if domain:
        people_params["company_domain"] = domain
    if company_name:
        people_params["company_name"] = company_name

    # PDL: works on free tier (100 matches/month), has LinkedIn-derived data
    pdl_key = getattr(settings, "PDL_API_KEY", None)
    apollo_key = getattr(settings, "APOLLO_API_KEY", None)

    if pdl_key and people_params:
        steps.append(
            {
                "name": "pdl_people_discovery",
                "connector": "pdl",
                "params": people_params.copy(),
            }
        )
    # Only fall back to Apollo if PDL is NOT configured or if we want redundancy
    # (For now we treat them as either/or to save tokens/calls unless explicitly both wanted)
    elif apollo_key and people_params:
        steps.append(
            {
                "name": "people_enrichment",
                "connector": "apollo",
                "params": people_params.copy(),
            }
        )

    return steps[:MAX_PLANNER_STEPS]


def plan_research(target_input: dict) -> List[PlanStep]:
    """
    Entry point used by the orchestrator.

    We use a deterministic hybrid plan instead of an LLM-based planner now. This
    guarantees that we always:

    - Hit the company website (with subpages) for founding, HQ, identifiers,
      team, product, and tech (Exa).
    - Pull recent news from the open web (Exa).
    - Identify people associated with the company:
      * Apollo: org firmographics + people (requires paid plan)
      * PDL: leadership discovery with work history/education (free tier available)
    - Funding from Companies House, eventually Pitchbook, OpenCorporates (if configured)
    - Call /search for deep evidence (patents, regulatory filings, specs) (Exa).
    - Call the `openai_web` connector to obtain a reasoned competitor short-list
      (sole input to the Competitors section).
    - Enrich Apollo-discovered people with PDL for deeper biography data
    """
    try:
        plan = _default_plan(target_input)
        logger.info("Planner generated hybrid Exa + OpenAI plan", extra={"step": "plan"})
        return plan
    except Exception as e:
        logger.exception("Planner failed unexpectedly, returning empty plan: %s", e)
        return []
