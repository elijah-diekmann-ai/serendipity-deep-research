from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Literal, Tuple
from urllib.parse import urlparse
import logging
import re

from ..core.config import get_settings
from .connectors.pdl import PDLConnector

settings = get_settings()
logger = logging.getLogger(__name__)


NON_CANONICAL_DOMAINS = {
    "linkedin.com", "www.linkedin.com",
    "crunchbase.com", "www.crunchbase.com",
    "pitchbook.com", "www.pitchbook.com",
    "bloomberg.com", "www.bloomberg.com",
    "wikipedia.org", "en.wikipedia.org",
    "twitter.com", "x.com",
    "facebook.com", "instagram.com",
    "youtube.com",
    "glassdoor.com", "www.glassdoor.com",
    "ycombinator.com", "www.ycombinator.com",
}

def _normalize_name(s: str) -> str:
    """
    Simplify company name for comparison (remove legal suffixes, lowercase).
    """
    if not s:
        return ""
    s = s.lower().strip()
    # Remove common legal suffixes
    s = re.sub(r"\b(inc|llc|ltd|limited|corp|corporation|gmbh|pty|plc)\b\.?", "", s)
    # Remove special chars
    s = re.sub(r"[^a-z0-9\s]", "", s)
    return s.strip()

def _domain_core_tokens(domain: str) -> List[str]:
    """
    Extract core tokens from a domain (e.g. "apple.com" -> ["apple"]).
    """
    if not domain:
        return []
    # remove www. prefix
    d = domain.lower()
    if d.startswith("www."):
        d = d[4:]
    
    # split by dot
    parts = d.split('.')
    # Simple heuristic: assume last segment is TLD
    if len(parts) > 1:
        parts = parts[:-1]
    
    return parts

def _simple_similarity(name: str, domain: str) -> float:
    """
    Check if normalized name appears in domain tokens.
    Returns 1.0 if exact match, 0.8 if partial match, 0.0 otherwise.
    """
    norm_name = _normalize_name(name)
    if not norm_name:
        return 0.0
    
    domain_tokens = _domain_core_tokens(domain)
    name_tokens = norm_name.split()
    
    # Check if all name tokens appear in domain tokens (order independent)
    # e.g. "Acme Corp" -> "acme" in "acme.com"
    
    match_count = 0
    for nt in name_tokens:
        for dt in domain_tokens:
            if nt in dt or dt in nt:
                match_count += 1
                break
    
    if match_count == len(name_tokens):
        return 1.0
    if match_count > 0:
        return 0.5 + (0.5 * (match_count / len(name_tokens)))
        
    return 0.0


def _normalize_country(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return re.sub(r"[^a-z0-9]", "", value.lower().strip()) or None


def _guess_country_from_location_name(location_name: Optional[str]) -> Optional[str]:
    if not location_name:
        return None
    parts = [p.strip() for p in location_name.split(",") if p.strip()]
    if not parts:
        return None
    return _normalize_country(parts[-1])


def _rank_gleif_candidates(
    candidates: List[dict],
    primary_name: Optional[str],
    pdl_company: dict,
    target_country_code: Optional[str],
    domain_tokens: List[str],
) -> tuple[Optional[dict], List[dict]]:
    """
    Score GLEIF candidates using additional context (PDL, domain, country hints).
    Returns (best_candidate, ranked_candidates).
    """
    normalized_names: List[str] = []
    for name in [primary_name, pdl_company.get("name")]:
        if not name:
            continue
        norm = _normalize_name(name)
        if norm and norm not in normalized_names:
            normalized_names.append(norm)

    country_hints = set()
    if target_country_code:
        norm_country = _normalize_country(target_country_code)
        if norm_country:
            country_hints.add(norm_country)
    pdl_country = _guess_country_from_location_name(pdl_company.get("location_name"))
    if pdl_country:
        country_hints.add(pdl_country)

    domain_tokens_lower = [tok.lower() for tok in domain_tokens if tok]

    best_candidate: Optional[dict] = None
    best_score = float("-inf")
    prepared: List[dict] = []

    for cand in candidates:
        cand_copy = dict(cand)
        score = float(cand_copy.get("base_score") or 0.0)
        legal_name = (cand_copy.get("legal_name") or "").lower()
        normalized_legal_name = _normalize_name(cand_copy.get("legal_name") or "")

        # Name alignment weighting
        for idx, hint_name in enumerate(normalized_names):
            weight = max(0.5, 3.0 - idx)
            if hint_name and normalized_legal_name == hint_name:
                score += weight
            elif hint_name and hint_name in normalized_legal_name:
                score += weight * 0.4

        # Country / jurisdiction alignment
        cand_countries = set()
        for field in ("legal_address", "headquarters_address"):
            addr = cand_copy.get(field) or {}
            cand_country = _normalize_country(addr.get("country"))
            if cand_country:
                cand_countries.add(cand_country)
        juris = _normalize_country(cand_copy.get("legal_jurisdiction"))
        if juris:
            cand_countries.add(juris)

        if country_hints and cand_countries and (cand_countries & country_hints):
            score += 1.2

        # Domain token alignment (brand vs. legal name)
        for tok in domain_tokens_lower:
            if tok and tok in legal_name:
                score += 0.8
                break

        cand_copy["match_score"] = round(score, 3)
        cand_copy["is_primary"] = False
        prepared.append(cand_copy)

        if score > best_score:
            best_score = score
            best_candidate = cand_copy

    prepared.sort(key=lambda c: c.get("match_score", 0.0), reverse=True)
    if best_candidate:
        best_candidate["is_primary"] = True

    return best_candidate, prepared

@dataclass
class DomainCandidate:
    count: int = 0
    homepage_hits: int = 0          # URLs that look like root: "/", "", "/home"
    name_in_domain: float = 0.0     # max similarity of company_name vs domain core
    name_in_title: int = 0          # does title contain company_name
    name_in_snippet: int = 0        # snippet contains name


@dataclass
class PersonNode:
    """
    In-graph representation of a person (colleague/officer/associated).

    identity_source:
        - "web": extracted from generic web/filings/registries (default).
        - "apollo": discovered via Apollo.io People API.
        - "pdl": discovered via People Data Labs Person Search.
        - "manual": user-specified / override.

    enrichment_source:
        - Reserved for deeper biography providers (e.g. "pdl").
    """

    full_name: str
    roles: List[str] = field(default_factory=list)
    linkedin_url: Optional[str] = None
    photo_url: Optional[str] = None
    identity_source: Literal["web", "apollo", "pdl", "manual"] = "web"
    enrichment_source: Optional[Literal["pdl"]] = None
    apollo_person_id: Optional[str] = None
    # enrichment is provider -> provider-specific payload
    enrichment: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PersonTargetNode:
    """
    Primary person target for "person" mode research.
    """
    full_name: str
    normalized_name: str
    linkedin_url: Optional[str] = None
    primary_role: Optional[str] = None        # e.g. "CEO, Acme Robotics"
    primary_company: Optional[str] = None
    location: Optional[str] = None
    enrichment: Dict[str, Any] = field(default_factory=dict)
    web_snippets: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class CompanyNode:
    """
    In-graph representation of the target company.

    Apollo-specific fields are kept explicit so we can:
    - Use them as fallbacks when web/filing sources don't disclose scale.
    - Keep firmographics provenance clear in the Writer.
    """

    name: str
    domain: Optional[str] = None
    domain_confidence: Optional[float] = None
    domain_source: Optional[str] = None
    companies_house_number: Optional[str] = None
    apollo_organization_id: Optional[str] = None
    apollo_estimated_num_employees: Optional[int | str] = None
    apollo_founded_year: Optional[int] = None
    apollo_annual_revenue: Optional[int | str] = None
    profile: Dict[str, Any] = field(default_factory=dict)
    people: List[PersonNode] = field(default_factory=list)
    web_snippets: List[Dict[str, Any]] = field(default_factory=list)
    # Structured competitor objects produced by reasoning-first connectors
    competitors: List[Dict[str, Any]] = field(default_factory=list)
    # NEW – normalized funding rounds, primarily from PDL Company today,
    # but schema is compatible with PitchBook or other sources.
    funding_rounds: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class KnowledgeGraph:
    company: Optional[CompanyNode]
    target_type: Literal["company", "person", "unknown"] = "company"
    person: Optional[PersonTargetNode] = None


def _extract_domain_from_url(url: str) -> Optional[str]:
    try:
        if "://" not in url:
            url = "https://" + url
        parsed = urlparse(url)
        return parsed.netloc or None
    except Exception:
        return None


def _infer_domain(
    target_input: Optional[dict],
    apollo_data: dict,
    pdl_company: dict,
    snippet_candidates: List[Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Returns (domain, source, confidence)
    """
    company_name = (
        (target_input or {}).get("company_name")
        or apollo_data.get("organization", {}).get("name")
        or pdl_company.get("name")
    )

    # 1) From explicit website (highest trust)
    if target_input:
        website = target_input.get("website")
        if website:
            d = _extract_domain_from_url(website)
            if d:
                return d.lower(), "user", 0.99

    # 2) From PDL Company (highly reliable as it's an enrichment match)
    pdl_website = pdl_company.get("website")
    if pdl_website:
        d = _extract_domain_from_url(pdl_website)
        if d:
            return d.lower(), "pdl_company", 0.90

    # 3) From Apollo organization
    org = apollo_data.get("organization") or {}
    domain = (
        org.get("primary_domain")
        or org.get("organization_domain")
        or org.get("domain")
    )
    if domain:
        return str(domain).lower(), "apollo", 0.9

    # 4) Scored Exa-based inference
    if not company_name:
        # Fallback to domain-majority logic if no name is available.
        domains = [s.get("domain") for s in snippet_candidates if s.get("domain")]
        if domains:
            counts = Counter(d.lower() for d in domains)
            best = counts.most_common(1)[0][0]
            return best, "majority_guess", 0.3
        return None, None, None

    # Build candidate map
    candidates: Dict[str, DomainCandidate] = {}
    norm_company_name = _normalize_name(company_name)

    for s in snippet_candidates:
        d = s.get("domain")
        if not d:
            continue
        d = d.lower()
        if d in NON_CANONICAL_DOMAINS:
            continue
        
        # Also skip subdomains of non-canonical if possible, but exact match is good for now
        
        if d not in candidates:
            candidates[d] = DomainCandidate()
        
        cand = candidates[d]
        cand.count += 1
        
        # Homepage hit check
        url = s.get("url") or ""
        path = urlparse(url).path
        if path in ("", "/", "/home", "/index", "/en", "/en/"):
            cand.homepage_hits += 1
            
        # Name in domain
        sim = _simple_similarity(company_name, d)
        if sim > cand.name_in_domain:
            cand.name_in_domain = sim
            
        # Name in title/snippet
        title = (s.get("title") or "").lower()
        snippet = (s.get("snippet") or "").lower()
        if norm_company_name in title:
            cand.name_in_title += 1
        if norm_company_name in snippet:
            cand.name_in_snippet += 1

    if not candidates:
        return None, None, None

    # Score candidates
    scored = []
    for d, cand in candidates.items():
        score = (
            2.0 * cand.count +
            3.0 * cand.homepage_hits +
            4.0 * cand.name_in_domain +
            1.5 * cand.name_in_title +
            1.0 * cand.name_in_snippet
        )
        scored.append((score, d))
    
    scored.sort(key=lambda x: x[0], reverse=True)
    
    best_score, best_domain = scored[0]
    
    MIN_SCORE = 5.0
    if best_score < MIN_SCORE:
        return None, None, None
        
    # Ambiguity check
    normalized_confidence = 0.5
    if len(scored) > 1:
        second_score, _ = scored[1]
        if (best_score - second_score) < 3.0:
            # ambiguous
            return None, None, None
        
        # Confidence calculation
        # Cap at 0.85 for inferred domains (Apollo/User are higher)
        ratio = best_score / (best_score + second_score + 1.0)
        normalized_confidence = min(0.85, ratio)
    else:
        # Only one candidate
        normalized_confidence = min(0.85, best_score / 10.0)

    return best_domain, "exa", normalized_confidence


def _merge_person(existing: PersonNode, new: PersonNode) -> PersonNode:
    """
    Merge two PersonNode instances representing the same individual.

    Rules:
    - Union roles.
    - Prefer Apollo as the identity_source when available (for stable IDs).
    - Preserve the first non-empty linkedin_url / photo_url.
    - Merge enrichment dicts provider-wise.
    - If PDL enrichment is present, set enrichment_source="pdl".
    """
    existing.roles = sorted({*existing.roles, *new.roles})

    # Prefer Apollo as the identity layer when available.
    if new.identity_source == "apollo" and existing.identity_source != "apollo":
        existing.identity_source = "apollo"
        if new.apollo_person_id:
            existing.apollo_person_id = new.apollo_person_id

    # Propagate Apollo ID if missing.
    if not existing.apollo_person_id and new.apollo_person_id:
        existing.apollo_person_id = new.apollo_person_id

    # Fill in missing identity fields.
    if not existing.linkedin_url and new.linkedin_url:
        existing.linkedin_url = new.linkedin_url
    if not existing.photo_url and new.photo_url:
        existing.photo_url = new.photo_url

    # Enrichment provenance (e.g. PDL as "Biographer").
    if new.enrichment_source == "pdl":
        existing.enrichment_source = "pdl"

    for provider, payload in new.enrichment.items():
        if provider not in existing.enrichment:
            existing.enrichment[provider] = payload

    return existing


def _build_people_nodes(
    ch_data: dict,
    apollo_data: dict,
    pdl_data: dict,
) -> List[PersonNode]:
    """
    Robustly resolves people from multiple sources (Companies House, Apollo, PDL).

    Even in an Exa-only deployment, we keep this logic so the system can
    seamlessly upgrade when those keys are configured.
    """
    resolved_nodes: List[PersonNode] = []

    def find_match(p: PersonNode) -> PersonNode | None:
        # 0. Apollo person ID match
        if p.apollo_person_id:
            for node in resolved_nodes:
                if node.apollo_person_id and node.apollo_person_id == p.apollo_person_id:
                    return node

        # 1. Exact LinkedIn match
        if p.linkedin_url:
            for node in resolved_nodes:
                if node.linkedin_url and node.linkedin_url == p.linkedin_url:
                    return node

        # 2. Name match (case-insensitive), tolerating missing LinkedIn
        p_name_clean = p.full_name.strip().lower()

        for node in resolved_nodes:
            node_name_clean = node.full_name.strip().lower()
            if node_name_clean == p_name_clean:
                if node.linkedin_url and p.linkedin_url and node.linkedin_url != p.linkedin_url:
                    # Two different LinkedIn profiles → treat as distinct people.
                    continue
                return node

        return None

    def ingest(person: PersonNode) -> None:
        match = find_match(person)
        if match:
            _merge_person(match, person)
        else:
            resolved_nodes.append(person)

    # Companies House officers
    for officer in ch_data.get("officers", []):
        full_name = officer.get("name", "Unknown")
        role = officer.get("officer_role", "Officer")
        node = PersonNode(
            full_name=full_name,
            roles=[role] if role else [],
            linkedin_url=None,
            identity_source="web",
            enrichment={"companies_house": officer},
        )
        ingest(node)

    # Apollo people (discovery / identity)
    for p in apollo_data.get("people", []) or []:
        full_name = (
            p.get("full_name")
            or p.get("name")
            or " ".join(
                x for x in [p.get("first_name"), p.get("last_name")] if x
            )
            or "Unknown"
        )
        title = p.get("title") or p.get("job_title")
        linkedin_url = p.get("linkedin_url")
        photo_url = p.get("photo_url")
        apollo_person_id = p.get("apollo_person_id") or p.get("id")

        node = PersonNode(
            full_name=full_name,
            roles=[title] if title else [],
            linkedin_url=linkedin_url,
            photo_url=photo_url,
            identity_source="apollo",
            apollo_person_id=apollo_person_id,
            enrichment={"apollo": p},
        )
        ingest(node)

    # PDL people (discovery with rich work history / education)
    # PDL can act as both identity source AND enrichment
    # In "Strictly PDL" mode, these nodes are primary.
    for p in pdl_data.get("people", []) or []:
        full_name = p.get("full_name") or "Unknown"
        title = p.get("title")
        linkedin_url = p.get("linkedin_url")
        photo_url = p.get("photo_url")
        pdl_id = p.get("pdl_id")

        # Get full PDL data for enrichment
        pdl_enrichment = p.get("pdl_data") or p

        node = PersonNode(
            full_name=full_name,
            roles=[title] if title else [],
            linkedin_url=linkedin_url,
            photo_url=photo_url,
            # PDL-discovered people use "pdl" as identity source
            # This allows them to be treated similarly to Apollo for enrichment
            identity_source="pdl",
            enrichment_source="pdl",
            enrichment={"pdl": pdl_enrichment},
        )
        ingest(node)

    return resolved_nodes


def _enrich_with_pdl(
    people: List[PersonNode],
    company_name: str,
    company_domain: Optional[str],
) -> None:
    """
    Use People Data Labs to enrich PersonNodes in place.

    PDL acts as a "Biographer" layer – deeper work history, education, skills.
    We keep this behind a feature flag (PDL_API_KEY) so the core identity
    resolution logic remains usable without it.

    IMPORTANT:
    - For identity_source="pdl" nodes, we assume they are already enriched (via Search).
    """
    if not settings.PDL_API_KEY or not people:
        return

    people_inputs: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()

    for idx, person in enumerate(people):
        # Only enrich Apollo-derived PersonNodes
        # NOTE: We skip PDL-sourced nodes because they are already fully hydrated from discovery.
        if person.identity_source != "apollo":
            continue

        parts = [person.full_name.strip().lower()]
        if person.linkedin_url:
            parts.append(person.linkedin_url)
        key = "|".join(parts) or f"idx:{idx}"
        if key in seen_keys:
            continue
        seen_keys.add(key)

        # Use company_name primarily; fall back to company_domain as a website-style
        # hint if name is unavailable, to satisfy PDL's "company OR school OR location" requirement.
        company_hint: Optional[str] = (
            company_name
            or (company_domain and f"https://{company_domain}")
            or None
        )

        people_inputs.append(
            {
                "key": key,
                "name": person.full_name,
                "company": company_hint,
                "linkedin_url": person.linkedin_url,
            }
        )

    if not people_inputs:
        return

    connector = PDLConnector()

    async def _run() -> Dict[str, Dict[str, Any]]:
        return await connector.enrich_many(people_inputs)

    try:
        enriched_by_key = asyncio.run(_run())
    except RuntimeError as e:
        # If an event loop is already running (e.g. notebook), don't crash; just skip PDL.
        logger.warning(
            "Skipping PDL enrichment due to running event loop: %s",
            e,
        )
        return

    # Map from the deterministic "key" to indices in the people list.
    # Only include Apollo-derived people (matching the filtering above).
    key_to_indices: Dict[str, List[int]] = {}
    for idx, person in enumerate(people):
        if person.identity_source != "apollo":
            continue
        parts = [person.full_name.strip().lower()]
        if person.linkedin_url:
            parts.append(person.linkedin_url)
        key = "|".join(parts) or f"idx:{idx}"
        key_to_indices.setdefault(key, []).append(idx)

    # Attach PDL enrichment payloads (flattened 'data' objects) to PersonNodes.
    for key, payload in enriched_by_key.items():
        for idx in key_to_indices.get(key, []):
            person = people[idx]
            person.enrichment.setdefault("pdl", payload)
            person.enrichment_source = "pdl"


def resolve_entities(raw_results: dict, target_input: Optional[dict] = None) -> KnowledgeGraph:
    """
    Build a KnowledgeGraph from connector outputs.

    Behaviour:
    - Collect Exa and OpenAI "snippets" from all web-search-like steps, deduped by URL.
    - Collect structured competitor objects from any connector that returns a
      `competitors` list (currently the OpenAI web connector).
    - Ingest Companies House / Apollo into structured CompanyNode and PersonNodes.
    - Leave Apollo firmographics explicit on CompanyNode so the Writer can treat
      them as a fallback when web/filings don't disclose scale.
    """
    target_type = (target_input or {}).get("target_type") or "company"
    person_name = (target_input or {}).get("person_name") or ""
    
    web_snippets: List[Dict[str, Any]] = []
    domain_snippet_candidates: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    competitors_structured: List[Dict[str, Any]] = []
    funding_rounds_structured: List[Dict[str, Any]] = []

    for step_name, payload in (raw_results or {}).items():
        if not isinstance(payload, dict):
            continue

        # Generic snippets from Exa, OpenAI web, etc.
        snippets = payload.get("snippets")
        if snippets:
            for s in snippets:
                url = s.get("url")
                if url and url in seen_urls:
                    continue
                if url:
                    seen_urls.add(url)
                web_snippets.append(s)

                # Only treat Exa-derived steps as candidates for inferring the
                # *target* company's domain.
                if isinstance(step_name, str) and (
                    step_name.startswith("search_exa_") or step_name.startswith("exa_")
                ):
                    domain_snippet_candidates.append(s)

        # Structured competitors (if present)
        comps = payload.get("competitors")
        if isinstance(comps, list):
            for c in comps:
                if isinstance(c, dict):
                    competitors_structured.append(c)

        # Structured funding rounds from PitchBook (or other future sources)
        rounds = payload.get("funding_rounds")
        if isinstance(rounds, list):
            for r in rounds:
                if isinstance(r, dict):
                    funding_rounds_structured.append(r)
    
    # --- BRANCH: PERSON TARGET ---
    if target_type == "person":
        pdl_data = (
            raw_results.get("pdl_people_discovery")
            or raw_results.get("pdl_person_search")
            or {}
        )
        pdl_people = pdl_data.get("people") or []
        
        # Agentic web search results (Bio + Timeline)
        openai_person_res = raw_results.get("openai_person_profile", {}) or {}
        openai_snippets = openai_person_res.get("snippets") or []
        person_bio = openai_person_res.get("person_bio", {})
        
        # Heuristic: Pick the best PDL candidate if available
        candidate = None
        if pdl_people and person_name:
            norm_target = _normalize_name(person_name)
            for p in pdl_people:
                 if _normalize_name(p.get("full_name") or "") == norm_target:
                     candidate = p
                     break
            # DO NOT fallback to the first PDL result if no name match.
            # It is better to have a sparse profile than a wrong one (e.g. CEO instead of engineer).
                
        # Combine snippets: Exa + OpenAI Bio
        combined_snippets = web_snippets + openai_snippets
        
        # Build PersonTargetNode
        if candidate:
            full_name = candidate.get("full_name") or person_name
            node = PersonTargetNode(
                full_name=full_name,
                normalized_name=_normalize_name(full_name),
                linkedin_url=candidate.get("linkedin_url"),
                primary_role=candidate.get("title"),
                primary_company=candidate.get("organization") or candidate.get("job_company_name"),
                location=candidate.get("location_name") or candidate.get("location"),
                enrichment={"pdl": candidate.get("pdl_data") or candidate, "openai_bio": person_bio},
                web_snippets=combined_snippets
            )
        else:
            # Fallback: Use OpenAI agentic bio if PDL failed
            openai_person = person_bio.get("person") or {}
            full_name = openai_person.get("name") or person_name
            
            node = PersonTargetNode(
                full_name=full_name,
                normalized_name=_normalize_name(full_name),
                linkedin_url=openai_person.get("linkedin_url"),
                primary_role=openai_person.get("current_role"),
                primary_company=openai_person.get("current_company"),
                # OpenAI bio usually provides summary but not location explicitly in top-level
                enrichment={"openai_bio": person_bio},
                web_snippets=combined_snippets
            )
            
        # Stub CompanyNode for backward compatibility with Writer
        company_stub = CompanyNode(
            name=person_name,
            profile={"person_target": True},
            web_snippets=combined_snippets,
            people=[], 
            competitors=competitors_structured
        )
        
        return KnowledgeGraph(
            target_type="person",
            company=company_stub,
            person=node
        )
        
    # --- BRANCH: COMPANY TARGET (Existing Logic) ---

    ch_data = raw_results.get("companies_house_lookup", {}) or {}
    apollo_data = raw_results.get("people_enrichment", {}) or {}
    pdl_data = raw_results.get("pdl_people_discovery", {}) or {}
    oc_data = raw_results.get("open_corporates_lookup", {}) or {}
    oc_company = oc_data.get("company") or {}
    gleif_data = raw_results.get("gleif_lookup", {}) or {}
    gleif_company = gleif_data.get("company") or {}
    gleif_candidates_raw = gleif_data.get("candidates") or []
    pdl_company_data = raw_results.get("pdl_company_enrich", {}) or {}
    pdl_company = pdl_company_data.get("company") or {}
    openai_founding = raw_results.get("openai_founding", {}) or {}
    
    # Extract OpenAI leadership people
    openai_leadership = raw_results.get("openai_leadership", {}) or {}
    people_web = openai_leadership.get("people_web") or []

    ch_company = ch_data.get("company", {}) or {}
    apollo_org = apollo_data.get("organization") or {}

    company_name = (
        (target_input or {}).get("company_name")
        or pdl_company.get("name")
        or apollo_org.get("name")
        or ch_company.get("company_name")
        or oc_company.get("name")
        or gleif_company.get("legal_name")
        or "Unknown"
    )

    domain, domain_source, domain_conf = _infer_domain(
        target_input,
        apollo_data,
        pdl_company,
        domain_snippet_candidates or web_snippets,
    )

    gleif_ranked_candidates: List[dict] = []
    # Re-rank GLEIF candidates using richer context if available.
    if gleif_company or gleif_candidates_raw:
        candidate_pool = gleif_candidates_raw[:]
        if not candidate_pool and gleif_company:
            candidate_pool = [gleif_company]

        domain_tokens = _domain_core_tokens(domain or "") if domain else []
        best, ranked = _rank_gleif_candidates(
            candidate_pool,
            company_name or gleif_company.get("legal_name"),
            pdl_company,
            (target_input or {}).get("country_code"),
            domain_tokens,
        )

        if best:
            gleif_company = best
        gleif_ranked_candidates = ranked

    if oc_company:
        # Compact, human-readable snippet summarising key corporate facts.
        oc_snippet_lines = []

        if oc_company.get("name"):
            oc_snippet_lines.append(f"Legal name: {oc_company['name']}")
        if oc_company.get("company_number"):
            oc_snippet_lines.append(f"Company number: {oc_company['company_number']}")
        if oc_company.get("jurisdiction_code"):
            oc_snippet_lines.append(f"Jurisdiction: {oc_company['jurisdiction_code']}")
        if oc_company.get("incorporation_date"):
            oc_snippet_lines.append(f"Incorporation date: {oc_company['incorporation_date']}")
        if oc_company.get("current_status") is not None:
            oc_snippet_lines.append(f"Status: {oc_company['current_status']}")
        if oc_company.get("registered_address_in_full"):
            oc_snippet_lines.append(
                f"Registered address: {oc_company['registered_address_in_full']}"
            )
        # Include identifier summary if present
        identifiers = oc_company.get("identifiers") or []
        if identifiers:
            # identifiers list from _fetch_company normalization
            parts = []
            for ident in identifiers:
                uid = ident.get("uid")
                system = ident.get("system_code")
                if uid and system:
                    parts.append(f"{system}={uid}")
            if parts:
                oc_snippet_lines.append("Identifiers: " + "; ".join(parts))

        if oc_snippet_lines:
            web_snippets.append(
                {
                    "provider": "open_corporates",
                    "title": f"OpenCorporates company record for {oc_company.get('name') or company_name}",
                    "snippet": "\n".join(oc_snippet_lines),
                    "url": oc_company.get("opencorporates_url") or oc_company.get("registry_url"),
                }
            )

    if gleif_company:
        gleif_snippet_lines = []

        if gleif_company.get("legal_name"):
            gleif_snippet_lines.append(f"Legal name: {gleif_company['legal_name']}")
        if gleif_company.get("lei"):
            gleif_snippet_lines.append(f"LEI: {gleif_company['lei']}")
        if gleif_company.get("legal_jurisdiction"):
            gleif_snippet_lines.append(
                f"Legal jurisdiction: {gleif_company['legal_jurisdiction']}"
            )

        ra_id = gleif_company.get("registration_authority_id")
        ra_num = gleif_company.get("registration_authority_entity_id")
        if ra_id or ra_num:
            gleif_snippet_lines.append(
                "Registration authority: "
                f"{ra_id or 'N/A'} – local registration ID: {ra_num or 'N/A'}"
            )

        legal_addr = gleif_company.get("legal_address") or {}
        if legal_addr:
            addr_parts = [
                legal_addr.get("city"),
                legal_addr.get("region"),
                legal_addr.get("country"),
                legal_addr.get("postal_code"),
            ]
            addr_str = ", ".join(p for p in addr_parts if p)
            if addr_str:
                gleif_snippet_lines.append(f"Registered address: {addr_str}")

        reg = (gleif_company.get("registration") or {})
        if reg.get("status"):
            gleif_snippet_lines.append(f"LEI registration status: {reg['status']}")
        if reg.get("initial_registration_date"):
            gleif_snippet_lines.append(
                f"LEI first issued: {reg['initial_registration_date']}"
            )

        if gleif_snippet_lines:
            gleif_base = getattr(settings, "GLEIF_BASE_URL", "https://api.gleif.org/api/v1").replace("/api/v1", "")
            # Typically GLEIF search UI is https://search.gleif.org/#/record/{lei}
            # But sticking to what the instruction asked:
            url = (
                f"https://search.gleif.org/#/record/{gleif_company['lei']}"
                if gleif_company.get("lei")
                else None
            )
            
            web_snippets.append(
                {
                    "provider": "gleif",
                    "title": (
                        f"GLEIF LEI record for "
                        f"{gleif_company.get('legal_name') or company_name}"
                    ),
                    "snippet": "\n".join(gleif_snippet_lines),
                    "url": url,
                }
            )

    # Base profile from Companies House
    profile: Dict[str, Any] = {
        "registered_office": ch_company.get("registered_office_address"),
        "status": ch_company.get("company_status"),
        "incorporated_on": ch_company.get("date_of_creation"),
        "filings": ch_data.get("filings", []),
    }

    # OpenCorporates company snapshot (if available)
    if oc_company:
        oc_profile = {
            "name": oc_company.get("name"),
            "company_number": oc_company.get("company_number"),
            "jurisdiction_code": oc_company.get("jurisdiction_code"),
            "company_type": oc_company.get("company_type"),
            "current_status": oc_company.get("current_status"),
            "incorporation_date": oc_company.get("incorporation_date"),
            "dissolution_date": oc_company.get("dissolution_date"),
            "registered_address": oc_company.get("registered_address"),
            "registered_address_in_full": oc_company.get("registered_address_in_full"),
            "registry_url": oc_company.get("registry_url"),
            "opencorporates_url": oc_company.get("opencorporates_url"),
            "identifiers": oc_company.get("identifiers") or [],
            "previous_names": oc_company.get("previous_names") or [],
        }
        profile["opencorporates_company"] = oc_profile

        # Fill gaps if Companies House data isn't available
        if not profile.get("incorporated_on") and oc_profile.get("incorporation_date"):
            profile["incorporated_on"] = oc_profile["incorporation_date"]

        if not profile.get("status") and oc_profile.get("current_status"):
            profile["status"] = oc_profile["current_status"]

        if not profile.get("registered_office"):
            # Use structured address if present; else full string
            profile["registered_office"] = (
                oc_profile.get("registered_address")
                or oc_profile.get("registered_address_in_full")
            )

    # GLEIF LEI-based company snapshot
    if gleif_company:
        gleif_profile = {
            "lei": gleif_company.get("lei"),
            "legal_name": gleif_company.get("legal_name"),
            "legal_jurisdiction": gleif_company.get("legal_jurisdiction"),
            "entity_category": gleif_company.get("entity_category"),
            "entity_status": gleif_company.get("entity_status"),
            "legal_address": gleif_company.get("legal_address"),
            "headquarters_address": gleif_company.get("headquarters_address"),
            "registration_authority_id": gleif_company.get("registration_authority_id"),
            "registration_authority_entity_id": gleif_company.get("registration_authority_entity_id"),
            "registration": gleif_company.get("registration"),
        }
        profile["gleif_company"] = gleif_profile

        # If we still don't have a status or registered_office, GLEIF can backfill.
        if not profile.get("status") and gleif_profile.get("entity_status"):
            profile["status"] = gleif_profile["entity_status"]
        if not profile.get("registered_office") and gleif_profile.get("legal_address"):
            profile["registered_office"] = gleif_profile["legal_address"]

    if gleif_ranked_candidates:
        profile["gleif_candidates"] = gleif_ranked_candidates

    # Apollo firmographics snapshot (kept small + normalised)
    if apollo_org:
        firmographics = {
            "apollo_organization_id": apollo_org.get("apollo_organization_id")
            or apollo_org.get("id"),
            "name": apollo_org.get("name"),
            "primary_domain": apollo_org.get("primary_domain"),
            "estimated_num_employees": apollo_org.get("estimated_num_employees"),
            "founded_year": apollo_org.get("founded_year"),
            "annual_revenue": apollo_org.get("annual_revenue"),
        }
        profile["apollo_organization"] = firmographics
        profile["apollo_firmographics"] = firmographics

    # PDL Company profile & Funding
    if pdl_company:
        profile["pdl_company"] = pdl_company
    
    pdl_funding_rollup = pdl_company_data.get("funding_rollup")
    if pdl_funding_rollup:
        profile["pdl_funding_rollup"] = pdl_funding_rollup

    pdl_funding_details = pdl_company_data.get("funding_details") or []
    if pdl_funding_details:
        for fd in pdl_funding_details:
            funding_rounds_structured.append({
                "date": fd.get("funding_round_date"),
                "amount": fd.get("funding_raised"),
                "currency": fd.get("funding_currency"),
                "type": fd.get("funding_type"),
                "investors_companies": fd.get("investing_companies") or [],
                "investors_individuals": fd.get("investing_individuals") or [],
                "source": "pdl_company"
            })

    # OpenAI Founding Facts fallback
    founding_facts = openai_founding.get("founding_facts")
    if founding_facts:
        profile["founding_facts_web"] = founding_facts

    company_node = CompanyNode(
        name=company_name,
        domain=domain,
        domain_confidence=domain_conf,
        domain_source=domain_source,
        companies_house_number=ch_company.get("company_number"),
        apollo_organization_id=(
            apollo_org.get("apollo_organization_id")
            or apollo_org.get("id")
            if apollo_org
            else None
        ),
        apollo_estimated_num_employees=apollo_org.get("estimated_num_employees")
        if apollo_org
        else None,
        apollo_founded_year=apollo_org.get("founded_year") if apollo_org else None,
        apollo_annual_revenue=apollo_org.get("annual_revenue") if apollo_org else None,
        profile=profile,
        people=[],
        web_snippets=web_snippets,
        competitors=competitors_structured,
        funding_rounds=funding_rounds_structured,
    )

    people_nodes = _build_people_nodes(ch_data, apollo_data, pdl_data)

    # OpenAI web-derived leadership
    for p in people_web:
        full_name = p.get("name") or "Unknown"
        role = p.get("role")
        node = PersonNode(
            full_name=full_name,
            roles=[role] if role else [],
            linkedin_url=None,
            photo_url=None,
            identity_source="web",
            enrichment={"openai-web": p},
        )
        # We append these directly; _enrich_with_pdl below will attempt to enrich them
        # if they look like valid targets (though they lack LinkedIn URLs).
        people_nodes.append(node)

    # Optionally enrich via People Data Labs (biography layer)
    # Only enriches Apollo-discovered people who don't already have PDL data
    _enrich_with_pdl(people_nodes, company_name, domain)

    company_node.people = people_nodes

    return KnowledgeGraph(company=company_node)
