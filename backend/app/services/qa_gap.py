"""
Gap Detection Module for Q&A Micro-Research

Detects if a Q&A answer has gaps that could benefit from additional research.
Uses deterministic heuristics first, with optional lightweight LLM classifier for ambiguous cases.
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Set

from ..models.source import Source

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gap Detection Configuration
# ---------------------------------------------------------------------------

# Phrases that indicate missing information in the answer
# NOTE: These are matched with optional "the" via regex (see GAP_PHRASE_PATTERNS below)
GAP_INDICATOR_PHRASES = [
    "not disclosed in available sources",
    "not found in available sources",
    "not present in available sources",
    "no information available",
    "not mentioned in the sources",
    "sources do not contain",
    "unable to find",
    "no data available",
    "information not available",
    "could not be determined",
    "not specified in",
    "no evidence of",
    # NEW phrases added from audit findings
    "not identifiable in available sources",
    "not identifiable",
    "cannot reliably identify",
    "cannot be identified",
    "unable to identify",
    "no explicit mention",
    "no explicit",
    # "the" variants for phrases that LLMs often generate with "the"
    "not disclosed in the available sources",
    "not found in the available sources",
    "not present in the available sources",
    "not mentioned in available sources",
    "not available in the sources",
    "not in the provided sources",
]

# Regex patterns for flexible gap phrase matching
# These catch variations like "cannot be individually analyzed", "could not find any", etc.
GAP_PHRASE_PATTERNS = [
    r"\bcannot\s+be\s+\w+\s*(analyzed|determined|verified|confirmed|identified)",
    r"\bcould\s+not\s+(find|locate|identify|determine|verify)\b",
    r"\bunable\s+to\s+(find|locate|identify|determine|verify)\b",
    r"\bno\s+(specific|detailed|explicit|clear)\s+(information|data|evidence|mention)",
    r"\bnot\s+(explicitly\s+)?(stated|mentioned|specified|disclosed|provided)\s+in",
    r"\b(lacks|missing)\s+(information|data|details)\s+(about|on|regarding)",
]

# User phrases that explicitly request additional research
EXPLICIT_RESEARCH_TRIGGERS = [
    "look this up",
    "search for",
    "dig deeper",
    "find more",
    "can you research",
    "look up",
    "search the web",
    "find information",
    "get more details",
    "investigate",
    "do additional research",
    "more research",
    # Additional triggers for structured data requests
    "structured sources",
    "from pdl",
    "leadership roster",
    "consolidated roster",
    "compile a roster",
    "compile a list",
    "list all",
    # Looser matching patterns (Issue 2 fix)
    "can you search",
    "can you find",
    "can you look",
    "please search",
    "please find",
    "please look up",
    "could you search",
    "could you find",
]

# Questions that imply need for external registries/databases
# These should trigger micro-research even if answer seems "comprehensive"
REGISTRY_IMPLYING_PATTERNS = [
    r"\bpatent\s+(database|registry|search|lookup)",
    r"\bconference\s+(program|schedule|session)",
    r"\b(annual\s+report|annual\s+account|investor\s+report)",
    r"\b(\d+\s+most\s+recent|list\s+all|compile\s+a)",
    r"\bstructured\s+sources",
    r"\bfrom\s+pdl\b",
    r"\bpatent\s+databases?\b",
    r"\b(aps|ieee|acm)\s+(meeting|conference|program)",
]


def _implies_external_registry(question: str) -> bool:
    """
    Check if question implies need for external registry/database lookup.
    
    These questions often need structured data from external sources
    (patent databases, conference programs, investor reports) that
    may not be fully covered in the baseline research.
    """
    q_lower = question.lower()
    for pattern in REGISTRY_IMPLYING_PATTERNS:
        if re.search(pattern, q_lower):
            return True
    return False

# Intent classification based on question keywords
INTENT_KEYWORD_MAP: dict[str, list[str]] = {
    "funding_investors": [
        "investor", "investors", "funding", "raised", "round", "series",
        "seed", "venture", "capital", "vc", "angel", "lead investor",
        "participated", "backed by", "who invested", "funding round",
    ],
    "revenue_arr": [
        "revenue", "arr", "mrr", "sales", "income", "earnings",
        "profitable", "profitability", "financial", "growth rate",
    ],
    # NOTE: research_papers must come BEFORE patents to take priority
    # for questions about peer-reviewed papers, DOIs, journals
    "research_papers": [
        "peer-reviewed", "peer reviewed", "paper", "papers", "publication",
        "publications", "doi", "journal", "journals", "abstract",
        "citation", "citations", "preprint", "preprints", "arxiv",
        "academic", "scholarly", "research paper", "scientific paper",
        "nature paper", "science paper", "published in",
    ],
    "patents": [
        "patent", "patents", "ip", "intellectual property", "invention",
        "filing", "uspto", "epo", "patent number", "patent portfolio",
    ],
    "litigation": [
        "lawsuit", "litigation", "legal", "court", "sue", "sued",
        "settlement", "dispute", "injunction", "infringement",
    ],
    "founder_background": [
        "founder", "co-founder", "background", "previous", "prior",
        "experience", "education", "degree", "university", "career",
        "work history", "biography", "bio",
    ],
    "competitors": [
        "competitor", "competitors", "competing", "alternative",
        "rival", "market share", "competitive", "vs", "versus",
    ],
    "technology": [
        "technology", "tech stack", "architecture", "platform",
        "how it works", "technical", "infrastructure", "api",
    ],
    "regulatory": [
        "regulatory", "regulation", "compliance", "fda", "sec",
        "approval", "license", "certification", "audit",
    ],
    "acquisitions": [
        "acquisition", "acquired", "merger", "m&a", "bought",
        "purchase", "takeover", "exit", "ipo",
    ],
    # NOTE: programs_contracts must come BEFORE customers to take priority
    # for questions about government programs, grants, consortiums
    "programs_contracts": [
        "program", "programs", "project", "projects", "initiative",
        "consortium", "consortiums", "grant", "grants", "award", "awards",
        "doe", "darpa", "nsf", "nih", "arpa", "government contract",
        "government funding", "federal", "defence", "defense",
        "trailblazer", "qbi", "benchmarking initiative",
    ],
    "customers": [
        "customer", "customers", "client", "clients", "commercial",
        "end user", "case study", "success story", "reference customer",
        "logo", "deployment", "rollout", "production", "contract",
        "agreement", "purchase order", "procurement", "partner",
        "partnership", "collaboration", "pilot", "proof of concept", "poc",
    ],
}


@dataclass
class GapDetectionResult:
    """Result of gap detection analysis."""
    should_propose: bool
    gap_statement: str
    intent: Optional[str] = None
    missing_slots: dict = field(default_factory=dict)
    confidence: float = 0.0
    detection_method: str = "none"  # "phrase_match" | "explicit_request" | "llm" | "none"


def _normalize_text(text: str) -> str:
    """Normalize text for matching."""
    return text.lower().strip()


def _extract_gap_phrases(answer: str) -> List[str]:
    """
    Extract specific gap phrases from the answer that indicate missing information.
    Returns the matched phrases for constructing gap statements.
    
    Uses both:
    1. Exact substring matching for common phrases
    2. Regex patterns for flexible matching of variations
    """
    answer_lower = _normalize_text(answer)
    matched_phrases: List[str] = []
    
    # 1. Check exact phrase matches
    for phrase in GAP_INDICATOR_PHRASES:
        if phrase in answer_lower:
            matched_phrases.append(phrase)
    
    # 2. Check regex patterns for flexible matching
    for pattern in GAP_PHRASE_PATTERNS:
        match = re.search(pattern, answer_lower)
        if match:
            # Add the matched text as the phrase
            matched_text = match.group(0)
            if matched_text not in matched_phrases:
                matched_phrases.append(matched_text)
    
    return matched_phrases


def _check_explicit_research_request(question: str) -> bool:
    """Check if the user explicitly requested additional research."""
    question_lower = _normalize_text(question)
    
    for trigger in EXPLICIT_RESEARCH_TRIGGERS:
        if trigger in question_lower:
            return True
    
    return False


def _detect_intent(question: str) -> Optional[str]:
    """
    Detect the intent/topic of the question based on keywords.
    Returns the most likely intent or None if unclear.
    """
    question_lower = _normalize_text(question)
    intent_scores: dict[str, int] = {}
    
    for intent, keywords in INTENT_KEYWORD_MAP.items():
        score = sum(1 for kw in keywords if kw in question_lower)
        if score > 0:
            intent_scores[intent] = score
    
    if not intent_scores:
        return None
    
    # Return the intent with the highest score
    return max(intent_scores, key=intent_scores.get)


def _extract_person_name(question: str) -> Optional[str]:
    """
    Extract a person's name from a question using regex patterns.
    
    Handles common patterns like:
    - "Look up John Smith's background"
    - "Research Jane Doe (CEO)"
    - "What did Michael Johnson do before joining?"
    
    Returns the extracted name or None if no name found.
    """
    # Common words that are NOT person names (verbs, titles, etc.)
    NON_NAME_FIRST_WORDS = {
        "the", "company", "ceo", "cto", "cfo", "coo",
        "series", "what", "who", "how", "where", "when", "why",
        "can", "could", "would", "should", "look", "find", "search",
        "research", "tell", "list", "get", "about", "for",
    }
    
    def is_valid_name(name: str) -> bool:
        """Check if a string looks like a valid person name."""
        words = name.split()
        if not (2 <= len(words) <= 4):
            return False
        # First word should not be a common non-name word
        if words[0].lower() in NON_NAME_FIRST_WORDS:
            return False
        # All words should be capitalized
        if not all(w[0].isupper() for w in words):
            return False
        return True
    
    # Pattern 1: After "research", "look up", "about" - extract the following name
    # "Research Jane Doe (CEO)" -> "Jane Doe"
    # "look up John Smith's" -> "John Smith"
    # Note: Use [Rr] etc. for case-insensitive matching of the verb
    verb_patterns = [
        r"[Rr]esearch\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})(?:\s*\(|\s+and\b|\s+\w)",
        r"[Ll]ook\s+[Uu]p\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})(?:'s|\s|$|\?)",
        r"[Aa]bout\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})(?:'s|\s|$|\?)",
    ]
    
    for pattern in verb_patterns:
        match = re.search(pattern, question)
        if match:
            name = match.group(1).strip()
            if is_valid_name(name):
                return name
    
    # Pattern 2: Name followed by possessive 's
    # "John Smith's background" -> "John Smith"
    poss_match = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'s\b", question)
    if poss_match:
        name = poss_match.group(1).strip()
        if is_valid_name(name):
            return name
    
    # Pattern 3: Name followed by parenthetical (role)
    # "Jane Doe (CEO)" -> "Jane Doe"
    # But NOT "Research Jane Doe (" - already handled by verb_patterns
    paren_match = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*\(", question)
    if paren_match:
        name = paren_match.group(1).strip()
        if is_valid_name(name):
            return name
    
    # Fallback: Look for any capitalized two-word sequence that looks like a name
    # "What did John Smith do before joining?" -> "John Smith"
    potential_names = re.findall(r"\b([A-Z][a-z]+\s+[A-Z][a-z]+)\b", question)
    for name in potential_names:
        if is_valid_name(name):
            return name
    
    return None


def _extract_missing_slots(question: str, intent: Optional[str]) -> dict:
    """
    Extract contextual slots that might help target the micro-research.
    For example, timeframes, specific entities, jurisdictions, person names, etc.
    """
    # Import here to avoid circular dependency
    from .micro_planner import _extract_must_include_terms
    
    slots: dict = {}
    question_lower = _normalize_text(question)
    
    # Extract year mentions (use non-capturing group to get full year like "2023", not just "20")
    year_matches = re.findall(r'\b((?:19|20)\d{2})\b', question)
    if year_matches:
        slots["years"] = year_matches
    
    # Extract funding round types (use "round" key to align with micro_planner expectations)
    round_types = ["seed", "series a", "series b", "series c", "series d", "pre-seed", "bridge"]
    for rt in round_types:
        if rt in question_lower:
            slots["round"] = rt
            break
    
    # Extract jurisdiction hints (handle punctuation like "in the US?")
    jurisdictions = ["us", "uk", "eu", "australia", "canada", "germany", "france"]
    # Normalize punctuation for matching
    question_normalized = re.sub(r'[?!.,;:]', ' ', question_lower)
    for j in jurisdictions:
        if f" {j} " in f" {question_normalized} " or question_normalized.strip().endswith(f" {j}"):
            slots["jurisdiction"] = j
            break
    
    # Extract customer segment hint
    if "commercial" in question_lower:
        slots["customer_segment"] = "commercial"
    
    # Extract person name for founder_background or person-related queries
    # This enables micro-planner to use person-specific connectors (PDL, OpenAI person mode)
    if intent in ("founder_background",) or any(
        kw in question_lower for kw in ["background", "prior", "previous", "career", "education"]
    ):
        person_name = _extract_person_name(question)
        if person_name:
            slots["person_name"] = person_name
    
    # Extract must-include terms (quoted strings, acronyms, program names)
    # These are high-value specific entities that should appear verbatim in queries
    must_include = _extract_must_include_terms(question)
    if must_include:
        slots["must_include_terms"] = must_include
    
    return slots


def _build_gap_statement(
    question: str,
    matched_phrases: List[str],
    intent: Optional[str],
    explicit_request: bool,
) -> str:
    """
    Build a human-readable gap statement describing what's missing.
    """
    if explicit_request:
        return f"User requested additional research: \"{question[:100]}...\""
    
    if not matched_phrases:
        return "Additional information may be available from external sources."
    
    # Build statement based on intent
    intent_descriptions = {
        "funding_investors": "Investor and funding details",
        "revenue_arr": "Revenue and financial metrics",
        "research_papers": "Academic papers and publications",
        "patents": "Patent and intellectual property information",
        "litigation": "Legal and litigation information",
        "founder_background": "Founder background and career history",
        "competitors": "Competitor information",
        "technology": "Technical architecture details",
        "regulatory": "Regulatory and compliance information",
        "acquisitions": "M&A and acquisition information",
        "programs_contracts": "Government programs, grants, and contracts",
        "customers": "Commercial customers, deployments, and partnerships",
    }
    
    if intent and intent in intent_descriptions:
        topic = intent_descriptions[intent]
        return f"{topic} not fully covered in available sources."
    
    # Generic statement
    return "Some requested information is not present in available sources."


def detect_gap(
    question: str,
    answer_markdown: str,
    used_source_ids: Set[int],
    all_sources: List[Source],
) -> GapDetectionResult:
    """
    Detect if a Q&A answer has gaps that could benefit from additional research.
    
    Uses deterministic heuristics:
    1. Check if answer contains gap indicator phrases ("Not disclosed in available sources")
    2. Check if user explicitly requests research ("look this up", "dig deeper")
    
    Args:
        question: The user's question
        answer_markdown: The generated answer
        used_source_ids: Set of source IDs used in the answer
        all_sources: All available sources for the job
        
    Returns:
        GapDetectionResult with should_propose=True if micro-research is recommended
    """
    # Method 1: Check for explicit research request from user
    explicit_request = _check_explicit_research_request(question)
    if explicit_request:
        intent = _detect_intent(question)
        missing_slots = _extract_missing_slots(question, intent)
        gap_statement = _build_gap_statement(question, [], intent, explicit_request=True)
        
        logger.info(
            "Gap detected via explicit request: intent=%s",
            intent,
            extra={"intent": intent, "method": "explicit_request"},
        )
        
        return GapDetectionResult(
            should_propose=True,
            gap_statement=gap_statement,
            intent=intent,
            missing_slots=missing_slots,
            confidence=0.95,
            detection_method="explicit_request",
        )
    
    # Method 2: Check for gap indicator phrases in the answer
    matched_phrases = _extract_gap_phrases(answer_markdown)
    if matched_phrases:
        # Comprehensiveness check: If the answer is already thorough (long + many sources),
        # don't propose additional research - we've already exhausted what's available.
        # This prevents redundant micro-research when the answer is comprehensive but
        # the specific detail requested simply isn't publicly available.
        
        answer_length = len(answer_markdown)
        num_sources_used = len(used_source_ids)
        
        # Thresholds for "comprehensive answer"
        # - Long answer (>2000 chars) with many sources (>8) = already thorough
        # - Or very long answer (>4000 chars) with moderate sources (>5)
        is_comprehensive = (
            (answer_length > 2000 and num_sources_used >= 8) or
            (answer_length > 4000 and num_sources_used >= 5)
        )
        
        # OVERRIDE: Don't skip if question implies external registries
        # Even comprehensive answers may miss structured registry data
        if is_comprehensive and _implies_external_registry(question):
            logger.info(
                "Answer is comprehensive but question implies external registry; "
                "proceeding with micro-research proposal",
                extra={
                    "answer_length": answer_length,
                    "num_sources": num_sources_used,
                    "matched_phrases": matched_phrases,
                    "method": "registry_override",
                },
            )
            is_comprehensive = False  # Allow micro-research
        
        if is_comprehensive:
            logger.info(
                "Gap phrases found but answer is comprehensive (%d chars, %d sources); "
                "skipping micro-research proposal",
                answer_length,
                num_sources_used,
                extra={
                    "answer_length": answer_length,
                    "num_sources": num_sources_used,
                    "matched_phrases": matched_phrases,
                    "method": "comprehensive_skip",
                },
            )
            return GapDetectionResult(
                should_propose=False,
                gap_statement="",
                intent=None,
                missing_slots={},
                confidence=0.0,
                detection_method="comprehensive_skip",
            )
        
        intent = _detect_intent(question)
        missing_slots = _extract_missing_slots(question, intent)
        gap_statement = _build_gap_statement(question, matched_phrases, intent, explicit_request=False)
        
        # Confidence based on number of gap phrases found
        confidence = min(0.9, 0.5 + 0.1 * len(matched_phrases))
        
        logger.info(
            "Gap detected via phrase match: %d phrases, intent=%s, answer_length=%d, sources=%d",
            len(matched_phrases),
            intent,
            answer_length,
            num_sources_used,
            extra={"matched_phrases": matched_phrases, "intent": intent, "method": "phrase_match"},
        )
        
        return GapDetectionResult(
            should_propose=True,
            gap_statement=gap_statement,
            intent=intent,
            missing_slots=missing_slots,
            confidence=confidence,
            detection_method="phrase_match",
        )
    
    # No gap detected
    logger.debug("No gap detected for question: %s", question[:100])
    
    return GapDetectionResult(
        should_propose=False,
        gap_statement="",
        intent=None,
        missing_slots={},
        confidence=0.0,
        detection_method="none",
    )

