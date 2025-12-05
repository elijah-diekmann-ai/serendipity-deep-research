"""
Shared test fixtures for micro-research tests.

Contains mock data, test cases, and controlled inputs for testing
gap detection, intent classification, and plan generation.
"""
from dataclasses import dataclass
from typing import List, Optional, Dict, Any


# ---------------------------------------------------------------------------
# Gap Phrase Test Cases
# ---------------------------------------------------------------------------

# Phrases that SHOULD trigger gap detection (existing + new)
GAP_PHRASES_SHOULD_TRIGGER = [
    # Existing phrases in qa_gap.py
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
    # NEW phrases that should be added
    "not identifiable in available sources",
    "cannot reliably identify",
    "cannot be identified",
    "unable to identify",
    "no explicit mention",
]

# Phrases that should NOT trigger gap detection
GAP_PHRASES_SHOULD_NOT_TRIGGER = [
    "The company was founded in 2020.",
    "According to the sources, the CEO is John Smith.",
    "The funding round was $10M.",
    "This information is well documented.",
]


# ---------------------------------------------------------------------------
# Intent Classification Test Cases
# ---------------------------------------------------------------------------

@dataclass
class IntentTestCase:
    """Test case for intent classification."""
    question: str
    expected_intent: str
    description: str


# Questions that should route to specific intents
INTENT_TEST_CASES: List[IntentTestCase] = [
    # Patents (existing intent)
    IntentTestCase(
        question="What patents does Diraq have?",
        expected_intent="patents",
        description="Basic patent question",
    ),
    IntentTestCase(
        question="Look up the USPTO filings for the company",
        expected_intent="patents",
        description="Patent filing question",
    ),
    
    # Research Papers (NEW intent - should NOT route to patents)
    IntentTestCase(
        question="Find peer-reviewed papers about their technology",
        expected_intent="research_papers",
        description="Peer-reviewed papers should NOT route to patents",
    ),
    IntentTestCase(
        question="What is the DOI for the 2025 Nature paper?",
        expected_intent="research_papers",
        description="DOI question should route to research_papers",
    ),
    IntentTestCase(
        question="Search for publications in academic journals",
        expected_intent="research_papers",
        description="Journal publications should route to research_papers",
    ),
    IntentTestCase(
        question="Find the arXiv preprints from the team",
        expected_intent="research_papers",
        description="arXiv preprints should route to research_papers",
    ),
    
    # Programs/Contracts (NEW intent - should NOT route to customers)
    IntentTestCase(
        question="What is the DARPA consortium role?",
        expected_intent="programs_contracts",
        description="DARPA consortium should NOT route to customers",
    ),
    IntentTestCase(
        question="Search for DOE Quandarum project details",
        expected_intent="programs_contracts",
        description="DOE project should route to programs_contracts",
    ),
    IntentTestCase(
        question="What government grants has the company received?",
        expected_intent="programs_contracts",
        description="Government grants should route to programs_contracts",
    ),
    IntentTestCase(
        question="Find the NSF award details",
        expected_intent="programs_contracts",
        description="NSF award should route to programs_contracts",
    ),
    
    # Founder Background (existing intent)
    IntentTestCase(
        question="Look up Stefanie Tardo's background",
        expected_intent="founder_background",
        description="Person background question",
    ),
    IntentTestCase(
        question="Research John Smith's prior roles and education",
        expected_intent="founder_background",
        description="Prior roles question",
    ),
    IntentTestCase(
        question="What is the CEO's career history?",
        expected_intent="founder_background",
        description="Career history question",
    ),
    
    # Funding/Investors (existing intent)
    IntentTestCase(
        question="Who are the Series A investors?",
        expected_intent="funding_investors",
        description="Investor question",
    ),
    IntentTestCase(
        question="How much funding has the company raised?",
        expected_intent="funding_investors",
        description="Funding amount question",
    ),
    
    # Competitors (existing intent)
    IntentTestCase(
        question="Who are the main competitors?",
        expected_intent="competitors",
        description="Competitor question",
    ),
    IntentTestCase(
        question="What alternatives exist in the market?",
        expected_intent="competitors",
        description="Market alternatives question",
    ),
    
    # Customers (existing intent)
    IntentTestCase(
        question="Who are their commercial customers?",
        expected_intent="customers",
        description="Customer question",
    ),
    IntentTestCase(
        question="Find case studies and success stories",
        expected_intent="customers",
        description="Case study question",
    ),
]


# ---------------------------------------------------------------------------
# Person Name Extraction Test Cases
# ---------------------------------------------------------------------------

@dataclass
class PersonNameTestCase:
    """Test case for person name extraction."""
    question: str
    expected_name: Optional[str]
    description: str


PERSON_NAME_TEST_CASES: List[PersonNameTestCase] = [
    # Should extract name
    PersonNameTestCase(
        question="Look up Stefanie Tardo's prior roles",
        expected_name="Stefanie Tardo",
        description="Possessive form with 's",
    ),
    PersonNameTestCase(
        question="Research Lasantha Thennakoon (CFO)",
        expected_name="Lasantha Thennakoon",
        description="Name followed by parenthetical role",
    ),
    PersonNameTestCase(
        question="What did John Smith do before joining?",
        expected_name="John Smith",
        description="Name in middle of question",
    ),
    PersonNameTestCase(
        question="Can you look up Andrew Dzurak's education?",
        expected_name="Andrew Dzurak",
        description="Name with 'look up' prefix",
    ),
    PersonNameTestCase(
        question="Research Jane Doe and her career",
        expected_name="Jane Doe",
        description="Name with 'research' prefix",
    ),
    PersonNameTestCase(
        question="Find information about Michael Johnson's background",
        expected_name="Michael Johnson",
        description="Name with possessive",
    ),
    
    # Should NOT extract name (return None)
    PersonNameTestCase(
        question="Tell me about the CEO",
        expected_name=None,
        description="Role without specific name",
    ),
    PersonNameTestCase(
        question="Who is the founder?",
        expected_name=None,
        description="Generic founder question",
    ),
    PersonNameTestCase(
        question="What is the leadership team?",
        expected_name=None,
        description="Generic leadership question",
    ),
    PersonNameTestCase(
        question="List the executives",
        expected_name=None,
        description="Generic executives question",
    ),
]


# ---------------------------------------------------------------------------
# Comprehensiveness Override Test Cases
# ---------------------------------------------------------------------------

@dataclass 
class ComprehensivenessTestCase:
    """Test case for comprehensiveness override logic."""
    question: str
    answer_length: int
    source_count: int
    has_gap_phrase: bool
    is_explicit_request: bool
    expected_should_propose: bool
    description: str


COMPREHENSIVENESS_TEST_CASES: List[ComprehensivenessTestCase] = [
    # Explicit request should ALWAYS propose (bypass comprehensiveness)
    ComprehensivenessTestCase(
        question="Search for patent EP3966938B1",
        answer_length=3000,  # Long answer
        source_count=20,     # Many sources
        has_gap_phrase=True,
        is_explicit_request=True,
        expected_should_propose=True,
        description="Explicit 'search for' bypasses comprehensiveness",
    ),
    ComprehensivenessTestCase(
        question="Look up the DARPA consortium partners",
        answer_length=2500,
        source_count=15,
        has_gap_phrase=True,
        is_explicit_request=True,
        expected_should_propose=True,
        description="Explicit 'look up' bypasses comprehensiveness",
    ),
    ComprehensivenessTestCase(
        question="Can you research John Smith's background?",
        answer_length=4500,
        source_count=10,
        has_gap_phrase=True,
        is_explicit_request=True,
        expected_should_propose=True,
        description="Explicit 'research' bypasses comprehensiveness",
    ),
    
    # Gap phrase WITHOUT explicit request - comprehensiveness check applies
    ComprehensivenessTestCase(
        question="What patents does Diraq have?",
        answer_length=2500,  # >2000 chars
        source_count=10,     # >= 8 sources
        has_gap_phrase=True,
        is_explicit_request=False,
        expected_should_propose=False,
        description="Comprehensive answer without explicit request -> no proposal",
    ),
    ComprehensivenessTestCase(
        question="What patents does Diraq have?",
        answer_length=1500,  # <2000 chars
        source_count=5,      # < 8 sources
        has_gap_phrase=True,
        is_explicit_request=False,
        expected_should_propose=True,
        description="Short answer with gap phrase -> propose",
    ),
    
    # No gap phrase at all
    ComprehensivenessTestCase(
        question="What is the company overview?",
        answer_length=3000,
        source_count=20,
        has_gap_phrase=False,
        is_explicit_request=False,
        expected_should_propose=False,
        description="No gap phrase -> no proposal",
    ),
]


# ---------------------------------------------------------------------------
# Query Hint Derivation Test Cases
# ---------------------------------------------------------------------------

@dataclass
class QueryHintTestCase:
    """Test case for query hint derivation."""
    question: str
    company_name: str
    expected_keywords: List[str]  # Keywords that MUST be in hint
    excluded_keywords: List[str]  # Keywords that must NOT be in hint
    description: str


QUERY_HINT_TEST_CASES: List[QueryHintTestCase] = [
    QueryHintTestCase(
        question="Look up patent 'SYSTEM AND METHOD FOR CONTROLLING QUANTUM PROCESSING ELEMENTS'",
        company_name="Diraq",
        expected_keywords=["system", "method", "controlling", "quantum", "processing", "elements"],
        excluded_keywords=["diraq", "look", "up", "the"],
        description="Should extract patent title keywords",
    ),
    QueryHintTestCase(
        question="Find the DARPA Quantum Benchmarking Initiative partners",
        company_name="Diraq",
        expected_keywords=["darpa", "quantum", "benchmarking", "initiative", "partners"],
        excluded_keywords=["diraq", "find", "the"],
        description="Should extract program name keywords",
    ),
    QueryHintTestCase(
        question="Search for DOE Quandarum project details",
        company_name="Diraq",
        expected_keywords=["quandarum", "project", "details"],
        excluded_keywords=["diraq", "search", "for"],
        description="Should extract project name keywords",
    ),
    QueryHintTestCase(
        question="What is the Nature paper DOI 10.1038/s41586-025-09531-9?",
        company_name="Diraq",
        expected_keywords=["nature", "paper", "doi"],
        excluded_keywords=["diraq", "what", "is", "the"],
        description="Should extract publication keywords",
    ),
]


# ---------------------------------------------------------------------------
# Plan Quality Test Cases
# ---------------------------------------------------------------------------

@dataclass
class PlanQualityTestCase:
    """Test case for plan generation quality."""
    intent: str
    expected_task_types: List[str]  # Task types that SHOULD be in plan
    forbidden_task_types: List[str]  # Task types that should NOT be in plan
    description: str


PLAN_QUALITY_TEST_CASES: List[PlanQualityTestCase] = [
    PlanQualityTestCase(
        intent="research_papers",
        expected_task_types=["exa_research_paper"],
        forbidden_task_types=["exa_patent_search"],
        description="research_papers intent should use exa_research_paper, NOT exa_patent_search",
    ),
    PlanQualityTestCase(
        intent="programs_contracts",
        expected_task_types=["exa_general_search", "exa_news_search"],
        forbidden_task_types=["exa_patent_search"],
        description="programs_contracts should use general/news search",
    ),
    PlanQualityTestCase(
        intent="patents",
        expected_task_types=["exa_patent_search"],
        forbidden_task_types=[],
        description="patents intent should use exa_patent_search",
    ),
    PlanQualityTestCase(
        intent="founder_background",
        expected_task_types=["openai_web_search", "exa_site_search"],
        forbidden_task_types=[],
        description="founder_background should use openai_web and site search",
    ),
]


# ---------------------------------------------------------------------------
# Mock Answer Texts
# ---------------------------------------------------------------------------

ANSWER_WITH_GAP_EXISTING_PHRASE = """
Based on available sources, Diraq has filed several patents related to quantum computing.

- Patent EP3966938B1 covers quantum processing elements.

The remaining 9 patents are **not disclosed in available sources**. [S6077]
"""

ANSWER_WITH_GAP_NEW_PHRASE = """
Based on the available data, the specific patent numbers for 'control' and 'readout' 
technologies are **not identifiable in available sources**.

We cannot reliably identify the full patent portfolio from current sources. [S6077]
"""

ANSWER_WITHOUT_GAP = """
Diraq has raised $35M in funding across multiple rounds:

- Series A (2022): $20M led by Allectus Capital [S6039]
- Series A-2 (2024): $15M led by Quantonation [S6039]

The company has secured significant government grants as well. [S6048]
"""

ANSWER_COMPREHENSIVE_WITH_GAP = """
This is a comprehensive answer with over 2000 characters that contains detailed information.

""" + ("Lorem ipsum dolor sit amet. " * 80) + """

However, the specific detail requested is **not disclosed in available sources**.
""" + " [S6001]" * 10  # Multiple source citations


# ---------------------------------------------------------------------------
# Mock Source Objects
# ---------------------------------------------------------------------------

@dataclass
class MockSource:
    """Mock source object for testing."""
    id: int
    url: str
    title: str
    snippet: str
    provider: str


SPARSE_BASELINE_SOURCES: List[MockSource] = [
    MockSource(
        id=6001,
        url="https://diraq.com/about",
        title="About Diraq",
        snippet="Diraq is a quantum computing company...",
        provider="exa",
    ),
    MockSource(
        id=6002,
        url="https://news.com/diraq-funding",
        title="Diraq Raises $20M",
        snippet="Quantum startup Diraq announced...",
        provider="exa",
    ),
]

COMPREHENSIVE_BASELINE_SOURCES: List[MockSource] = SPARSE_BASELINE_SOURCES + [
    MockSource(
        id=6003,
        url="https://patents.google.com/patent/EP3966938B1",
        title="Patent EP3966938B1",
        snippet="Quantum processing elements patent...",
        provider="exa",
    ),
    MockSource(
        id=6004,
        url="https://nature.com/articles/s41586-025-09531-9",
        title="Nature Paper 2025",
        snippet="Silicon spin qubit demonstration...",
        provider="exa",
    ),
    MockSource(
        id=6005,
        url="https://linkedin.com/in/stefanie-tardo",
        title="Stefanie Tardo - COO",
        snippet="Chief Operating Officer at Diraq...",
        provider="pdl",
    ),
] + [
    MockSource(
        id=6006 + i,
        url=f"https://example.com/source-{i}",
        title=f"Source {i}",
        snippet=f"Additional source content {i}...",
        provider="exa",
    )
    for i in range(20)
]


# ---------------------------------------------------------------------------
# Ground Truth Test Cases
# ---------------------------------------------------------------------------

GROUND_TRUTH_CASES: Dict[str, Dict[str, Any]] = {
    "diraq_patent_specific": {
        "question": "What is patent EP3966938B1 about?",
        "expected_intent": "patents",
        "expected_query_terms": ["EP3966938B1"],
        "expected_task_types": ["exa_patent_search", "exa_general_search"],
    },
    "diraq_nature_paper": {
        "question": "Find the DOI for the 2025 Nature silicon qubit paper",
        "expected_intent": "research_papers",
        "expected_query_terms": ["nature", "silicon", "qubit", "2025"],
        "expected_task_types": ["exa_research_paper"],
    },
    "diraq_darpa_consortium": {
        "question": "What is Diraq's role in the DARPA QBI consortium?",
        "expected_intent": "programs_contracts",
        "expected_query_terms": ["darpa", "qbi", "consortium"],
        "expected_task_types": ["exa_general_search", "exa_news_search"],
    },
    "diraq_person_specific": {
        "question": "Look up Stefanie Tardo's prior roles before Diraq",
        "expected_intent": "founder_background",
        "expected_person_name": "Stefanie Tardo",
        "expected_task_types": ["pdl_person_enrich", "openai_web_search"],
    },
}

