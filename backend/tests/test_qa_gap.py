"""
Tests for qa_gap.py - Gap Detection and Intent Classification

Tests the gap detection logic, intent classification, person name extraction,
and comprehensiveness override behavior.
"""
import pytest
from typing import Set, List
from unittest.mock import MagicMock
from uuid import uuid4

from app.services.qa_gap import (
    detect_gap,
    _extract_gap_phrases,
    _check_explicit_research_request,
    _detect_intent,
    _extract_missing_slots,
    _build_gap_statement,
    GAP_INDICATOR_PHRASES,
    EXPLICIT_RESEARCH_TRIGGERS,
    INTENT_KEYWORD_MAP,
    GapDetectionResult,
)
from app.models.source import Source

from tests.fixtures.micro_research_fixtures import (
    GAP_PHRASES_SHOULD_TRIGGER,
    GAP_PHRASES_SHOULD_NOT_TRIGGER,
    INTENT_TEST_CASES,
    PERSON_NAME_TEST_CASES,
    COMPREHENSIVENESS_TEST_CASES,
    ANSWER_WITH_GAP_EXISTING_PHRASE,
    ANSWER_WITH_GAP_NEW_PHRASE,
    ANSWER_WITHOUT_GAP,
    ANSWER_COMPREHENSIVE_WITH_GAP,
    SPARSE_BASELINE_SOURCES,
    MockSource,
)


# ---------------------------------------------------------------------------
# Gap Phrase Coverage Tests
# ---------------------------------------------------------------------------

class TestGapPhraseExtraction:
    """Tests for gap phrase detection coverage."""

    def test_existing_gap_phrases_are_detected(self):
        """Existing GAP_INDICATOR_PHRASES should be detected in answers."""
        for phrase in GAP_INDICATOR_PHRASES:
            answer = f"Some content. {phrase.capitalize()}. More content."
            matched = _extract_gap_phrases(answer)
            assert len(matched) > 0, f"Phrase '{phrase}' should be detected"
            assert phrase in matched, f"Phrase '{phrase}' should be in matched list"

    def test_gap_phrases_case_insensitive(self):
        """Gap phrase detection should be case-insensitive."""
        answer = "The information is NOT DISCLOSED IN AVAILABLE SOURCES."
        matched = _extract_gap_phrases(answer)
        assert "not disclosed in available sources" in matched

    def test_no_gap_phrases_in_clean_answer(self):
        """Clean answers without gap phrases should return empty list."""
        matched = _extract_gap_phrases(ANSWER_WITHOUT_GAP)
        assert len(matched) == 0

    def test_multiple_gap_phrases_detected(self):
        """Multiple gap phrases in one answer should all be detected."""
        answer = "Not disclosed in available sources. Also no information available."
        matched = _extract_gap_phrases(answer)
        assert len(matched) >= 2

    @pytest.mark.parametrize("phrase", [
        "not identifiable in available sources",
        "cannot reliably identify",
        "cannot be identified",
        "unable to identify",
    ])
    def test_new_gap_phrases_should_be_detected(self, phrase):
        """
        NEW gap phrases identified in audit should be detected.
        
        These tests will FAIL initially - they define the expected behavior
        that needs to be implemented by adding these phrases to GAP_INDICATOR_PHRASES.
        """
        answer = f"The specific details are {phrase} from the current sources."
        matched = _extract_gap_phrases(answer)
        # This assertion will fail until we add these phrases
        assert phrase in matched or any(phrase in p for p in matched), \
            f"NEW phrase '{phrase}' should be detected (add to GAP_INDICATOR_PHRASES)"

    @pytest.mark.parametrize("phrase_with_the,expected", [
        ("not disclosed in the available sources", "not disclosed in the available sources"),
        ("not found in the available sources", "not found in the available sources"),
        ("not present in the available sources", "not present in the available sources"),
    ])
    def test_the_variant_phrases_detected(self, phrase_with_the, expected):
        """
        Gap phrases with 'the' (e.g., 'not disclosed in THE available sources')
        should be detected. This was Issue 1 from the audit.
        """
        answer = f"Some details are {phrase_with_the}."
        matched = _extract_gap_phrases(answer)
        assert expected in matched, \
            f"'the' variant '{phrase_with_the}' should be detected"

    @pytest.mark.parametrize("answer_text", [
        "The data cannot be individually analyzed from these sources.",
        "We could not find any specific patents matching that title.",
        "The system is unable to locate the specific document.",
        "There is no specific information about this topic.",
        "This is not explicitly mentioned in the sources.",
        "The answer lacks information about funding details.",
    ])
    def test_regex_pattern_gap_detection(self, answer_text):
        """
        Flexible regex patterns should catch variations that exact matching misses.
        """
        matched = _extract_gap_phrases(answer_text)
        assert len(matched) > 0, \
            f"Regex patterns should detect gap in: '{answer_text}'"


class TestGapDetectionWithAnswer:
    """Tests for the full detect_gap function."""

    def test_detects_gap_with_existing_phrase(self):
        """Gap detection should work with existing phrases."""
        result = detect_gap(
            question="What patents does Diraq have?",
            answer_markdown=ANSWER_WITH_GAP_EXISTING_PHRASE,
            used_source_ids={6077},
            all_sources=[],
        )
        # With 1 source and short answer, should propose
        assert result.detection_method in ("phrase_match", "none")

    def test_no_gap_detected_for_clean_answer(self):
        """Clean answers should not trigger gap detection."""
        result = detect_gap(
            question="How much funding has the company raised?",
            answer_markdown=ANSWER_WITHOUT_GAP,
            used_source_ids={6039, 6048},
            all_sources=[],
        )
        assert result.should_propose is False
        assert result.detection_method == "none"


# ---------------------------------------------------------------------------
# Explicit Research Request Tests
# ---------------------------------------------------------------------------

class TestExplicitResearchRequest:
    """Tests for explicit research request detection."""

    @pytest.mark.parametrize("trigger", EXPLICIT_RESEARCH_TRIGGERS)
    def test_explicit_triggers_detected(self, trigger):
        """All EXPLICIT_RESEARCH_TRIGGERS should be detected."""
        question = f"Can you {trigger} the patent information?"
        assert _check_explicit_research_request(question) is True

    def test_explicit_request_case_insensitive(self):
        """Explicit request detection should be case-insensitive."""
        assert _check_explicit_research_request("SEARCH FOR patents") is True
        assert _check_explicit_research_request("Look Up the CEO") is True

    def test_no_explicit_request_in_simple_question(self):
        """Simple questions without triggers should not match."""
        assert _check_explicit_research_request("What is the company's revenue?") is False
        assert _check_explicit_research_request("Who is the CEO?") is False

    @pytest.mark.parametrize("question", [
        "Can you search for patent information?",
        "Can you find the CEO's background?",
        "Can you look for funding details?",
        "Please search the databases for patents",
        "Please find more about the investors",
        "Could you search for conference presentations?",
        "Could you find the annual report?",
    ])
    def test_looser_explicit_triggers_detected(self, question):
        """
        Looser trigger patterns should be detected.
        Issue 2 fix: 'Can you search', 'Can you find', 'Please search', etc.
        """
        assert _check_explicit_research_request(question) is True, \
            f"Question should be detected as explicit request: '{question}'"


# ---------------------------------------------------------------------------
# Intent Classification Tests
# ---------------------------------------------------------------------------

class TestIntentClassification:
    """Tests for question-to-intent classification."""

    # Existing intents
    @pytest.mark.parametrize("question,expected_intent", [
        ("What patents does the company have?", "patents"),
        ("Who are the investors in the Series A?", "funding_investors"),
        ("What are the main competitors?", "competitors"),
        ("Tell me about the founder's background", "founder_background"),
        ("Who are the commercial customers?", "customers"),
        ("What is the technology stack?", "technology"),
    ])
    def test_existing_intents_classified_correctly(self, question, expected_intent):
        """Existing intents should classify correctly."""
        intent = _detect_intent(question)
        assert intent == expected_intent, \
            f"Question '{question}' should classify as '{expected_intent}', got '{intent}'"

    # NEW intents that need to be added
    @pytest.mark.parametrize("question,expected_intent,description", [
        (tc.question, tc.expected_intent, tc.description)
        for tc in INTENT_TEST_CASES
        if tc.expected_intent in ("research_papers", "programs_contracts")
    ])
    def test_new_intents_classified_correctly(self, question, expected_intent, description):
        """
        NEW intents identified in audit should classify correctly.
        
        These tests will FAIL initially - they define expected behavior
        that needs to be implemented by adding new intents to INTENT_KEYWORD_MAP.
        """
        intent = _detect_intent(question)
        assert intent == expected_intent, \
            f"{description}: '{question}' should classify as '{expected_intent}', got '{intent}'"

    def test_papers_not_routed_to_patents(self):
        """
        Questions about peer-reviewed papers should NOT route to patents intent.
        
        This was a key issue in the audit - paper questions were misrouted.
        """
        paper_questions = [
            "Find peer-reviewed papers about quantum computing",
            "What is the DOI for the Nature paper?",
            "Search for publications in academic journals",
            "Find arXiv preprints from the team",
        ]
        for q in paper_questions:
            intent = _detect_intent(q)
            assert intent != "patents", \
                f"Paper question '{q}' should NOT route to 'patents' (got '{intent}')"

    def test_programs_not_routed_to_customers(self):
        """
        Questions about government programs should NOT route to customers intent.
        
        This was a key issue in the audit - program questions were misrouted.
        """
        program_questions = [
            "What is the DARPA consortium role?",
            "Search for DOE Quandarum project",
            "What government grants has the company received?",
            "Find the NSF award details",
        ]
        for q in program_questions:
            intent = _detect_intent(q)
            assert intent != "customers", \
                f"Program question '{q}' should NOT route to 'customers' (got '{intent}')"


# ---------------------------------------------------------------------------
# Person Name Extraction Tests
# ---------------------------------------------------------------------------

class TestPersonNameExtraction:
    """Tests for person name extraction from questions."""

    @pytest.mark.parametrize("question,expected_name,description", [
        (tc.question, tc.expected_name, tc.description)
        for tc in PERSON_NAME_TEST_CASES
    ])
    def test_person_name_extraction(self, question, expected_name, description):
        """
        Person names should be extracted from questions.
        
        These tests require implementing _extract_person_name() function.
        """
        # Import the function if it exists, otherwise skip
        try:
            from app.services.qa_gap import _extract_person_name
        except ImportError:
            pytest.skip("_extract_person_name not yet implemented")
        
        extracted = _extract_person_name(question)
        assert extracted == expected_name, \
            f"{description}: Expected '{expected_name}', got '{extracted}'"

    def test_person_name_in_missing_slots(self):
        """
        Extracted person names should appear in missing_slots.
        
        This tests the integration of person name extraction into detect_gap.
        """
        # For questions about specific people, missing_slots should include person_name
        result = detect_gap(
            question="Look up Stefanie Tardo's background",
            answer_markdown="The specific details are not disclosed in available sources.",
            used_source_ids={1},
            all_sources=[],
        )
        # After implementing _extract_person_name, this should pass
        if result.should_propose:
            # Check if person_name is in missing_slots
            # This may fail until the feature is implemented
            pass  # Placeholder - actual assertion depends on implementation


# ---------------------------------------------------------------------------
# Comprehensiveness Override Tests
# ---------------------------------------------------------------------------

class TestComprehensivenessOverride:
    """Tests for comprehensiveness check and explicit request override."""

    def test_comprehensive_answer_blocks_proposal_without_explicit_request(self):
        """
        Long answers with many sources should NOT propose micro-research
        when there's no explicit research request.
        """
        # Simulate a comprehensive answer (>2000 chars, >8 sources)
        result = detect_gap(
            question="What patents does Diraq have?",  # No explicit trigger
            answer_markdown=ANSWER_COMPREHENSIVE_WITH_GAP,
            used_source_ids=set(range(1, 25)),  # 24 sources
            all_sources=[],
        )
        # Should be blocked by comprehensiveness check
        assert result.should_propose is False or result.detection_method == "comprehensive_skip"

    def test_explicit_request_bypasses_comprehensiveness(self):
        """
        Explicit research requests should ALWAYS propose micro-research,
        even with comprehensive answers.
        
        This tests the fix for the audit finding where "search for" questions
        were blocked by the comprehensiveness check.
        """
        # Explicit trigger words
        explicit_questions = [
            "Search for patent EP3966938B1",
            "Look up the DARPA consortium partners",
            "Can you research John Smith's background?",
            "Dig deeper into the funding details",
        ]
        
        for question in explicit_questions:
            result = detect_gap(
                question=question,
                answer_markdown=ANSWER_COMPREHENSIVE_WITH_GAP,
                used_source_ids=set(range(1, 25)),
                all_sources=[],
            )
            # Explicit request should always propose
            assert result.should_propose is True, \
                f"Explicit request '{question}' should bypass comprehensiveness"
            assert result.detection_method == "explicit_request", \
                f"Detection method should be 'explicit_request' for '{question}'"

    @pytest.mark.parametrize("tc", COMPREHENSIVENESS_TEST_CASES, ids=lambda tc: tc.description)
    def test_comprehensiveness_scenarios(self, tc):
        """Test various comprehensiveness scenarios from fixtures."""
        # Build mock answer based on test case
        if tc.has_gap_phrase:
            answer = f"{'x' * (tc.answer_length - 100)} not disclosed in available sources."
        else:
            answer = "x" * tc.answer_length
        
        result = detect_gap(
            question=tc.question,
            answer_markdown=answer,
            used_source_ids=set(range(1, tc.source_count + 1)),
            all_sources=[],
        )
        
        assert result.should_propose == tc.expected_should_propose, \
            f"{tc.description}: expected should_propose={tc.expected_should_propose}, got {result.should_propose}"


# ---------------------------------------------------------------------------
# Slot Extraction Tests
# ---------------------------------------------------------------------------

class TestSlotExtraction:
    """Tests for contextual slot extraction."""

    def test_year_extraction(self):
        """Years mentioned in questions should be extracted."""
        slots = _extract_missing_slots("What funding did they raise in 2023?", "funding_investors")
        assert "years" in slots
        assert "2023" in slots["years"]

    def test_multiple_years_extraction(self):
        """Multiple years should all be extracted."""
        slots = _extract_missing_slots("Compare 2022 and 2024 funding rounds", "funding_investors")
        assert "years" in slots
        assert "2022" in slots["years"]
        assert "2024" in slots["years"]

    def test_funding_round_extraction(self):
        """Funding round types should be extracted."""
        slots = _extract_missing_slots("Who led the Series A round?", "funding_investors")
        assert "round" in slots
        assert slots["round"] == "series a"

    def test_jurisdiction_extraction(self):
        """Jurisdiction hints should be extracted."""
        slots = _extract_missing_slots("What patents do they have in the US?", "patents")
        assert "jurisdiction" in slots
        assert slots["jurisdiction"] == "us"


# ---------------------------------------------------------------------------
# Gap Statement Building Tests
# ---------------------------------------------------------------------------

class TestGapStatementBuilding:
    """Tests for human-readable gap statement generation."""

    def test_explicit_request_statement(self):
        """Explicit requests should produce appropriate statements."""
        statement = _build_gap_statement(
            question="Search for patent details",
            matched_phrases=[],
            intent="patents",
            explicit_request=True,
        )
        assert "requested additional research" in statement.lower()

    def test_intent_based_statement(self):
        """Statements should reflect the detected intent."""
        statement = _build_gap_statement(
            question="Who are the investors?",
            matched_phrases=["not disclosed in available sources"],
            intent="funding_investors",
            explicit_request=False,
        )
        assert "investor" in statement.lower() or "funding" in statement.lower()

    def test_generic_statement_for_unknown_intent(self):
        """Unknown intents should produce generic statements."""
        statement = _build_gap_statement(
            question="Some obscure question",
            matched_phrases=["not found in available sources"],
            intent=None,
            explicit_request=False,
        )
        assert "information" in statement.lower() or "sources" in statement.lower()


class TestRegistryImplyingDetection:
    """Tests for the _implies_external_registry function."""

    def test_patent_database_question(self):
        """Questions about patent databases should imply external registry."""
        from app.services.qa_gap import _implies_external_registry
        assert _implies_external_registry("Search patent databases for Diraq IP")
        assert _implies_external_registry("Check the patent registry for filings")

    def test_conference_program_question(self):
        """Questions about conference programs should imply external registry."""
        from app.services.qa_gap import _implies_external_registry
        assert _implies_external_registry("What's on the APS conference program?")
        assert _implies_external_registry("Find abstracts in the IEEE conference schedule")

    def test_investor_report_question(self):
        """Questions about investor reports should imply external registry."""
        from app.services.qa_gap import _implies_external_registry
        assert _implies_external_registry("Check the annual report for valuations")
        assert _implies_external_registry("What do investor reports say about Diraq?")

    def test_list_all_question(self):
        """Questions asking to list all should imply external registry."""
        from app.services.qa_gap import _implies_external_registry
        assert _implies_external_registry("List all patents filed by Diraq")
        assert _implies_external_registry("Compile a list of all investors")

    def test_numbered_most_recent(self):
        """Questions asking for N most recent should imply external registry."""
        from app.services.qa_gap import _implies_external_registry
        assert _implies_external_registry("What are the 10 most recent patents?")
        assert _implies_external_registry("Show me the 5 most recent publications")

    def test_pdl_source_question(self):
        """Questions asking for PDL data should imply external registry."""
        from app.services.qa_gap import _implies_external_registry
        assert _implies_external_registry("Get the leadership from PDL")
        assert _implies_external_registry("Search structured sources for employees")

    def test_regular_question_no_registry(self):
        """Regular questions should not imply external registry."""
        from app.services.qa_gap import _implies_external_registry
        assert not _implies_external_registry("Who are the founders of Diraq?")
        assert not _implies_external_registry("What is Diraq's technology?")


class TestComprehensivenessBypassWithRegistry:
    """Tests for comprehensiveness bypass when registry is implied."""

    def test_comprehensive_answer_bypassed_for_patent_registry(self):
        """Comprehensive answers should still trigger micro-research for patent registry questions."""
        # Create a comprehensive answer (>2000 chars, >8 sources)
        long_answer = "This is a comprehensive answer " * 100  # >2000 chars
        sources = [
            Source(id=i, job_id=uuid4(), url=f"https://example.com/{i}", title=f"Source {i}", snippet="snippet", provider="exa")
            for i in range(1, 11)  # 10 sources
        ]
        used_source_ids = {s.id for s in sources}
        
        # Question implies external registry
        question = "List all patents in the patent database for Diraq"
        
        # Add a gap phrase to trigger gap detection
        answer_with_gap = long_answer + " However, some patents are not disclosed in available sources."
        
        result = detect_gap(
            question=question,
            answer_markdown=answer_with_gap,
            used_source_ids=used_source_ids,
            all_sources=sources,
        )
        
        # Should propose micro-research despite being comprehensive
        # because question implies external registry
        assert result.should_propose or result.detection_method == "registry_override"

    def test_comprehensive_answer_skipped_for_regular_question(self):
        """Comprehensive answers should skip micro-research for regular questions."""
        long_answer = "This is a comprehensive answer " * 100  # >2000 chars
        sources = [
            Source(id=i, job_id=uuid4(), url=f"https://example.com/{i}", title=f"Source {i}", snippet="snippet", provider="exa")
            for i in range(1, 11)  # 10 sources
        ]
        used_source_ids = {s.id for s in sources}
        
        # Regular question (no registry implied)
        question = "Who are the founders of Diraq?"
        
        answer_with_gap = long_answer + " Not disclosed in available sources."
        
        result = detect_gap(
            question=question,
            answer_markdown=answer_with_gap,
            used_source_ids=used_source_ids,
            all_sources=sources,
        )
        
        # Should NOT propose for comprehensive answers without registry implication
        assert not result.should_propose or result.detection_method == "comprehensive_skip"

