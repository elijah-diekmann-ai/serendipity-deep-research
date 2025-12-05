"""
Tests for micro_research.py

Tests provider labeling in snippet extraction and other micro-research functions.
"""
import pytest

from app.services.micro_research import (
    _compute_content_hash,
    _extract_snippets_from_results,
    _infer_provider_from_step,
)


class TestInferProviderFromStep:
    """Tests for the _infer_provider_from_step helper function."""

    def test_exa_step_returns_exa(self):
        """Step names containing 'exa' should return 'exa' provider."""
        assert _infer_provider_from_step("micro_exa_news_search_0") == "exa"
        assert _infer_provider_from_step("micro_exa_site_search_1") == "exa"
        assert _infer_provider_from_step("micro_exa_funding_search_2") == "exa"

    def test_openai_step_returns_openai_web(self):
        """Step names containing 'openai' should return 'openai-web' provider."""
        assert _infer_provider_from_step("micro_openai_web_search_0") == "openai-web"
        assert _infer_provider_from_step("openai_search_step") == "openai-web"

    def test_pdl_step_returns_pdl(self):
        """Step names containing 'pdl' should return 'pdl' provider."""
        assert _infer_provider_from_step("micro_pdl_person_search_0") == "pdl"
        assert _infer_provider_from_step("micro_pdl_person_enrich_1") == "pdl"

    def test_pdl_company_step_returns_pdl_company(self):
        """Step names containing 'pdl_company' should return 'pdl_company' provider."""
        assert _infer_provider_from_step("micro_pdl_company_search_0") == "pdl_company"

    def test_gleif_step_returns_gleif(self):
        """Step names containing 'gleif' should return 'gleif' provider."""
        assert _infer_provider_from_step("micro_gleif_lei_lookup_0") == "gleif"

    def test_unknown_step_returns_unknown(self):
        """Unknown step names should return 'unknown'."""
        assert _infer_provider_from_step("some_other_step") == "unknown"


class TestExtractSnippetsFromResults:
    """Tests for the _extract_snippets_from_results function."""

    def test_exa_results_labeled_as_exa(self):
        """Exa results should be labeled with 'exa' provider."""
        raw_results = {
            "micro_exa_news_search_0": {
                "results": [
                    {
                        "url": "https://example.com/news",
                        "title": "Test News",
                        "text": "This is news content.",
                        "published_date": "2024-01-15",
                    }
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert snippets[0]["provider"] == "exa"
        assert snippets[0]["title"] == "Test News"
        assert snippets[0]["url"] == "https://example.com/news"

    def test_openai_web_snippets_labeled_correctly(self):
        """OpenAI web search results should be labeled with 'openai-web'."""
        raw_results = {
            "micro_openai_web_search_0": {
                "web_snippets": [
                    {
                        "url": "https://example.com/openai",
                        "title": "OpenAI Result",
                        "snippet": "Content from OpenAI web search.",
                    }
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert snippets[0]["provider"] == "openai-web"
        assert snippets[0]["title"] == "OpenAI Result"

    def test_openai_structured_output_labeled_correctly(self):
        """OpenAI structured output should be labeled with 'openai-web'."""
        raw_results = {
            "micro_openai_web_search_0": {
                "structured_output": {
                    "competitors": ["Company A", "Company B"],
                }
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert snippets[0]["provider"] == "openai-web"
        assert "Structured data" in snippets[0]["title"]

    def test_pdl_people_results_labeled_correctly(self):
        """PDL people results should be labeled with 'pdl' provider."""
        raw_results = {
            "micro_pdl_person_enrich_0": {
                "people": [
                    {
                        "full_name": "John Smith",
                        "title": "CEO",  # New normalized field
                        "company": "Example Inc",  # New normalized field
                        "linkedin_url": "https://linkedin.com/in/johnsmith",
                    }
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert snippets[0]["provider"] == "pdl"
        assert "John Smith" in snippets[0]["title"]

    def test_pdl_company_results_labeled_correctly(self):
        """PDL company results should be labeled with 'pdl_company' provider."""
        raw_results = {
            "micro_pdl_company_search_0": {
                "company": {
                    "name": "Example Corp",
                    "founded": 2020,
                    "total_funding_raised": 5000000,
                }
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert snippets[0]["provider"] == "pdl_company"

    def test_gleif_results_labeled_correctly(self):
        """GLEIF results should be labeled with 'gleif' provider."""
        raw_results = {
            "micro_gleif_lei_lookup_0": {
                "results": [
                    {
                        "url": None,
                        "title": "GLEIF Legal Entity",
                        "text": "LEI: 549300ABCDEFGHIJKLMN",
                    }
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert snippets[0]["provider"] == "gleif"

    def test_item_provider_overrides_step_inference(self):
        """Item-level provider should override step-level inference."""
        raw_results = {
            "micro_exa_news_search_0": {
                "results": [
                    {
                        "url": "https://example.com/special",
                        "title": "Special Source",
                        "text": "Content with explicit provider.",
                        "provider": "custom_source",  # Explicit provider
                    }
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert snippets[0]["provider"] == "custom_source"

    def test_dedupe_by_url(self):
        """Duplicate URLs should be deduplicated."""
        raw_results = {
            "micro_exa_news_search_0": {
                "results": [
                    {"url": "https://example.com/same", "title": "First", "text": "First content"},
                    {"url": "https://example.com/same", "title": "Second", "text": "Second content"},
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1  # Second one should be deduped
        assert snippets[0]["title"] == "First"

    def test_handles_exa_highlights(self):
        """Should handle Exa highlights array as snippet text."""
        raw_results = {
            "micro_exa_site_search_0": {
                "results": [
                    {
                        "url": "https://example.com/highlights",
                        "title": "With Highlights",
                        "highlights": ["First highlight", "Second highlight"],
                    }
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert "First highlight" in snippets[0]["snippet"]
        assert "Second highlight" in snippets[0]["snippet"]

    def test_empty_results_returns_empty(self):
        """Empty results should return empty list."""
        raw_results = {}
        snippets = _extract_snippets_from_results(raw_results)
        assert snippets == []

    def test_skips_items_without_text(self):
        """Items without any text content should be skipped."""
        raw_results = {
            "micro_exa_news_search_0": {
                "results": [
                    {"url": "https://example.com/empty", "title": "No Content"},
                    {"url": "https://example.com/valid", "title": "Has Content", "text": "Valid text"},
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert snippets[0]["title"] == "Has Content"


class TestPDLFieldNormalization:
    """Tests for PDL field name normalization in snippet extraction."""

    def test_uses_normalized_title_field(self):
        """Should use 'title' field for job title (normalized PDL output)."""
        raw_results = {
            "micro_pdl_person_enrich_0": {
                "people": [
                    {
                        "full_name": "Jane Doe",
                        "title": "CTO",  # Normalized field
                        "company": "TechCorp",  # Normalized field
                    }
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert "Title: CTO" in snippets[0]["snippet"]
        assert "Company: TechCorp" in snippets[0]["snippet"]

    def test_falls_back_to_raw_job_title(self):
        """Should fall back to 'job_title' if 'title' not present."""
        raw_results = {
            "micro_pdl_person_enrich_0": {
                "people": [
                    {
                        "full_name": "Jane Doe",
                        "job_title": "Engineer",  # Raw PDL field
                        "job_company_name": "OldCorp",  # Raw PDL field
                    }
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert "Title: Engineer" in snippets[0]["snippet"]
        assert "Company: OldCorp" in snippets[0]["snippet"]

    def test_handles_education_from_pdl_data(self):
        """Should extract education from pdl_data if present."""
        raw_results = {
            "micro_pdl_person_enrich_0": {
                "people": [
                    {
                        "full_name": "John Scholar",
                        "pdl_data": {
                            "education": [
                                {"school": {"name": "Stanford University"}}
                            ]
                        }
                    }
                ]
            }
        }
        snippets = _extract_snippets_from_results(raw_results)
        assert len(snippets) == 1
        assert "Education: Stanford University" in snippets[0]["snippet"]


class TestComputeContentHash:
    """Tests for the content hash computation function."""

    def test_same_content_same_hash(self):
        """Identical content should produce identical hashes."""
        text1 = "This is some test content."
        text2 = "This is some test content."
        assert _compute_content_hash(text1) == _compute_content_hash(text2)

    def test_normalized_whitespace(self):
        """Whitespace differences should not affect hash."""
        text1 = "This   is  some    content."
        text2 = "This is some content."
        assert _compute_content_hash(text1) == _compute_content_hash(text2)

    def test_case_insensitive(self):
        """Hash should be case-insensitive."""
        text1 = "THIS IS SOME CONTENT"
        text2 = "this is some content"
        assert _compute_content_hash(text1) == _compute_content_hash(text2)

    def test_different_content_different_hash(self):
        """Different content should produce different hashes."""
        text1 = "First content"
        text2 = "Second content"
        assert _compute_content_hash(text1) != _compute_content_hash(text2)

    def test_hash_is_64_chars(self):
        """SHA256 hex digest should be 64 characters."""
        hash_val = _compute_content_hash("Any text")
        assert len(hash_val) == 64

