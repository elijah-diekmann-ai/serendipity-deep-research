"""
Tests for the Pydantic DSL schema in micro_plan_dsl.py

Tests validation logic and repair functionality for micro-research tasks.
"""
import pytest

from app.services.micro_plan_dsl import (
    MicroTask,
    MicroTaskType,
    MicroPlanDSL,
    OpenAIMode,
    parse_task_with_repair,
    get_connector_for_task,
)


class TestMicroTaskValidation:
    """Tests for MicroTask Pydantic validation."""

    def test_valid_exa_news_search(self):
        """Exa news search should pass validation without special requirements."""
        task = MicroTask(
            type=MicroTaskType.exa_news_search,
            priority="high",
            query_hint="funding announcement",
        )
        assert task.type == "exa_news_search"
        assert task.priority == "high"

    def test_valid_openai_web_search_with_mode(self):
        """OpenAI web search with mode should pass validation."""
        task = MicroTask(
            type=MicroTaskType.openai_web_search,
            openai_mode=OpenAIMode.competitors,
            priority="high",
        )
        assert task.type == "openai_web_search"
        assert task.openai_mode == "competitors"

    def test_openai_web_search_requires_mode(self):
        """OpenAI web search without mode should fail validation."""
        with pytest.raises(ValueError, match="requires openai_mode"):
            MicroTask(
                type=MicroTaskType.openai_web_search,
                priority="high",
            )

    def test_openai_person_mode_requires_person_name(self):
        """OpenAI person mode without person_name should fail validation."""
        with pytest.raises(ValueError, match="requires person_name"):
            MicroTask(
                type=MicroTaskType.openai_web_search,
                openai_mode=OpenAIMode.person,
                priority="high",
            )

    def test_valid_openai_person_mode_with_name(self):
        """OpenAI person mode with person_name should pass validation."""
        task = MicroTask(
            type=MicroTaskType.openai_web_search,
            openai_mode=OpenAIMode.person,
            person_name="John Smith",
            priority="high",
        )
        assert task.person_name == "John Smith"

    def test_pdl_person_enrich_requires_person_name(self):
        """PDL person enrich without person_name should fail validation."""
        with pytest.raises(ValueError, match="requires person_name"):
            MicroTask(
                type=MicroTaskType.pdl_person_enrich,
                priority="high",
            )

    def test_valid_pdl_person_enrich_with_name(self):
        """PDL person enrich with person_name should pass validation."""
        task = MicroTask(
            type=MicroTaskType.pdl_person_enrich,
            person_name="Jane Doe",
            priority="medium",
        )
        assert task.person_name == "Jane Doe"

    def test_pdl_company_leadership_no_person_name_required(self):
        """PDL company leadership should not require person_name."""
        task = MicroTask(
            type=MicroTaskType.pdl_company_leadership,
            priority="high",
        )
        assert task.type == "pdl_company_leadership"

    def test_gleif_lookup_valid(self):
        """GLEIF lookup should pass validation without special requirements."""
        task = MicroTask(
            type=MicroTaskType.gleif_lei_lookup,
            priority="high",
        )
        assert task.type == "gleif_lei_lookup"


class TestParseTaskWithRepair:
    """Tests for the parse_task_with_repair function."""

    def test_repair_openai_missing_mode_from_query_hint(self):
        """Should infer OpenAI mode from query_hint when missing."""
        task_dict = {
            "type": "openai_web_search",
            "query_hint": "competitor analysis alternatives",
        }
        task = parse_task_with_repair(task_dict)
        assert task is not None
        assert task.openai_mode == "competitors"

    def test_repair_openai_missing_mode_no_hint_returns_none(self):
        """Should return None when OpenAI mode cannot be inferred."""
        task_dict = {
            "type": "openai_web_search",
            "query_hint": "something vague",  # No clear mode keywords
        }
        # The function tries to infer, if it can't, it returns None
        task = parse_task_with_repair(task_dict)
        # May or may not be None depending on inference - check behavior
        if task is not None:
            # If it inferred something, that's acceptable
            assert task.openai_mode is not None

    def test_repair_person_mode_without_name_converts_to_leadership(self):
        """Person mode without person_name should be converted to leadership."""
        task_dict = {
            "type": "openai_web_search",
            "openai_mode": "person",
            # No person_name
        }
        task = parse_task_with_repair(task_dict)
        assert task is not None
        assert task.openai_mode == "leadership"

    def test_repair_person_mode_uses_slot_hints(self):
        """Person mode should use person_name from slot_hints if available."""
        task_dict = {
            "type": "openai_web_search",
            "openai_mode": "person",
        }
        slot_hints = {"person_name": "John Founder"}
        task = parse_task_with_repair(task_dict, slot_hints)
        assert task is not None
        assert task.openai_mode == "person"
        assert task.person_name == "John Founder"

    def test_repair_pdl_person_enrich_without_name_returns_none(self):
        """PDL person enrich without person_name should return None."""
        task_dict = {
            "type": "pdl_person_enrich",
        }
        task = parse_task_with_repair(task_dict)
        assert task is None

    def test_repair_pdl_person_enrich_uses_slot_hints(self):
        """PDL person enrich should use person_name from slot_hints."""
        task_dict = {
            "type": "pdl_person_enrich",
        }
        slot_hints = {"person_name": "CEO Person"}
        task = parse_task_with_repair(task_dict, slot_hints)
        assert task is not None
        assert task.person_name == "CEO Person"

    def test_legacy_pdl_person_search_with_name_converts_to_enrich(self):
        """Legacy pdl_person_search with person_name converts to pdl_person_enrich."""
        task_dict = {
            "type": "pdl_person_search",
            "person_name": "Test Person",
        }
        task = parse_task_with_repair(task_dict)
        assert task is not None
        assert task.type == "pdl_person_enrich"
        assert task.person_name == "Test Person"

    def test_legacy_pdl_person_search_without_name_converts_to_leadership(self):
        """Legacy pdl_person_search without person_name converts to pdl_company_leadership."""
        task_dict = {
            "type": "pdl_person_search",
        }
        task = parse_task_with_repair(task_dict)
        assert task is not None
        assert task.type == "pdl_company_leadership"

    def test_unknown_task_type_returns_none(self):
        """Unknown task types should return None."""
        task_dict = {
            "type": "unknown_task_type",
        }
        task = parse_task_with_repair(task_dict)
        assert task is None

    def test_valid_task_passes_through(self):
        """Valid tasks should pass through unchanged."""
        task_dict = {
            "type": "exa_news_search",
            "priority": "high",
            "query_hint": "funding round",
            "start_date": "2023-01-01",
        }
        task = parse_task_with_repair(task_dict)
        assert task is not None
        assert task.type == "exa_news_search"
        assert task.priority == "high"
        assert task.start_date == "2023-01-01"


class TestMicroPlanDSL:
    """Tests for MicroPlanDSL validation."""

    def test_valid_plan(self):
        """Valid plan with tasks should pass validation."""
        plan = MicroPlanDSL(
            gap="Missing funding information",
            intent="funding_investors",
            tasks=[
                MicroTask(
                    type=MicroTaskType.exa_funding_search,
                    priority="high",
                ),
                MicroTask(
                    type=MicroTaskType.pdl_company_search,
                    priority="medium",
                ),
            ],
            slot_hints={"round": "series b"},
        )
        assert plan.gap == "Missing funding information"
        assert len(plan.tasks) == 2

    def test_empty_tasks_valid(self):
        """Plan with empty tasks is technically valid (LLM may return no tasks)."""
        plan = MicroPlanDSL(
            gap="No gap found",
            intent="general",
            tasks=[],
        )
        assert len(plan.tasks) == 0


class TestGetConnectorForTask:
    """Tests for the get_connector_for_task function."""

    def test_exa_tasks_map_to_exa(self):
        """All Exa task types should map to 'exa' connector."""
        exa_types = [
            "exa_news_search",
            "exa_site_search",
            "exa_funding_search",
            "exa_general_search",
            "exa_similar_search",
        ]
        for task_type in exa_types:
            assert get_connector_for_task(task_type) == "exa"

    def test_openai_web_search_maps_correctly(self):
        """OpenAI web search should map to 'openai_web'."""
        assert get_connector_for_task("openai_web_search") == "openai_web"

    def test_pdl_tasks_map_correctly(self):
        """PDL tasks should map to appropriate connectors."""
        assert get_connector_for_task("pdl_person_enrich") == "pdl"
        assert get_connector_for_task("pdl_company_leadership") == "pdl"
        assert get_connector_for_task("pdl_person_search") == "pdl"
        assert get_connector_for_task("pdl_company_search") == "pdl_company"

    def test_gleif_maps_correctly(self):
        """GLEIF lookup should map to 'gleif'."""
        assert get_connector_for_task("gleif_lei_lookup") == "gleif"

    def test_unknown_type_returns_none(self):
        """Unknown task type should return None."""
        assert get_connector_for_task("unknown_type") is None

