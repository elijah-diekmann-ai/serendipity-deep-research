from __future__ import annotations

import asyncio
from typing import Any, Dict, List
import logging

from .base import BaseConnector, ConnectorResult
from .exa import ExaConnector
from .companies_house import CompaniesHouseConnector
from .apollo import ApolloConnector
from .openai_web import OpenAIWebSearchConnector
from .pdl import PDLConnector
from .pdl_company import PDLCompanyConnector
from .opencorporates import OpenCorporatesConnector
from .gleif import GLEIFConnector
from .pitchbook import PitchbookConnector

logger = logging.getLogger(__name__)


class ConnectorRunner:
    """
    Registry + executor for all connectors.

    - Instantiates each connector once per process.
    - Executes plan steps concurrently via asyncio.
    - Returns a dict[step_name] -> dict(payload) suitable for entity_resolution.
    """

    def __init__(self) -> None:
        # Connectors are intentionally modular. New connectors (like OpenAI web
        # search) can be added here without changing the orchestrator.
        self._connectors: Dict[str, BaseConnector] = {
            "exa": ExaConnector(),
            "gleif": GLEIFConnector(),
            "openai_web": OpenAIWebSearchConnector(),
            "pdl": PDLConnector(),              # People discovery (persons)
            "pdl_company": PDLCompanyConnector(),  # NEW
            # retained for future use:
            "companies_house": CompaniesHouseConnector(),
            "open_corporates": OpenCorporatesConnector(),
            "apollo": ApolloConnector(),
            "pitchbook": PitchbookConnector(),
        }

    def _get_connector(self, name: str) -> BaseConnector | None:
        return self._connectors.get(name)

    def execute_plan(self, plan: List[Dict[str, Any]], target_input: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        Execute all plan steps concurrently.

        Each plan step has shape:
            {"name": str, "connector": str, "params": dict}

        Returns:
            {step_name: dict_result}
        """

        async def _run_all() -> Dict[str, Dict[str, Any]]:
            tasks: Dict[str, asyncio.Task] = {}

            for step in plan:
                name = step.get("name") or "unnamed_step"
                connector_name = step.get("connector")
                params = step.get("params") or {}

                connector = self._get_connector(connector_name)
                if not connector:
                    logger.warning(
                        "No connector registered for '%s'; skipping step '%s'",
                        connector_name,
                        name,
                        extra={"connector": connector_name, "step": name},
                    )
                    continue

                async def _run_step(
                    step_name: str, conn: BaseConnector, step_params: Dict[str, Any]
                ) -> ConnectorResult:
                    try:
                        res = await conn.fetch(**step_params)
                        logger.info(
                            "Connector '%s' completed step '%s'",
                            conn.name,
                            step_name,
                            extra={"connector": conn.name, "step": step_name},
                        )
                        return res
                    except Exception as e:
                        logger.exception(
                            "Connector '%s' failed for step '%s': %s",
                            conn.name,
                            step_name,
                            e,
                            extra={"connector": conn.name, "step": step_name},
                        )
                        return ConnectorResult({})

                tasks[name] = asyncio.create_task(
                    _run_step(name, connector, params)
                )

            results: Dict[str, Dict[str, Any]] = {}
            for name, task in tasks.items():
                res = await task
                # ConnectorResult is dict-like
                results[name] = dict(res)

            return results

        # Use a dedicated event loop; Celery workers are synchronous
        try:
            return asyncio.run(_run_all())
        except RuntimeError:
            # Fallback: create a new loop manually
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(_run_all())
            finally:
                loop.close()


def get_connectors() -> ConnectorRunner:
    return ConnectorRunner()
