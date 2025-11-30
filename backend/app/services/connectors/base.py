from abc import ABC, abstractmethod
from typing import Any

class ConnectorResult(dict):
    """Light wrapper, but can add metadata later."""

class BaseConnector(ABC):
    name: str

    @abstractmethod
    async def fetch(self, **kwargs) -> ConnectorResult:
        ...

