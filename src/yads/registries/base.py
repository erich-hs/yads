"""Abstract registry interface for spec storage and retrieval."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..spec import YadsSpec


class BaseRegistry(ABC):
    """Abstract base class for spec registry implementations."""

    @abstractmethod
    def register_spec(self, spec: YadsSpec) -> str:
        """Persist a spec and return its identifier."""
        ...

    @abstractmethod
    def get_spec(self, name: str, version: str) -> YadsSpec:
        """Retrieve a specific version of a spec."""
        ...
