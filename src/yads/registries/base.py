"""Abstract registry interface for schema storage and retrieval."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from yads.spec import SchemaSpec


class BaseRegistry(ABC):
    """Abstract base class for schema registry implementations."""

    @abstractmethod
    def register_schema(self, spec: SchemaSpec) -> str:
        """Persist a schema specification and return its identifier."""
        raise NotImplementedError

    @abstractmethod
    def get_schema(self, name: str, version: str) -> SchemaSpec:
        """Retrieve a specific version of a schema specification."""
        raise NotImplementedError
