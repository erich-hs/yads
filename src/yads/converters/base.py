"""Base converter interface for schema transformations.

This module defines the abstract base class for all yads converters. Converters
are responsible for transforming SchemaSpec objects into target formats such as
SQL DDL, framework-specific schemas or other representations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..spec import SchemaSpec


class BaseConverter(ABC):
    """Abstract base class for schema converters."""

    @abstractmethod
    def convert(self, spec: SchemaSpec, **kwargs: Any) -> Any:
        """Convert a SchemaSpec to the target format."""
        ...
