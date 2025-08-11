"""Base converter interface for spec transformations.

This module defines the abstract base class for all yads converters. Converters
are responsible for transforming YadsSpec objects into target formats such as
SQL DDL, framework-specific schemas or other representations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..spec import YadsSpec


class BaseConverter(ABC):
    """Abstract base class for spec converters."""

    @abstractmethod
    def convert(self, spec: YadsSpec, **kwargs: Any) -> Any:
        """Convert a YadsSpec to the target format."""
        ...
