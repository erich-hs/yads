"""
This module provides the base class for all yads generators.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..specifications import TableSpecification


class SchemaGenerator(ABC):
    """
    Base class for all schema generators.
    """

    def __init__(self, table_spec: "TableSpecification"):
        """
        Initializes the schema generator.

        Args:
            table_spec: The table specification to translate.
        """
        self.table_spec = table_spec

    @abstractmethod
    def generate(self) -> Any:
        """
        Generates a schema representation from the specification.
        """
        raise NotImplementedError
