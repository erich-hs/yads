from abc import ABC, abstractmethod
from typing import Any

from yads.spec import SchemaSpec


class BaseConverter(ABC):
    """
    Abstract base class for all spec converters.

    A converter's responsibility is to transform a SchemaSpec object into
    a target high-level format.
    """

    @abstractmethod
    def convert(self, spec: SchemaSpec) -> Any:
        """
        Converts a SchemaSpec object into the target format.

        Args:
            spec: The SchemaSpec object to convert.

        Returns:
            The converted schema in its target representation. The exact type
            of the return value will depend on the specific converter
            (e.g., a string for SQL DDL, a type for Pydantic).
        """
        raise NotImplementedError
