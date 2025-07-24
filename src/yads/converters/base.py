from abc import ABC, abstractmethod
from typing import Any

from yads.spec import SchemaSpec


class BaseConverter(ABC):
    """Abstract base class for spec converters.

    A converter transforms a SchemaSpec object into a target format,
    such as a sqlglot AST or a PySpark schema.
    """

    @abstractmethod
    def convert(self, spec: SchemaSpec) -> Any:
        """Converts a SchemaSpec object into the target format.

        Args:
            spec: The SchemaSpec object to convert.

        Returns:
            The converted schema in its target representation. The return type
            depends on the converter.
        """
        raise NotImplementedError
