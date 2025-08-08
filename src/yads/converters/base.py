"""Base converter interface for schema transformations.

This module defines the abstract base class for all yads converters. Converters
are responsible for transforming SchemaSpec objects into target formats such as
SQL DDL, framework-specific schemas (PySpark, PyArrow), or other representations.

The converter architecture allows for extensible transformation pipelines where
different converters can be composed or chained together to achieve complex
transformations while maintaining a consistent interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from yads.spec import SchemaSpec


class BaseConverter(ABC):
    """Abstract base class for schema converters.

    All converters in yads inherit from this base class, providing a consistent
    interface for transforming SchemaSpec objects into various target formats.
    This enables a pluggable architecture where different converters can be
    used interchangeably based on the desired output format.

    Converters are responsible for:
    - Transforming yads types to target type systems
    - Converting constraints to target-specific representations
    - Handling format-specific features and limitations
    - Providing appropriate error handling and validation

    Example:
        >>> class MyCustomConverter(BaseConverter):
        ...     def convert(self, spec, **kwargs):
        ...         # Custom conversion logic here
        ...         return transformed_spec
        >>>
        >>> converter = MyCustomConverter()
        >>> result = converter.convert(schema_spec)
    """

    @abstractmethod
    def convert(self, spec: SchemaSpec, **kwargs: Any) -> Any:
        """Convert a SchemaSpec to the target format.

        This method performs the core transformation from a yads SchemaSpec
        to the converter's target representation. The exact return type and
        behavior depends on the specific converter implementation.

        Args:
            spec: The SchemaSpec object to convert.
            **kwargs: Additional converter-specific options that may modify
                     the conversion behavior, output format, or validation rules.

        Returns:
            The converted schema in the target format. The specific type depends
            on the converter (e.g., str for SQL, StructType for PySpark, etc.).
        """
        ...
