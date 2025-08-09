"""Data type definitions for yads specifications.

This module provides the canonical type system for yads, defining primitive types
(strings, numbers, dates), complex types (arrays, structs, maps), and specialized
types (intervals, UUIDs). These types form the foundation for schema definitions
and are used throughout spec conversions.

The type system is designed to be expressive and database-agnostic, while providing
sufficient detail for accurate conversion to specific SQL dialects and data processing
frameworks.

Example:
    >>> from yads.types import String, Integer, Decimal, Struct
    >>> from yads.spec import Field
    >>>
    >>> # Create basic types
    >>> name_type = String(length=100)
    >>> age_type = Integer(bits=32)
    >>>
    >>> # Create complex types
    >>> address_type = Struct(fields=[
    ...     Field(name="street", type=String()),
    ...     Field(name="city", type=String()),
    ...     Field(name="zip", type=String(length=10))
    ... ])
"""

from __future__ import annotations

import textwrap
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from yads.exceptions import TypeDefinitionError

if TYPE_CHECKING:
    from .spec import Field


__all__ = [
    "Type",
    "String",
    "Integer",
    "Float",
    "Boolean",
    "Decimal",
    "Date",
    "Timestamp",
    "TimestampTZ",
    "Binary",
    "JSON",
    "UUID",
    "Interval",
    "IntervalTimeUnit",
    "Array",
    "Struct",
    "Map",
]


class Type(ABC):
    """Abstract base class for all yads data types.

    All type definitions in yads inherit from this base class, providing
    a consistent interface for type representation and conversion across
    different target systems.
    """

    def __str__(self) -> str:
        return self.__class__.__name__.lower()


@dataclass(frozen=True)
class String(Type):
    """Variable-length string type with optional maximum length constraint.

    Represents text data that can be converted to STRING-like types in various
    SQL dialects or data-processing frameworks. The length parameter specifies
    the maximum number of characters allowed.

    Args:
        length: Maximum number of characters. If None, represents unlimited length.

    Raises:
        TypeDefinitionError: If length is not a positive integer.

    Example:
        >>> # Unlimited length string
        >>> String()

        >>> # String with maximum length
        >>> String(length=255)

        >>> # Common use in field definition
        >>> from yads.spec import Field
        >>> Field(name="username", type=String(length=50))
    """

    length: int | None = None

    def __post_init__(self):
        if self.length is not None and self.length <= 0:
            raise TypeDefinitionError(
                f"String 'length' must be a positive integer, not {self.length}."
            )

    def __str__(self) -> str:
        if self.length is not None:
            return f"string({self.length})"
        return "string"


@dataclass(frozen=True)
class Integer(Type):
    """Signed integer type with optional bit-width specification.

    Represents whole numbers that can be converted to various integer types
    in SQL dialects and data processing frameworks. The bit-width determines
    the range of values that can be stored.

    Args:
        bits: Number of bits for the integer. Must be 8, 16, 32, or 64.
              If None, uses the default integer type for the target system.

    Raises:
        TypeDefinitionError: If bits is not one of the valid values.

    Example:
        >>> # Default integer (typically 32-bit)
        >>> Integer()

        >>> # Specific bit-width integers
        >>> Integer(bits=8)   # TINYINT
        >>> Integer(bits=16)  # SMALLINT
        >>> Integer(bits=32)  # INT
        >>> Integer(bits=64)  # BIGINT
    """

    bits: int | None = None

    def __post_init__(self):
        if self.bits is not None and self.bits not in {8, 16, 32, 64}:
            raise TypeDefinitionError(
                f"Integer 'bits' must be one of 8, 16, 32, 64, not {self.bits}."
            )

    def __str__(self) -> str:
        if self.bits is not None:
            return f"integer(bits={self.bits})"
        return "integer"


@dataclass(frozen=True)
class Float(Type):
    """IEEE floating-point number type with optional precision specification.

    Represents approximate numeric values with fractional components.

    Args:
        bits: Number of bits for the float. Must be 32 or 64.
              32-bit corresponds to single precision, 64-bit to double precision.
              If None, uses the default float type for the target system.

    Raises:
        TypeDefinitionError: If bits is not 32 or 64.

    Example:
        >>> # Default float (typically 64-bit)
        >>> Float()

        >>> # Single precision
        >>> Float(bits=32)

        >>> # Double precision
        >>> Float(bits=64)
    """

    bits: int | None = None

    def __post_init__(self):
        if self.bits is not None and self.bits not in {32, 64}:
            raise TypeDefinitionError(
                f"Float 'bits' must be one of 32 or 64, not {self.bits}."
            )

    def __str__(self) -> str:
        if self.bits is not None:
            return f"float(bits={self.bits})"
        return "float"


@dataclass(frozen=True)
class Boolean(Type):
    """Boolean type representing true/false values.

    Maps to BOOLEAN types in SQL dialects or equivalent binary representations
    in data processing frameworks.

    Example:
        >>> Boolean()
        >>>
        >>> # Use in field definition
        >>> Field(name="is_active", type=Boolean())
    """


@dataclass(frozen=True)
class Decimal(Type):
    """Fixed-precision decimal type.

    Args:
        precision: Total number of digits (before and after decimal point).
        scale: Number of digits after the decimal point.

    Both precision and scale must be specified together, or both omitted
    for a default decimal type.

    Raises:
        TypeDefinitionError: If only one of precision/scale is specified,
                           or if values are invalid.

    Example:
        >>> # Default decimal
        >>> Decimal()

        >>> # Use in field definition
        >>> Decimal(precision=10, scale=2)
    """

    precision: int | None = None
    scale: int | None = None

    def __post_init__(self):
        if (self.precision is None) != (self.scale is None):
            raise TypeDefinitionError(
                "Decimal type requires both 'precision' and 'scale', or neither."
            )
        if self.precision is not None and (
            not isinstance(self.precision, int) or self.precision <= 0
        ):
            raise TypeDefinitionError(
                f"Decimal 'precision' must be a positive integer, not {self.precision}."
            )
        if self.scale is not None and (not isinstance(self.scale, int) or self.scale < 0):
            raise TypeDefinitionError(
                f"Decimal 'scale' must be a positive integer, not {self.scale}."
            )

    def __str__(self) -> str:
        if self.precision is not None and self.scale is not None:
            return f"decimal({self.precision}, {self.scale})"
        return "decimal"


@dataclass(frozen=True)
class Date(Type):
    """Calendar date type representing year, month, and day.

    Maps to DATE-like types in SQL dialects or data-processing frameworks.
    Does not include time information.

    Example:
        >>> Date()
        >>>
        >>> # Use in field definition
        >>> Field(name="birth_date", type=Date())
    """


@dataclass(frozen=True)
class Timestamp(Type):
    """Date and time type without timezone information.

    Represents a specific point in time including date and time components,
    but without timezone awareness. Maps to TIMESTAMP or DATETIME types
    in SQL dialects.

    Example:
        >>> Timestamp()
        >>>
        >>> # Use in field definition
        >>> Field(name="created_at", type=Timestamp())
    """


@dataclass(frozen=True)
class TimestampTZ(Type):
    """Date and time type with timezone information.

    Similar to Timestamp but includes timezone awareness. Maps to
    TIMESTAMP WITH TIME ZONE or equivalent types in SQL dialects.

    Example:
        >>> TimestampTZ()
        >>>
        >>> # Use in field definition
        >>> Field(name="order_time", type=TimestampTZ())
    """


@dataclass(frozen=True)
class Binary(Type):
    """Binary data type for storing byte sequences.

    Used for storing arbitrary binary data such as images, documents,
    or serialized objects. Maps to BLOB, BINARY, or VARBINARY types
    in SQL dialects.

    Example:
        >>> Binary()
        >>>
        >>> # Use in field definition
        >>> Field(name="document", type=Binary())
    """


@dataclass(frozen=True)
class JSON(Type):
    """JSON document type for semi-structured data.

    Stores JSON documents with native support for JSON operations
    in compatible databases.

    Example:
        >>> JSON()
        >>>
        >>> # Use in field definition
        >>> Field(name="metadata", type=JSON())
    """


@dataclass(frozen=True)
class UUID(Type):
    """Universally Unique Identifier type.

    Represents 128-bit UUID values, commonly used for primary keys
    and unique identifiers in distributed systems.

    Example:
        >>> UUID()
        >>>
        >>> # Use in field definition
        >>> Field(name="user_id", type=UUID())
    """


class IntervalTimeUnit(str, Enum):
    """Time unit enumeration for interval types.

    Defines the valid time units that can be used in interval type
    definitions. Units are categorized into Year-Month and Day-Time
    groups for SQL compatibility.
    """

    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"


@dataclass(frozen=True)
class Interval(Type):
    """Time interval type representing a duration between two time points.

    Intervals can represent durations like "3 months", "2 days", or
    "5 hours 30 minutes". The interval is defined by start and optionally
    end time units.

    Args:
        interval_start: The starting (most significant) time unit.
        interval_end: The ending (least significant) time unit. If None,
                     represents a single-unit interval.

    The start and end units must belong to the same category:
    - Year-Month: YEAR, MONTH
    - Day-Time: DAY, HOUR, MINUTE, SECOND

    Raises:
        TypeDefinitionError: If start and end units are from different categories,
                           or if start is less significant than end.

    Example:
        >>> # Single unit intervals
        >>> Interval(IntervalTimeUnit.YEAR)
        >>> Interval(IntervalTimeUnit.DAY)

        >>> # Range intervals
        >>> Interval(IntervalTimeUnit.YEAR, IntervalTimeUnit.MONTH)
        >>> Interval(IntervalTimeUnit.DAY, IntervalTimeUnit.SECOND)
    """

    interval_start: IntervalTimeUnit
    interval_end: IntervalTimeUnit | None = None

    def __post_init__(self):
        _YEAR_MONTH_UNITS = {IntervalTimeUnit.YEAR, IntervalTimeUnit.MONTH}
        _DAY_TIME_UNITS = {
            IntervalTimeUnit.DAY,
            IntervalTimeUnit.HOUR,
            IntervalTimeUnit.MINUTE,
            IntervalTimeUnit.SECOND,
        }

        if self.interval_end:
            in_ym_start = self.interval_start in _YEAR_MONTH_UNITS
            in_ym_end = self.interval_end in _YEAR_MONTH_UNITS

            if in_ym_start != in_ym_end:
                category_start = "Year-Month" if in_ym_start else "Day-Time"
                category_end = "Year-Month" if in_ym_end else "Day-Time"
                raise TypeDefinitionError(
                    "Invalid Interval definition: 'interval_start' and 'interval_end' must "
                    "belong to the same category (either Year-Month or Day-Time). "
                    f"Received interval_start='{self.interval_start.value}' (category: "
                    f"{category_start}) and interval_end='{self.interval_end.value}' "
                    f"(category: {category_end})."
                )

        _UNIT_ORDER_MAP = {
            "Year-Month": [IntervalTimeUnit.YEAR, IntervalTimeUnit.MONTH],
            "Day-Time": [
                IntervalTimeUnit.DAY,
                IntervalTimeUnit.HOUR,
                IntervalTimeUnit.MINUTE,
                IntervalTimeUnit.SECOND,
            ],
        }

        category = (
            "Year-Month" if self.interval_start in _YEAR_MONTH_UNITS else "Day-Time"
        )
        order = _UNIT_ORDER_MAP[category]

        if self.interval_end and self.interval_start != self.interval_end:
            start_index = order.index(self.interval_start)
            end_index = order.index(self.interval_end)
            if start_index > end_index:
                raise TypeDefinitionError(
                    "Invalid Interval definition: 'interval_start' cannot be less "
                    "significant than 'interval_end'. "
                    f"Received interval_start='{self.interval_start.value}' and "
                    f"interval_end='{self.interval_end.value}'."
                )

    def __str__(self) -> str:
        if self.interval_end and self.interval_start != self.interval_end:
            return f"interval({self.interval_start.value} to {self.interval_end.value})"
        return f"interval({self.interval_start.value})"


@dataclass(frozen=True)
class Array(Type):
    """Array type containing elements of a homogeneous type.

    Represents ordered collections where all elements share the same type.
    Maps to ARRAY types in SQL dialects or list/array structures in data
    processing frameworks.

    Args:
        element: The type of elements contained in the array.

    Example:
        >>> # Array of strings
        >>> Array(element=String())

        >>> # Array of integers
        >>> Array(element=Integer(bits=32))

        >>> # Nested array (array of arrays)
        >>> Array(element=Array(element=String()))
    """

    element: Type

    def __str__(self) -> str:
        return f"array<{self.element}>"


@dataclass(frozen=True)
class Struct(Type):
    """Structured type containing named fields of potentially different types.

    Represents complex objects with named fields. Maps to STRUCT/ROW types in
    SQL dialects or nested objects in data-processing frameworks.

    Args:
        fields: List of Field objects defining the structure's schema.

    Example:
        >>> from yads.spec import Field
        >>>
        >>> # Address structure
        >>> address_type = Struct(fields=[
        ...     Field(name="street", type=String()),
        ...     Field(name="city", type=String()),
        ...     Field(name="postal_code", type=String(length=10))
        ... ])

        >>> # Nested structures
        >>> person_type = Struct(fields=[
        ...     Field(name="name", type=String()),
        ...     Field(name="age", type=Integer()),
        ...     Field(name="address", type=address_type)
        ... ])
    """

    fields: list["Field"]

    def __str__(self) -> str:
        fields_str = ",\n".join(str(field) for field in self.fields)
        indented_fields = textwrap.indent(fields_str, "  ")
        return f"struct<\n{indented_fields}\n>"


@dataclass(frozen=True)
class Map(Type):
    """Key-value mapping type with homogeneous key and value types.

    Represents associative arrays or dictionaries where all keys share one type
    and all values share another type. Maps to MAP types in SQL dialects or
    dictionary structures in data processing frameworks.

    Args:
        key: The type of all keys in the map.
        value: The type of all values in the map.

    Example:
        >>> # String-to-string mapping
        >>> Map(key=String(), value=String())

        >>> # String-to-integer mapping
        >>> Map(key=String(), value=Integer())

        >>> # Complex value types
        >>> Map(key=String(), value=Array(element=String()))
    """

    key: Type
    value: Type

    def __str__(self) -> str:
        return f"map<{self.key}, {self.value}>"


TYPE_ALIASES: dict[str, tuple[type[Type], dict[str, Any]]] = {
    # Numeric Types
    "int8": (Integer, {"bits": 8}),
    "tinyint": (Integer, {"bits": 8}),
    "byte": (Integer, {"bits": 8}),
    "int16": (Integer, {"bits": 16}),
    "smallint": (Integer, {"bits": 16}),
    "short": (Integer, {"bits": 16}),
    "int32": (Integer, {"bits": 32}),
    "int": (Integer, {"bits": 32}),
    "integer": (Integer, {"bits": 32}),
    "int64": (Integer, {"bits": 64}),
    "bigint": (Integer, {"bits": 64}),
    "long": (Integer, {"bits": 64}),
    "float": (Float, {"bits": 32}),
    "float32": (Float, {"bits": 32}),
    "float64": (Float, {"bits": 64}),
    "double": (Float, {"bits": 64}),
    "decimal": (Decimal, {}),
    "numeric": (Decimal, {}),
    # String Types
    "string": (String, {}),
    "text": (String, {}),
    "varchar": (String, {}),
    "char": (String, {}),
    # Binary Types
    "blob": (Binary, {}),
    "binary": (Binary, {}),
    "bytes": (Binary, {}),
    # Boolean Types
    "bool": (Boolean, {}),
    "boolean": (Boolean, {}),
    # Temporal Types
    "date": (Date, {}),
    "datetime": (Timestamp, {}),
    "timestamp": (Timestamp, {}),
    "timestamp_tz": (TimestampTZ, {}),
    "interval": (Interval, {}),
    # Complex Types
    "array": (Array, {}),
    "list": (Array, {}),
    "struct": (Struct, {}),
    "record": (Struct, {}),
    "map": (Map, {}),
    "dictionary": (Map, {}),
    "json": (JSON, {}),
    # Other Types
    "uuid": (UUID, {}),
}
