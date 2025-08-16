"""Data type definitions for yads specs.

This module provides the canonical type system for yads, defining primitive types
(strings, numbers, dates), complex types (arrays, structs, maps), and specialized
types (intervals, UUIDs). These types form the foundation for schema definitions
and are used throughout spec conversions.

The type system is designed to be expressive and database-agnostic, while providing
sufficient detail for accurate conversion to specific SQL dialects and data processing
frameworks.

Example:
    >>> import yads.types as ytypes
    >>> from yads.spec import Field
    >>>
    >>> # Create basic types
    >>> name_type = ytypes.String(length=100)
    >>> age_type = ytypes.Integer(bits=32)
    >>>
    >>> # Create complex types
    >>> address_type = ytypes.Struct(fields=[
    ...     Field(name="street", type=ytypes.String()),
    ...     Field(name="city", type=ytypes.String()),
    ...     Field(name="zip", type=ytypes.String(length=10))
    ... ])
"""

from __future__ import annotations

import textwrap
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from .exceptions import TypeDefinitionError

if TYPE_CHECKING:
    from .spec import Field


__all__ = [
    "YadsType",
    "String",
    "Integer",
    "Float",
    "Decimal",
    "Boolean",
    "Binary",
    "Date",
    "TimeUnit",
    "Time",
    "Timestamp",
    "TimestampTZ",
    "TimestampLTZ",
    "TimestampNTZ",
    "Duration",
    "IntervalTimeUnit",
    "Interval",
    "Array",
    "Struct",
    "Map",
    "JSON",
    "Geometry",
    "Geography",
    "UUID",
    "Void",
    "Variant",
]


def _format_type_str(type_name: str, params: list[tuple[str, Any]]) -> str:
    """Render a consistent named-parameter string for a type.

    Only parameters with non-None values are emitted. Values are rendered
    without quotes to match existing formatting expectations for identifiers
    like units or timezones (e.g., unit=ns, tz=UTC).
    """
    filtered = [(k, v) for k, v in params if v is not None]
    if not filtered:
        return type_name

    def _render_value(value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        return value

    inner = ", ".join(f"{k}={_render_value(v)}" for k, v in filtered)
    return f"{type_name}({inner})"


class YadsType(ABC):
    """Abstract base class for all yads data types.

    All type definitions in yads inherit from this base class, providing
    a consistent interface for type representation and conversion across
    different target systems.
    """

    def __str__(self) -> str:
        return self.__class__.__name__.lower()


@dataclass(frozen=True)
class String(YadsType):
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
        return _format_type_str("string", [("length", self.length)])


@dataclass(frozen=True)
class Integer(YadsType):
    """Integer type with optional bit-width and signedness specification.

    Represents whole numbers that can be converted to various integer types
    in SQL dialects and data processing frameworks. The bit-width determines
    the range of values that can be stored. The `signed` flag controls
    whether values are signed or unsigned.

    Args:
        bits: Number of bits for the integer. Must be 8, 16, 32, or 64.
            If None, uses the default integer type for the target system.
        signed: Whether the integer is signed. Defaults to True.

    Raises:
        TypeDefinitionError: If `bits` is not one of the valid values or
            if `signed` is not a boolean.

    Example:
        >>> # Default integer
        >>> Integer()

        >>> # Specific bit-width integers
        >>> Integer(bits=8)   # TINYINT
        >>> Integer(bits=16)  # SMALLINT
        >>> Integer(bits=32)  # INT
        >>> Integer(bits=64)  # BIGINT

        >>> # Unsigned integer
        >>> Integer(bits=32, signed=False)
    """

    bits: int | None = None
    signed: bool = True

    def __post_init__(self):
        if self.bits is not None and self.bits not in {8, 16, 32, 64}:
            raise TypeDefinitionError(
                f"Integer 'bits' must be one of 8, 16, 32, 64, not {self.bits}."
            )
        if not isinstance(self.signed, bool):
            raise TypeDefinitionError("Integer 'signed' must be a boolean.")

    def __str__(self) -> str:
        # Only render 'signed' when it differs from the default (False)
        if self.bits is not None and self.signed is False:
            return f"integer(bits={self.bits}, signed=False)"
        if self.bits is not None:
            return f"integer(bits={self.bits})"
        if self.signed is False:
            return "integer(signed=False)"
        return "integer"


@dataclass(frozen=True)
class Float(YadsType):
    """IEEE floating-point number type with optional precision specification.

    Represents approximate numeric values with fractional components.

    Args:
        bits: Number of bits for the float. Must be 16, 32, or 64.
              16-bit corresponds to half precision, 32-bit to single precision,
              and 64-bit to double precision. If None, uses the default float
              type for the target system.

    Raises:
        TypeDefinitionError: If bits is not 16, 32, or 64.

    Example:
        >>> # Default float (typically 64-bit)
        >>> Float()

        >>> # Half precision
        >>> Float(bits=16)

        >>> # Single precision
        >>> Float(bits=32)

        >>> # Double precision
        >>> Float(bits=64)
    """

    bits: int | None = None

    def __post_init__(self):
        if self.bits is not None and self.bits not in {16, 32, 64}:
            raise TypeDefinitionError(
                f"Float 'bits' must be one of 16, 32, or 64, not {self.bits}."
            )

    def __str__(self) -> str:
        return _format_type_str("float", [("bits", self.bits)])


@dataclass(frozen=True)
class Decimal(YadsType):
    """Fixed-precision decimal type.

    Args:
        precision: Total number of digits (before and after decimal point).
        scale: Number of digits after the decimal point. Can be negative to
            indicate rounding to the left of the decimal point.
        bits: Decimal arithmetic/storage width. One of `128` or `256`.
            Defaults to `None` (unspecified). Compatibility between bit width
            and precision is delegated to target converters.

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
    bits: int | None = None

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
        if self.scale is not None and (not isinstance(self.scale, int)):
            raise TypeDefinitionError(
                f"Decimal 'scale' must be an integer, not {self.scale}."
            )
        if self.bits is not None and self.bits not in {128, 256}:
            raise TypeDefinitionError(
                f"Decimal 'bits' must be one of 128 or 256, not {self.bits}."
            )

    def __str__(self) -> str:
        if self.precision is not None and self.scale is not None:
            return _format_type_str(
                "decimal",
                [
                    ("precision", self.precision),
                    ("scale", self.scale),
                    ("bits", self.bits),
                ],
            )
        return _format_type_str("decimal", [("bits", self.bits)])


@dataclass(frozen=True)
class Boolean(YadsType):
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
class Binary(YadsType):
    """Binary data type for storing byte sequences.

    Used for storing arbitrary binary data such as images, documents,
    or serialized objects. Maps to BLOB, BINARY, or VARBINARY types
    in SQL dialects.

    Args:
        length: Optional maximum number of bytes. If None, represents
            variable-length binary.

    Raises:
        TypeDefinitionError: If `length` is provided and is not a
            positive integer.

    Example:
        >>> Binary()
        >>>
        >>> # Use in field definition
        >>> Field(name="document", type=Binary())
        >>> Field(name="hash", type=Binary(length=32))
    """

    length: int | None = None

    def __post_init__(self):
        if self.length is not None and self.length <= 0:
            raise TypeDefinitionError(
                f"Binary 'length' must be a positive integer, not {self.length}."
            )

    def __str__(self) -> str:
        return _format_type_str("binary", [("length", self.length)])


@dataclass(frozen=True)
class Date(YadsType):
    """Calendar date type representing year, month, and day.

    Maps to DATE-like types in SQL dialects or data-processing frameworks.
    Does not include time information.

    Args:
        bits: Storage width for logical date. One of `32` or `64`. Defaults
            to `32`. This flag primarily affects non-SQL targets such as
            PyArrow.

    Example:
        >>> Date()
        >>> Date(bits=64)
        >>>
        >>> # Use in field definition
        >>> Field(name="birth_date", type=Date())
    """

    bits: int | None = None

    def __post_init__(self):
        if self.bits is not None and self.bits not in {32, 64}:
            raise TypeDefinitionError(
                f"Date 'bits' must be one of 32 or 64, not {self.bits}."
            )

    def __str__(self) -> str:
        return _format_type_str("date", [("bits", self.bits)])


class TimeUnit(str, Enum):
    """Granularity for logical time and timestamps.

    Order reflects increasing coarseness: ns < us < ms < s.
    """

    NS = "ns"
    US = "us"
    MS = "ms"
    S = "s"


@dataclass(frozen=True)
class Time(YadsType):
    """Time-of-day type with fractional precision.

    Represents a wall-clock time without a date component. Precision is expressed
    via a time unit granularity.

    Args:
        unit: Smallest time unit for values. One of `"s"`, `"ms"`, `"us"`,
            or `"ns"`. Defaults to `"ms"`.
        bits: Storage width for logical time. One of `32` or `64`.
            Defaults to `None`.

    Raises:
        TypeDefinitionError: If `unit` is not one of the supported values.

    Example:
        >>> Time()              # defaults to milliseconds
        >>> Time(unit="s")
        >>> Time(unit="ns", bits=64)
    """

    unit: TimeUnit | None = TimeUnit.MS
    bits: int | None = None

    def __post_init__(self):
        if not isinstance(self.unit, TimeUnit):
            allowed = {u.value for u in TimeUnit}
            raise TypeDefinitionError(
                f"Time 'unit' must be one of {allowed}, not {self.unit}."
            )
        if self.bits is not None and self.bits not in {32, 64}:
            raise TypeDefinitionError(
                f"Time 'bits' must be one of 32 or 64, not {self.bits}."
            )
        # Enforce unit compatibility with bit width
        # Unit-to-bits compatibility is enforced in specific converters where
        # the target system imposes restrictions (e.g., PyArrow time32/time64).

    def __str__(self) -> str:
        return _format_type_str(
            "time",
            [
                (
                    "unit",
                    self.unit.value if isinstance(self.unit, TimeUnit) else self.unit,
                ),
                ("bits", self.bits),
            ],
        )


@dataclass(frozen=True)
class Timestamp(YadsType):
    """Date and time type without timezone information.

    Represents a specific point in time including date and time components,
    with implicit timezone awareness (dependant on the target SQL dialect).
    Maps to TIMESTAMP or DATETIME types in SQL dialects.

    Args:
        unit: Smallest time unit for values. One of `"s"`, `"ms"`, `"us"`,
            or `"ns"`. Defaults to `"ns"`.

    Example:
        >>> Timestamp()
        >>> Timestamp(unit="ms")
        >>>
        >>> # Use in field definition
        >>> Field(name="created_at", type=Timestamp())
    """

    unit: TimeUnit | None = TimeUnit.NS

    def __post_init__(self):
        if not isinstance(self.unit, TimeUnit):
            allowed = {u.value for u in TimeUnit}
            raise TypeDefinitionError(
                f"Timestamp 'unit' must be one of {allowed}, not {self.unit}."
            )

    def __str__(self) -> str:
        return _format_type_str(
            "timestamp",
            [("unit", self.unit.value if isinstance(self.unit, TimeUnit) else self.unit)],
        )


@dataclass(frozen=True)
class TimestampTZ(YadsType):
    """Date and time type with timezone information.

    Similar to Timestamp but includes timezone awareness. Maps to
    TIMESTAMP WITH TIME ZONE or equivalent types in SQL dialects.

    Args:
        unit: Smallest time unit for values. One of `"s"`, `"ms"`, `"us"`,
            or `"ns"`. Defaults to `"ns"`.

    Example:
        >>> TimestampTZ()
        >>> TimestampTZ(unit="us")
        >>>
        >>> # Use in field definition
        >>> Field(name="order_time", type=TimestampTZ())
    """

    unit: TimeUnit | None = TimeUnit.NS

    def __post_init__(self):
        if not isinstance(self.unit, TimeUnit):
            allowed = {u.value for u in TimeUnit}
            raise TypeDefinitionError(
                f"TimestampTZ 'unit' must be one of {allowed}, not {self.unit}."
            )

    def __str__(self) -> str:
        return _format_type_str(
            "timestamptz",
            [("unit", self.unit.value if isinstance(self.unit, TimeUnit) else self.unit)],
        )


@dataclass(frozen=True)
class TimestampLTZ(YadsType):
    """Date and time type with local timezone information.

    Similar to Timestamp but includes local timezone awareness. Maps to
    TIMESTAMP WITH TIME ZONE or equivalent types in SQL dialects.

    Args:
        unit: Smallest time unit for values. One of `"s"`, `"ms"`, `"us"`,
            or `"ns"`. Defaults to `"ns"`.
        tz: IANA timezone name to interpret local-time values. Defaults to
            `"UTC"`. Must not be None. If timezone handling is not desired,
            use `Timestamp` or `TimestampNTZ` instead.

    Example:
        >>> TimestampLTZ()
        >>> TimestampLTZ(unit="s")
        >>> TimestampLTZ(unit="ns", tz="America/New_York")
        >>>
        >>> # Use in field definition
        >>> Field(name="order_time", type=TimestampLTZ())
    """

    unit: TimeUnit | None = TimeUnit.NS
    tz: str = "UTC"

    def __post_init__(self):
        if not isinstance(self.unit, TimeUnit):
            allowed = {u.value for u in TimeUnit}
            raise TypeDefinitionError(
                f"TimestampLTZ 'unit' must be one of {allowed}, not {self.unit}."
            )
        if self.tz is None:  # type: ignore[unreachable]
            raise TypeDefinitionError(
                "TimestampLTZ 'tz' must not be None. Use Timestamp or TimestampNTZ for no timezone."
            )
        if isinstance(self.tz, str) and not self.tz:
            raise TypeDefinitionError("TimestampLTZ 'tz' must be a non-empty string.")

    def __str__(self) -> str:
        return _format_type_str(
            "timestampltz",
            [
                (
                    "unit",
                    self.unit.value if isinstance(self.unit, TimeUnit) else self.unit,
                ),
                ("tz", self.tz),
            ],
        )


@dataclass(frozen=True)
class TimestampNTZ(YadsType):
    """Date and time type without timezone information.

    Similar to Timestamp but without timezone awareness. Maps to
    TIMESTAMP or DATETIME types in SQL dialects.

    Args:
        unit: Smallest time unit for values. One of `"s"`, `"ms"`, `"us"`,
            or `"ns"`. Defaults to `"ns"`.

    Example:
        >>> TimestampNTZ()
        >>> TimestampNTZ(unit="ms")
        >>>
        >>> # Use in field definition
        >>> Field(name="order_time", type=TimestampNTZ())
    """

    unit: TimeUnit | None = TimeUnit.NS

    def __post_init__(self):
        if not isinstance(self.unit, TimeUnit):
            allowed = {u.value for u in TimeUnit}
            raise TypeDefinitionError(
                f"TimestampNTZ 'unit' must be one of {allowed}, not {self.unit}."
            )

    def __str__(self) -> str:
        return _format_type_str(
            "timestampntz",
            [("unit", self.unit.value if isinstance(self.unit, TimeUnit) else self.unit)],
        )


@dataclass(frozen=True)
class Duration(YadsType):
    """Logical duration type with fractional precision.

    Represents an elapsed amount of time. Precision is expressed via a unit
    granularity.

    Args:
        unit: Smallest time unit for values. One of `"s"`, `"ms"`, `"us"`,
            or `"ns"`. Defaults to `"ns"`.

    Raises:
        TypeDefinitionError: If `unit` is not one of the supported values.

    Example:
        >>> Duration()
        >>> Duration(unit="ms")
    """

    unit: TimeUnit | None = TimeUnit.NS

    def __post_init__(self):
        if not isinstance(self.unit, TimeUnit):
            allowed = {u.value for u in TimeUnit}
            raise TypeDefinitionError(
                f"Duration 'unit' must be one of {allowed}, not {self.unit}."
            )

    def __str__(self) -> str:
        return _format_type_str(
            "duration",
            [("unit", self.unit.value if isinstance(self.unit, TimeUnit) else self.unit)],
        )


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
class Interval(YadsType):
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
        _UNIT_ORDER_MAP = {
            "Year-Month": [IntervalTimeUnit.YEAR, IntervalTimeUnit.MONTH],
            "Day-Time": [
                IntervalTimeUnit.DAY,
                IntervalTimeUnit.HOUR,
                IntervalTimeUnit.MINUTE,
                IntervalTimeUnit.SECOND,
            ],
        }

        if self.interval_end:
            in_ym_start = self.interval_start in _UNIT_ORDER_MAP["Year-Month"]
            in_ym_end = self.interval_end in _UNIT_ORDER_MAP["Year-Month"]

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

        category = (
            "Year-Month"
            if self.interval_start in _UNIT_ORDER_MAP["Year-Month"]
            else "Day-Time"
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
            return _format_type_str(
                "interval",
                [
                    ("interval_start", self.interval_start.value),
                    ("interval_end", self.interval_end.value),
                ],
            )
        return _format_type_str(
            "interval", [("interval_start", self.interval_start.value)]
        )


@dataclass(frozen=True)
class Array(YadsType):
    """Array type containing elements of a homogeneous type.

    Represents ordered collections where all elements share the same type.
    Maps to ARRAY types in SQL dialects or list/array structures in data
    processing frameworks.

    Args:
        element: The type of elements contained in the array.
        size: Optional maximum size for fixed-size arrays. If None, the
            array is variable-length.

    Example:
        >>> # Array of strings
        >>> Array(element=String())

        >>> # Array of integers
        >>> Array(element=Integer(bits=32))

        >>> # Nested array (array of arrays)
        >>> Array(element=Array(element=String()))
    """

    element: YadsType
    size: int | None = None

    def __str__(self) -> str:
        if self.size is not None:
            return f"array<{self.element}, size={self.size}>"
        return f"array<{self.element}>"


@dataclass(frozen=True)
class Struct(YadsType):
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

    fields: list[Field]

    def __str__(self) -> str:
        fields_str = ",\n".join(str(field) for field in self.fields)
        indented_fields = textwrap.indent(fields_str, "  ")
        return f"struct<\n{indented_fields}\n>"


@dataclass(frozen=True)
class Map(YadsType):
    """Key-value mapping type with homogeneous key and value types.

    Represents associative arrays or dictionaries where all keys share one type
    and all values share another type. Maps to MAP types in SQL dialects or
    dictionary structures in data processing frameworks.

    Args:
        key: The type of all keys in the map.
        value: The type of all values in the map.
        ordered: Whether the map has ordered keys. Defaults to False.

    Example:
        >>> # String-to-string mapping
        >>> Map(key=String(), value=String())

        >>> # String-to-integer mapping
        >>> Map(key=String(), value=Integer())

        >>> # Complex value types
        >>> Map(key=String(), value=Array(element=String()))
    """

    key: YadsType
    value: YadsType
    ordered: bool = False

    def __str__(self) -> str:
        if self.ordered:
            return f"map<{self.key}, {self.value}, ordered=True>"
        return f"map<{self.key}, {self.value}>"


@dataclass(frozen=True)
class JSON(YadsType):
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
class Geometry(YadsType):
    """Geometric object type with optional SRID.

    Represents planar geometry values such as points, linestrings, and polygons.

    Args:
        srid: Spatial reference identifier, for example an integer code or
            the string `"ANY"`. If `None`, no SRID is rendered.

    Examples:
        >>> Geometry()
        >>> Geometry(srid=0)
        >>> Geometry(srid="ANY")

    """

    srid: int | str | None = None

    def __str__(self) -> str:
        if self.srid is None:
            return "geometry"
        return _format_type_str("geometry", [("srid", self.srid)])


@dataclass(frozen=True)
class Geography(YadsType):
    """Geographic object type with optional SRID.

    Represents geographic values in a spherical coordinate system.

    Args:
        srid: Spatial reference identifier, e.g., integer code or the string
            `"ANY"`. If `None`, no SRID is rendered.

    Examples:
        >>> Geography()
        >>> Geography(srid=4326)
        >>> Geography(srid="ANY")

    """

    srid: int | str | None = None

    def __str__(self) -> str:
        if self.srid is None:
            return "geography"
        return _format_type_str("geography", [("srid", self.srid)])


@dataclass(frozen=True)
class UUID(YadsType):
    """Universally Unique Identifier type.

    Represents 128-bit UUID values, commonly used for primary keys
    and unique identifiers in distributed systems.

    Example:
        >>> UUID()
        >>>
        >>> # Use in field definition
        >>> Field(name="user_id", type=UUID())
    """


@dataclass(frozen=True)
class Void(YadsType):
    """Represents a NULL or VOID type.

    Example:
        >>> Void()
        >>>
        >>> # Use in field definition
        >>> Field(name="optional_field", type=Void())
    """


@dataclass(frozen=True)
class Variant(YadsType):
    """Variant type representing a union of potentially different types.

    Represents a value that can be one of several types. Maps to VARIANT types in SQL dialects.

    Example:
        >>> Variant(types=[String(), Integer()])
        >>> Variant(types=[String(), Integer(), Float()])
    """


TYPE_ALIASES: dict[str, tuple[type[YadsType], dict[str, Any]]] = {
    # String Types
    "string": (String, {}),
    "text": (String, {}),
    "varchar": (String, {}),
    "char": (String, {}),
    # Numeric Types
    "int8": (Integer, {"bits": 8}),
    "uint8": (Integer, {"bits": 8, "signed": False}),
    "tinyint": (Integer, {"bits": 8}),
    "byte": (Integer, {"bits": 8}),
    "int16": (Integer, {"bits": 16}),
    "uint16": (Integer, {"bits": 16, "signed": False}),
    "smallint": (Integer, {"bits": 16}),
    "short": (Integer, {"bits": 16}),
    "int32": (Integer, {"bits": 32}),
    "uint32": (Integer, {"bits": 32, "signed": False}),
    "int": (Integer, {"bits": 32}),
    "integer": (Integer, {"bits": 32}),
    "int64": (Integer, {"bits": 64}),
    "uint64": (Integer, {"bits": 64, "signed": False}),
    "bigint": (Integer, {"bits": 64}),
    "long": (Integer, {"bits": 64}),
    "float16": (Float, {"bits": 16}),
    "float": (Float, {"bits": 32}),
    "float32": (Float, {"bits": 32}),
    "float64": (Float, {"bits": 64}),
    "double": (Float, {"bits": 64}),
    "decimal": (Decimal, {}),
    "numeric": (Decimal, {}),
    # Boolean Types
    "bool": (Boolean, {}),
    "boolean": (Boolean, {}),
    # Binary Types
    "blob": (Binary, {}),
    "binary": (Binary, {}),
    "bytes": (Binary, {}),
    # Temporal Types
    "date": (Date, {}),
    "date32": (Date, {"bits": 32}),
    "date64": (Date, {"bits": 64}),
    "time": (Time, {}),
    "time32": (Time, {"bits": 32, "unit": TimeUnit.MS}),
    "time64": (Time, {"bits": 64, "unit": TimeUnit.NS}),
    "datetime": (Timestamp, {}),
    "timestamp": (Timestamp, {}),
    "timestamptz": (TimestampTZ, {}),
    "timestamp_tz": (TimestampTZ, {}),
    "timestampltz": (TimestampLTZ, {}),
    "timestamp_ltz": (TimestampLTZ, {}),
    "timestampntz": (TimestampNTZ, {}),
    "timestamp_ntz": (TimestampNTZ, {}),
    "duration": (Duration, {}),
    "interval": (Interval, {}),
    # Complex Types
    "array": (Array, {}),
    "list": (Array, {}),
    "struct": (Struct, {}),
    "record": (Struct, {}),
    "map": (Map, {}),
    "dictionary": (Map, {}),
    "json": (JSON, {}),
    # Spatial Types
    "geometry": (Geometry, {}),
    "geography": (Geography, {}),
    # Other Types
    "uuid": (UUID, {}),
    "void": (Void, {}),
    "null": (Void, {}),
    "variant": (Variant, {}),
}
