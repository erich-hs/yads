from __future__ import annotations

import textwrap
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

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
    "IntervalUnit",
    "Array",
    "Struct",
    "Map",
]


class Type(ABC):
    """The abstract base class for all canonical types."""

    def __str__(self) -> str:
        return self.__class__.__name__.lower()


@dataclass(frozen=True)
class String(Type):
    """A string data type, with an optional length."""

    length: int | None = None

    def __post_init__(self):
        if self.length is not None and self.length <= 0:
            raise ValueError("String 'length' must be a positive integer.")

    def __str__(self) -> str:
        if self.length is not None:
            return f"string({self.length})"
        return "string"


@dataclass(frozen=True)
class Integer(Type):
    """An integer data type, with an optional size in bits."""

    bits: int | None = None

    def __post_init__(self):
        if self.bits is not None and self.bits not in {8, 16, 32, 64}:
            raise ValueError(
                f"Integer 'bits' must be one of 8, 16, 32, 64, not {self.bits}."
            )

    def __str__(self) -> str:
        if self.bits is not None:
            return f"integer(bits={self.bits})"
        return "integer"


@dataclass(frozen=True)
class Float(Type):
    """A floating-point number type."""

    bits: int | None = None

    def __post_init__(self):
        if self.bits is not None and self.bits not in {32, 64}:
            raise ValueError(f"Float 'bits' must be one of 32 or 64, not {self.bits}.")

    def __str__(self) -> str:
        if self.bits is not None:
            return f"float(bits={self.bits})"
        return "float"


@dataclass(frozen=True)
class Boolean(Type):
    pass


@dataclass(frozen=True)
class Decimal(Type):
    """A decimal type with optional precision and scale."""

    precision: int | None = None
    scale: int | None = None

    def __post_init__(self):
        if (self.precision is None) != (self.scale is None):
            raise ValueError(
                "Decimal type requires both 'precision' and 'scale', or neither."
            )

    def __str__(self) -> str:
        if self.precision is not None and self.scale is not None:
            return f"decimal({self.precision}, {self.scale})"
        return "decimal"


@dataclass(frozen=True)
class Date(Type):
    pass


@dataclass(frozen=True)
class Timestamp(Type):
    pass


@dataclass(frozen=True)
class TimestampTZ(Type):
    pass


@dataclass(frozen=True)
class Binary(Type):
    pass


@dataclass(frozen=True)
class JSON(Type):
    pass


@dataclass(frozen=True)
class UUID(Type):
    pass


class IntervalUnit(str, Enum):
    """Enumeration for interval units."""

    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"


@dataclass(frozen=True)
class Interval(Type):
    """An interval data type, with start and end fields."""

    interval_start: IntervalUnit
    interval_end: IntervalUnit | None = None

    def __post_init__(self):
        _YEAR_MONTH_UNITS = {IntervalUnit.YEAR, IntervalUnit.MONTH}
        _DAY_TIME_UNITS = {
            IntervalUnit.DAY,
            IntervalUnit.HOUR,
            IntervalUnit.MINUTE,
            IntervalUnit.SECOND,
        }

        if self.interval_end:
            in_ym_start = self.interval_start in _YEAR_MONTH_UNITS
            in_ym_end = self.interval_end in _YEAR_MONTH_UNITS

            if in_ym_start != in_ym_end:
                category_start = "Year-Month" if in_ym_start else "Day-Time"
                category_end = "Year-Month" if in_ym_end else "Day-Time"
                raise ValueError(
                    "Invalid Interval definition: 'interval_start' and 'interval_end' must "
                    "belong to the same category (either Year-Month or Day-Time). "
                    f"Received interval_start='{self.interval_start.value}' (category: "
                    f"{category_start}) and interval_end='{self.interval_end.value}' "
                    f"(category: {category_end})."
                )

        _UNIT_ORDER_MAP = {
            "Year-Month": [IntervalUnit.YEAR, IntervalUnit.MONTH],
            "Day-Time": [
                IntervalUnit.DAY,
                IntervalUnit.HOUR,
                IntervalUnit.MINUTE,
                IntervalUnit.SECOND,
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
                raise ValueError(
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
    """An array data type, composed of elements of another type."""

    element: Type

    def __str__(self) -> str:
        return f"array<{self.element}>"


@dataclass(frozen=True)
class Struct(Type):
    """A struct data type, composed of a list of named and typed fields."""

    fields: list["Field"]

    def __str__(self) -> str:
        fields_str = ",\n".join(str(field) for field in self.fields)
        indented_fields = textwrap.indent(fields_str, "  ")
        return f"struct<\n{indented_fields}\n>"


@dataclass(frozen=True)
class Map(Type):
    """A map data type, composed of a key type and a value type."""

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
