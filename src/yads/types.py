from __future__ import annotations

import textwrap
from abc import ABC
from dataclasses import dataclass
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
    "Array",
    "Struct",
    "Map",
]


class Type(ABC):
    """The abstract base class for all canonical types."""

    def __str__(self) -> str:
        """Returns a string representation of the type."""
        return self.__class__.__name__.lower()


@dataclass(frozen=True)
class String(Type):
    """A string data type, with an optional length."""

    length: int | None = None

    def __post_init__(self):
        if self.length is not None and self.length <= 0:
            raise ValueError("String 'length' must be a positive integer.")

    def __str__(self) -> str:
        """Returns a string representation of the string type."""
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
        """Returns a string representation of the integer type."""
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


@dataclass(frozen=True)
class Array(Type):
    """An array data type, composed of elements of another type."""

    element: Type

    def __str__(self) -> str:
        """Returns a string representation of the array type."""
        return f"array<{self.element}>"


@dataclass(frozen=True)
class Struct(Type):
    """A struct data type, composed of a list of named and typed fields."""

    fields: list["Field"]

    def __str__(self) -> str:
        """Returns a string representation of the struct type."""
        fields_str = ",\n".join(str(field) for field in self.fields)
        indented_fields = textwrap.indent(fields_str, "  ")
        return f"struct<\n{indented_fields}\n>"


@dataclass(frozen=True)
class Map(Type):
    """A map data type, composed of a key type and a value type."""

    key: Type
    value: Type

    def __str__(self) -> str:
        """Returns a string representation of the map type."""
        return f"map<{self.key}, {self.value}>"


# A map from common type names to yads types.
TYPE_ALIASES: dict[str, type[Type]] = {
    "decimal": Decimal,
    "numeric": Decimal,
    "integer": Integer,
    "int": Integer,
    "float": Float,
    "boolean": Boolean,
    "bool": Boolean,
    "string": String,
    "text": String,
    "varchar": String,
    "char": String,
    "binary": Binary,
    "bytes": Binary,
    "json": JSON,
    "date": Date,
    "datetime": Timestamp,
    "timestamp": Timestamp,
    "timestamp_tz": TimestampTZ,
    "array": Array,
    "list": Array,
    "struct": Struct,
    "record": Struct,
    "map": Map,
    "dictionary": Map,
    "uuid": UUID,
}

# A map from type names with implicit parameters (e.g. `int32`) to a yads type
# and its parameters. This allows for more concise spec definitions.
PARAMETRIZED_TYPE_ALIASES: dict[str, tuple[type[Type], dict[str, Any]]] = {
    "int8": (Integer, {"bits": 8}),
    "tinyint": (Integer, {"bits": 8}),
    "int16": (Integer, {"bits": 16}),
    "smallint": (Integer, {"bits": 16}),
    "int32": (Integer, {"bits": 32}),
    "int64": (Integer, {"bits": 64}),
    "bigint": (Integer, {"bits": 64}),
    "float32": (Float, {"bits": 32}),
    "float64": (Float, {"bits": 64}),
    "double": (Float, {"bits": 64}),
}
