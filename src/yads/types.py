from __future__ import annotations

import textwrap
from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING

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

    def __str__(self) -> str:
        """Returns a string representation of the string type."""
        if self.length is not None:
            return f"string({self.length})"
        return "string"


@dataclass(frozen=True)
class Integer(Type):
    """An integer data type, with an optional size in bits."""

    bits: int | None = None

    def __str__(self) -> str:
        """Returns a string representation of the integer type."""
        if self.bits is not None:
            return f"integer(bits={self.bits})"
        return "integer"


@dataclass(frozen=True)
class Float(Type):
    """A float data type, with an optional size in bits."""

    bits: int | None = None

    def __str__(self) -> str:
        """Returns a string representation of the float type."""
        if self.bits is not None:
            return f"float(bits={self.bits})"
        return "float"


@dataclass(frozen=True)
class Boolean(Type):
    pass


@dataclass(frozen=True)
class Decimal(Type):
    """A decimal data type, with optional precision and scale."""

    precision: int | None = None
    scale: int | None = None

    def __str__(self) -> str:
        """Returns a string representation of the decimal type."""
        if self.precision is not None:
            if self.scale is not None:
                return f"decimal({self.precision},{self.scale})"
            return f"decimal({self.precision},)"
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


TYPE_ALIASES: dict[str, Type] = {
    # Integers
    "tinyint": Integer(bits=8),
    "smallint": Integer(bits=16),
    "int": Integer(bits=32),
    "integer": Integer(bits=32),
    "bigint": Integer(bits=64),
    # Floats
    "float": Float(bits=32),
    "real": Float(bits=32),
    "double": Float(bits=64),
    # Standard types
    "string": String(),
    "boolean": Boolean(),
    "binary": Binary(),
    "date": Date(),
    "timestamp": Timestamp(),
    "timestamp_tz": TimestampTZ(),
    "json": JSON(),
    "uuid": UUID(),
    # Complex types (defaults, can be overridden with params)
    "decimal": Decimal(),
    "array": Array(element=String()),
    "struct": Struct(fields=[]),
    "map": Map(key=String(), value=String()),
}
