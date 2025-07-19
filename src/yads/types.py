from __future__ import annotations

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
    length: int | None = None

    def __str__(self) -> str:
        """Returns a string representation of the string type."""
        if self.length is not None:
            return f"string({self.length})"
        return "string"


@dataclass(frozen=True)
class Integer(Type):
    pass


@dataclass(frozen=True)
class Float(Type):
    pass


@dataclass(frozen=True)
class Boolean(Type):
    pass


@dataclass(frozen=True)
class Decimal(Type):
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
    element: Type

    def __str__(self) -> str:
        """Returns a string representation of the array type."""
        return f"array<{self.element}>"


@dataclass(frozen=True)
class Struct(Type):
    fields: list["Field"]

    def __str__(self) -> str:
        """Returns a string representation of the struct type."""
        fields_str = ", ".join(str(field) for field in self.fields)
        return f"struct<{fields_str}>"


@dataclass(frozen=True)
class Map(Type):
    key: Type
    value: Type

    def __str__(self) -> str:
        """Returns a string representation of the map type."""
        return f"map<{self.key}, {self.value}>"
