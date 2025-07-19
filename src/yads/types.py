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

    pass


@dataclass(frozen=True)
class String(Type):
    length: int | None = None


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
    precision: int
    scale: int


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


@dataclass(frozen=True)
class Struct(Type):
    fields: list["Field"]


@dataclass(frozen=True)
class Map(Type):
    key: Type
    value: Type
