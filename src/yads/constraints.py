from __future__ import annotations

from abc import ABC
from typing import Any


class BaseConstraint(ABC):
    """The abstract base class for all column constraints."""

    def __init__(self, name: str | None = None) -> None:
        self.name = name


class NotNullConstraint(BaseConstraint):
    """Represents a NOT NULL constraint on a column."""

    def __repr__(self) -> str:
        if self.name:
            return f"NotNullConstraint(name={self.name!r})"
        return "NotNullConstraint()"


class PrimaryKeyConstraint(BaseConstraint):
    """Represents a PRIMARY KEY constraint on a column."""

    def __repr__(self) -> str:
        if self.name:
            return f"PrimaryKeyConstraint(name={self.name!r})"
        return "PrimaryKeyConstraint()"


class DefaultConstraint(BaseConstraint):
    """Represents a DEFAULT constraint on a column."""

    def __init__(self, value: Any, *, name: str | None = None) -> None:
        super().__init__(name)
        self.value = value

    def __repr__(self) -> str:
        if self.name:
            return f"DefaultConstraint(value={self.value!r}, name={self.name!r})"
        return f"DefaultConstraint(value={self.value!r})"
