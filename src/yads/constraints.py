from __future__ import annotations

from abc import ABC
from typing import Any


class BaseConstraint(ABC):
    """The abstract base class for all column constraints."""

    pass


class NotNullConstraint(BaseConstraint):
    """Represents a NOT NULL constraint on a column."""

    def __repr__(self) -> str:
        return "NotNullConstraint()"


class PrimaryKeyConstraint(BaseConstraint):
    """Represents a PRIMARY KEY constraint on a column."""

    def __repr__(self) -> str:
        return "PrimaryKeyConstraint()"


class DefaultConstraint(BaseConstraint):
    """Represents a DEFAULT constraint on a column."""

    def __init__(self, value: Any):
        self.value = value

    def __repr__(self) -> str:
        return f"DefaultConstraint(value={self.value!r})"
