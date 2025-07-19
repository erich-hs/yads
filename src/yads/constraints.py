from __future__ import annotations

from abc import ABC


class BaseConstraint(ABC):
    """The abstract base class for all column constraints."""

    pass


class NotNullConstraint(BaseConstraint):
    """Represents a NOT NULL constraint on a column."""

    def __repr__(self) -> str:
        return "NotNullConstraint()"
