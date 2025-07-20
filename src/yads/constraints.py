from __future__ import annotations

import textwrap
from typing import Any
from abc import ABC, abstractmethod
from dataclasses import dataclass


# Column Constraints
class ColumnConstraint(ABC):
    """The abstract base class for all column constraints."""


@dataclass(frozen=True)
class NotNullConstraint(ColumnConstraint):
    """Represents a NOT NULL constraint on a column."""

    def __str__(self) -> str:
        return "NotNullConstraint()"


@dataclass(frozen=True)
class PrimaryKeyConstraint(ColumnConstraint):
    """Represents a PRIMARY KEY constraint on a column."""

    def __str__(self) -> str:
        return "PrimaryKeyConstraint()"


@dataclass(frozen=True)
class DefaultConstraint(ColumnConstraint):
    """Represents a DEFAULT constraint on a column."""

    value: Any

    def __str__(self) -> str:
        return f"DefaultConstraint(value={self.value!r})"


# Table Constraints
@dataclass(frozen=True)
class TableConstraint(ABC):
    """The abstract base class for all table constraints."""

    @abstractmethod
    def get_constrained_columns(self) -> list[str]:
        """Returns the list of columns targeted by the constraint."""
        ...


@dataclass(frozen=True)
class PrimaryKeyTableConstraint(TableConstraint):
    """Represents a PRIMARY KEY constraint on a table. Can be used to defined composite primary keys."""

    columns: list[str]
    name: str | None = None

    def get_constrained_columns(self) -> list[str]:
        """Returns the list of columns constrained as a primary key."""
        return self.columns

    def __str__(self) -> str:
        parts = []
        if self.name:
            parts.append(f"name={self.name!r}")

        formatted_columns = ",\n".join(f"{col!r}" for col in self.columns)
        indented_columns = textwrap.indent(formatted_columns, "  ")
        parts.append(f"columns=[\n{indented_columns}\n]")

        pretty_parts = ",\n".join(parts)
        indented_parts = textwrap.indent(pretty_parts, "  ")
        return f"PrimaryKeyTableConstraint(\n{indented_parts}\n)"


# A mapping from column-level constraints to their table-level counterparts.
# This helps in identifying when a constraint is defined in two places.
CONSTRAINT_EQUIVALENTS: dict[type[ColumnConstraint], type[TableConstraint]] = {
    PrimaryKeyConstraint: PrimaryKeyTableConstraint
}
