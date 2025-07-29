from __future__ import annotations

import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Reference:
    """Represents a reference for a foreign key constraint."""

    table: str
    columns: list[str] | None = None

    def __post_init__(self):
        if self.columns == []:
            raise ValueError("Reference 'columns' cannot be an empty list.")

    def __str__(self) -> str:
        if self.columns:
            return f"{self.table}({', '.join(self.columns)})"
        return self.table


# Column Constraints
class ColumnConstraint(ABC):
    """Abstract base class for all column-level constraints."""


@dataclass(frozen=True)
class NotNullConstraint(ColumnConstraint):
    def __str__(self) -> str:
        return "NotNullConstraint()"


@dataclass(frozen=True)
class PrimaryKeyConstraint(ColumnConstraint):
    def __str__(self) -> str:
        return "PrimaryKeyConstraint()"


@dataclass(frozen=True)
class DefaultConstraint(ColumnConstraint):
    value: Any

    def __str__(self) -> str:
        return f"DefaultConstraint(value={self.value!r})"


@dataclass(frozen=True)
class ForeignKeyConstraint(ColumnConstraint):
    references: Reference
    name: str | None = None

    def __str__(self) -> str:
        parts = []
        if self.name:
            parts.append(f"name={self.name!r}")
        parts.append(f"references={self.references}")

        pretty_parts = ", ".join(parts)
        return f"ForeignKeyConstraint({pretty_parts})"


@dataclass(frozen=True)
class IdentityConstraint(ColumnConstraint):
    always: bool = True
    start: int | None = None
    increment: int | None = None

    def __post_init__(self):
        if self.increment == 0:
            raise ValueError("Identity 'increment' cannot be zero.")


# Table Constraints
@dataclass(frozen=True)
class TableConstraint(ABC):
    @abstractmethod
    def get_constrained_columns(self) -> list[str]: ...


@dataclass(frozen=True)
class PrimaryKeyTableConstraint(TableConstraint):
    columns: list[str]
    name: str | None = None

    def __post_init__(self):
        if not self.columns:
            raise ValueError("PrimaryKeyTableConstraint 'columns' cannot be empty.")

    def get_constrained_columns(self) -> list[str]:
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


@dataclass(frozen=True)
class ForeignKeyTableConstraint(TableConstraint):
    columns: list[str]
    references: Reference
    name: str | None = None

    def __post_init__(self):
        if not self.columns:
            raise ValueError("ForeignKeyTableConstraint 'columns' cannot be empty.")
        if self.references.columns and len(self.columns) != len(
            self.references.columns
        ):
            raise ValueError(
                "The number of columns in the foreign key must match the number of "
                "referenced columns."
            )

    def get_constrained_columns(self) -> list[str]:
        return self.columns

    def __str__(self) -> str:
        parts = []
        if self.name:
            parts.append(f"name={self.name!r}")

        formatted_columns = ",\n".join(f"{col!r}" for col in self.columns)
        indented_columns = textwrap.indent(formatted_columns, "  ")
        parts.append(f"columns=[\n{indented_columns}\n]")
        parts.append(f"references={self.references}")

        pretty_parts = ",\n".join(parts)
        indented_parts = textwrap.indent(pretty_parts, "  ")
        return f"ForeignKeyTableConstraint(\n{indented_parts}\n)"


# A mapping from column-level constraints to their table-level counterparts.
# This helps in identifying when a constraint is defined in two places.
CONSTRAINT_EQUIVALENTS: dict[type[ColumnConstraint], type[TableConstraint]] = {
    PrimaryKeyConstraint: PrimaryKeyTableConstraint,
    ForeignKeyConstraint: ForeignKeyTableConstraint,
}
