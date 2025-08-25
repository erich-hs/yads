"""Constraint definitions for data validation and spec enforcement.

This module provides constraint classes for both column-level and table-level
data validation. Constraints define rules that data must satisfy, such as
non-null requirements, primary key uniqueness, and foreign key relationships.

The constraint system supports both column-level constraints (applied to individual
columns) and table-level constraints (applied across multiple columns).

Example:
    >>> from yads.constraints import (
    ...     NotNullConstraint,
    ...     PrimaryKeyConstraint,
    ...     ForeignKeyReference,
    ...     ForeignKeyConstraint,
    ... )
    >>> from yads.spec import Field
    >>> import yads.types as ytypes
    >>>
    >>> # Column with multiple constraints
    >>> user_id_field = Field(
    ...     name="user_id",
    ...     type=ytypes.Integer(),
    ...     constraints=[
    ...         NotNullConstraint(),
    ...         PrimaryKeyConstraint()
    ...     ]
    ... )
    >>>
    >>> # Foreign key constraint
    >>> order_user_field = Field(
    ...     name="user_id",
    ...     type=ytypes.Integer(),
    ...     constraints=[
    ...         ForeignKeyConstraint(
    ...             references=ForeignKeyReference(table="users", columns=["id"])
    ...         )
    ...     ]
    ... )
"""

from __future__ import annotations

import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from .exceptions import InvalidConstraintError


@dataclass(frozen=True)
class ForeignKeyReference:
    """Reference specification for foreign key constraints.

    Defines the target table and optional column list for a foreign key
    relationship. Used by both column-level and table-level foreign key
    constraints to specify what the constraint references.

    Args:
        table: Name of the referenced table.
        columns: List of column names in the referenced table.

    Raises:
        InvalidConstraintError: If columns is an empty list.

    Example:
        >>> # Reference primary key of users table
        >>> ForeignKeyReference(table="users")

        >>> # Reference specific columns
        >>> ForeignKeyReference(table="users", columns=["id"])

        >>> # Multi-column reference
        >>> ForeignKeyReference(table="orders", columns=["order_id", "line_number"])
    """

    table: str
    columns: list[str] | None = None

    def __post_init__(self):
        if self.columns == []:
            raise InvalidConstraintError(
                "ForeignKeyReference 'columns' cannot be an empty list."
            )

    def __str__(self) -> str:
        if self.columns:
            return f"{self.table}({', '.join(self.columns)})"
        return self.table


# Column Constraints
class ColumnConstraint(ABC):
    """Abstract base class for column-level constraints.

    Column constraints are applied to individual columns and define
    validation rules that values in that column must satisfy.
    """


@dataclass(frozen=True)
class NotNullConstraint(ColumnConstraint):
    """Constraint requiring that column values cannot be NULL.

    Example:
        >>> import yads.types as ytypes
        >>> # Add to a field definition
        >>> Field(
        ...     name="email",
        ...     type=ytypes.String(),
        ...     constraints=[NotNullConstraint()]
        ... )
    """

    def __str__(self) -> str:
        return "NotNullConstraint()"


@dataclass(frozen=True)
class PrimaryKeyConstraint(ColumnConstraint):
    """Constraint designating a column as the primary key.

    Example:
        >>> import yads.types as ytypes
        >>> # Single-column primary key
        >>> Field(
        ...     name="id",
        ...     type=ytypes.Integer(),
        ...     constraints=[PrimaryKeyConstraint()]
        ... )
    """

    def __str__(self) -> str:
        return "PrimaryKeyConstraint()"


@dataclass(frozen=True)
class DefaultConstraint(ColumnConstraint):
    """Constraint providing a default value for a column.

    Specifies the value to use when no explicit value is provided
    during insert operations. The default value should be compatible
    with the column's data type.

    Args:
        value: The default value to use. Can be any JSON-serializable type.

    Example:
        >>> import yads.types as ytypes
        >>> # String default
        >>> DefaultConstraint(value="pending")

        >>> # Numeric default
        >>> DefaultConstraint(value=0)

        >>> # Boolean default
        >>> DefaultConstraint(value=True)

        >>> # Use in field definition
        >>> Field(
        ...     name="status",
        ...     type=ytypes.String(),
        ...     constraints=[DefaultConstraint(value="active")]
        ... )
    """

    value: Any

    def __str__(self) -> str:
        return f"DefaultConstraint(value={self.value!r})"


@dataclass(frozen=True)
class ForeignKeyConstraint(ColumnConstraint):
    """Column-level foreign key constraint.

    Args:
        references: The ForeignKeyReference specifying the target table and columns.
        name: Optional name for the constraint.

    Example:
        >>> # Simple foreign key to users table
        >>> ForeignKeyConstraint(
        ...     references=ForeignKeyReference(table="users")
        ... )

        >>> # Named foreign key with specific column
        >>> ForeignKeyConstraint(
        ...     name="fk_order_customer",
        ...     references=ForeignKeyReference(table="customers", columns=["id"])
        ... )
    """

    references: ForeignKeyReference
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
    """Constraint for auto-incrementing identity columns.

    Args:
        always: If True, values are always generated (GENERATED ALWAYS).
               If False, allows manual value insertion (GENERATED BY DEFAULT).
        start: Starting value for the sequence. If None, uses database default.
        increment: Increment step for each new value. If None, uses database default.

    Raises:
        InvalidConstraintError: If increment is zero.

    Example:
        >>> # Basic identity column
        >>> IdentityConstraint()

        >>> # Custom start and increment
        >>> IdentityConstraint(start=1000, increment=10)

        >>> # Allow manual insertion
        >>> IdentityConstraint(always=False, start=1, increment=1)
    """

    always: bool = True
    start: int | None = None
    increment: int | None = None

    def __post_init__(self):
        if self.increment == 0:
            raise InvalidConstraintError(
                f"Identity 'increment' must be a non-zero integer, not {self.increment}."
            )


# Table Constraints
@dataclass(frozen=True)
class TableConstraint(ABC):
    """Abstract base class for table-level constraints.

    Table constraints operate across multiple columns and are defined
    at the table level rather than on individual columns.
    """

    @property
    @abstractmethod
    def constrained_columns(self) -> list[str]:
        """Return the list of column names involved in this constraint."""


@dataclass(frozen=True)
class PrimaryKeyTableConstraint(TableConstraint):
    """Table-level primary key constraint.

    Args:
        columns: List of column names that form the composite primary key.
        name: Optional name for the constraint.

    Raises:
        InvalidConstraintError: If columns list is empty.

    Example:
        >>> # Composite primary key
        >>> PrimaryKeyTableConstraint(
        ...     columns=["order_id", "line_number"],
        ...     name="pk_order_lines"
        ... )

        >>> # Simple composite key without name
        >>> PrimaryKeyTableConstraint(columns=["year", "month", "category"])
    """

    columns: list[str]
    name: str | None = None

    def __post_init__(self):
        if not self.columns:
            raise InvalidConstraintError(
                "PrimaryKeyTableConstraint 'columns' cannot be empty."
            )

    @property
    def constrained_columns(self) -> list[str]:
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
    """Table-level foreign key constraint for composite relationships.

    Defines a foreign key across multiple columns, useful for referencing
    composite primary keys in other tables. The number of columns must match
    the number of referenced columns.

    Args:
        columns: List of column names in this table that form the foreign key.
        references: The ForeignKeyReference specifying the target table and columns.
        name: Optional name for the constraint.

    Raises:
        InvalidConstraintError: If columns list is empty or if the number of
                              columns doesn't match the referenced columns.

    Example:
        >>> # Composite foreign key
        >>> ForeignKeyTableConstraint(
        ...     columns=["customer_id", "customer_region"],
        ...     references=ForeignKeyReference(
        ...         table="customers",
        ...         columns=["id", "region"]
        ...     ),
        ...     name="fk_order_customer"
        ... )
    """

    columns: list[str]
    references: ForeignKeyReference
    name: str | None = None

    def __post_init__(self):
        if not self.columns:
            raise InvalidConstraintError(
                "ForeignKeyTableConstraint 'columns' cannot be empty."
            )
        if self.references.columns and len(self.columns) != len(self.references.columns):
            raise InvalidConstraintError(
                f"The number of columns in the foreign key ({len(self.columns)}) must match the number of "
                f"referenced columns ({len(self.references.columns)})."
            )

    @property
    def constrained_columns(self) -> list[str]:
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


CONSTRAINT_EQUIVALENTS: dict[type[ColumnConstraint], type[TableConstraint]] = {
    PrimaryKeyConstraint: PrimaryKeyTableConstraint,
    ForeignKeyConstraint: ForeignKeyTableConstraint,
}
