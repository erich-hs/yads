"""Built-in validation rules for the AstValidator.

This module contains pre-built validation rules that handle common compatibility
issues across different SQL dialects.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypeGuard

from sqlglot.expressions import (
    DataType,
    ColumnDef,
    ColumnConstraint,
    GeneratedAsIdentityColumnConstraint,
    PrimaryKey,
    Ordered,
)

if TYPE_CHECKING:
    from sqlglot import exp


def _get_ancestor_column_name(node: exp.Expression) -> str:
    column_def = node.find_ancestor(ColumnDef)
    return column_def.this.name if column_def else "UNKNOWN"


class AstValidationRule(ABC):
    """Abstract base for AST validation/adjustment rules."""

    @abstractmethod
    def validate(self, node: exp.Expression) -> str | None:
        """Return an error message for invalid node, or None if valid."""

    @abstractmethod
    def adjust(self, node: exp.Expression) -> exp.Expression:
        """Adjust the node in-place or return a replacement node."""

    @property
    @abstractmethod
    def adjustment_description(self) -> str:
        """Human-readable description of the rule's adjustment."""


class DisallowType(AstValidationRule):
    """Disallow a specific SQL data type and replace it with a fallback type.

    This rule flags any occurrence of a specific disallowed
    `sqlglot.expressions.DataType` in the AST. When adjustment is requested,
    the offending type is replaced by the provided fallback type (defaults to
    `TEXT`).

    Args:
        disallow_type: The `sqlglot.expressions.DataType.Type` enum member that
            should be disallowed.
        fallback_type: The `sqlglot.expressions.DataType.Type` enum member to
            use when replacing the disallowed type during adjustment. Defaults
            to `DataType.Type.TEXT`.
    """

    def __init__(
        self,
        disallow_type: DataType.Type,
        fallback_type: DataType.Type = DataType.Type.TEXT,
    ):
        self.disallow_type: DataType.Type = disallow_type
        self.fallback_type: DataType.Type = fallback_type

    def _is_disallowed_type(self, node: exp.Expression) -> TypeGuard[exp.DataType]:
        return isinstance(node, DataType) and node.this == self.disallow_type

    def validate(self, node: exp.Expression) -> str | None:
        if self._is_disallowed_type(node):
            column_name = _get_ancestor_column_name(node)
            return (
                f"Data type '{node.this.name}' is not supported for column "
                f"'{column_name}'."
            )
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if self._is_disallowed_type(node):
            return DataType(this=self.fallback_type)
        return node

    @property
    def adjustment_description(self) -> str:
        return f"The data type will be replaced with '{self.fallback_type.name}'."


class DisallowVoidType(AstValidationRule):
    """Disallow VOID type and replace it with TEXT.

    The converter represents VOID as a USERDEFINED data type with
    kind='VOID'. This rule matches that representation to provide a clear
    validation message and replacement behavior.
    """

    def validate(self, node: exp.Expression) -> str | None:
        if isinstance(node, DataType) and node.this == DataType.Type.USERDEFINED:
            # "kind" holds the textual type name when USERDEFINED is used
            kind = (node.args.get("kind") or "").upper()
            if kind == "VOID":
                column_name = _get_ancestor_column_name(node)
                return f"Data type 'VOID' is not supported for column '{column_name}'."
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if isinstance(node, DataType) and node.this == DataType.Type.USERDEFINED:
            kind = (node.args.get("kind") or "").upper()
            if kind == "VOID":
                return DataType(this=DataType.Type.TEXT)
        return node

    @property
    def adjustment_description(self) -> str:
        return "The data type will be replaced with 'TEXT'."


class DisallowFixedLengthString(AstValidationRule):
    """Remove fixed-length STRING types such as VARCHAR(50)."""

    def _is_fixed_length_string(self, node: exp.Expression) -> TypeGuard[exp.DataType]:
        return (
            isinstance(node, DataType)
            and node.this in DataType.TEXT_TYPES
            and bool(node.expressions)
        )

    def validate(self, node: exp.Expression) -> str | None:
        if self._is_fixed_length_string(node):
            column_name = _get_ancestor_column_name(node)
            return f"Fixed-length strings are not supported for column '{column_name}'."
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if self._is_fixed_length_string(node):
            node.set("expressions", None)
        return node

    @property
    def adjustment_description(self) -> str:
        return "The length parameter will be removed."


class DisallowParameterizedGeometry(AstValidationRule):
    """Disallow parameterized GEOMETRY types such as GEOMETRY(4326)."""

    def _is_parameterized_geometry(self, node: exp.Expression) -> TypeGuard[exp.DataType]:
        return (
            isinstance(node, DataType)
            and node.this == DataType.Type.GEOMETRY
            and bool(node.expressions)
        )

    def validate(self, node: exp.Expression) -> str | None:
        if self._is_parameterized_geometry(node):
            column_name = _get_ancestor_column_name(node)
            return (
                f"Parameterized 'GEOMETRY' is not supported for column '{column_name}'."
            )
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if self._is_parameterized_geometry(node):
            node.set("expressions", None)
        return node

    @property
    def adjustment_description(self) -> str:
        return "The parameters will be removed."


class DisallowColumnConstraintGeneratedIdentity(AstValidationRule):
    """Disallow GENERATED ALWAYS AS IDENTITY column constraint.

    Matches identity generation constraints attached to a column definition and
    removes them during adjustment.
    """

    def _has_identity_constraint(self, node: ColumnDef) -> bool:
        constraints: list[ColumnConstraint] | None = node.args.get("constraints")
        if not constraints:
            return False
        for constraint in constraints:
            if isinstance(constraint, ColumnConstraint) and isinstance(
                constraint.kind, GeneratedAsIdentityColumnConstraint
            ):
                # Only flag true IDENTITY (sequence) clauses, not generated columns
                # Generated columns carry an 'expression' argument
                if not constraint.kind.args.get("expression"):
                    return True
        return False

    def validate(self, node: exp.Expression) -> str | None:
        if isinstance(node, ColumnDef) and self._has_identity_constraint(node):
            column_name = node.this.name
            return (
                "GENERATED ALWAYS AS IDENTITY is not supported for column "
                f"'{column_name}'."
            )
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if isinstance(node, ColumnDef) and self._has_identity_constraint(node):
            constraints: list[ColumnConstraint] | None = node.args.get("constraints")
            if constraints:
                filtered = [
                    c
                    for c in constraints
                    if not isinstance(c.kind, GeneratedAsIdentityColumnConstraint)
                ]
                node.set("constraints", filtered or None)
        return node

    @property
    def adjustment_description(self) -> str:
        return "The identity generation clause will be removed."


class DisallowTableConstraintPrimaryKeyNullsFirst(AstValidationRule):
    """Remove NULLS FIRST from table-level PRIMARY KEY constraints."""

    def _has_nulls_first(self, node: PrimaryKey) -> bool:
        expressions = node.args.get("expressions") or []
        for expr in expressions:
            if isinstance(expr, Ordered) and expr.args.get("nulls_first") is True:
                return True
        return False

    def validate(self, node: exp.Expression) -> str | None:
        if isinstance(node, PrimaryKey) and self._has_nulls_first(node):
            return "NULLS FIRST is not supported in PRIMARY KEY constraints."
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if isinstance(node, PrimaryKey) and self._has_nulls_first(node):
            expressions = node.args.get("expressions") or []
            for expr in expressions:
                if isinstance(expr, Ordered) and expr.args.get("nulls_first") is True:
                    expr.set("nulls_first", None)
        return node

    @property
    def adjustment_description(self) -> str:
        return "The NULLS FIRST attribute will be removed."
