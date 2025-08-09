"""Built-in validation rules for the AstValidator.

This module contains pre-built validation rules that handle common compatibility
issues across different SQL dialects.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypeGuard

from sqlglot.expressions import DataType, ColumnDef

if TYPE_CHECKING:
    from sqlglot import exp


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


class DisallowFixedLengthString(AstValidationRule):
    """Remove fixed-length parameters from string data types (e.g., VARCHAR(50))."""

    def _is_fixed_length_string(self, node: exp.Expression) -> TypeGuard[exp.DataType]:
        return (
            isinstance(node, DataType)
            and node.this in DataType.TEXT_TYPES
            and bool(node.expressions)
        )

    def validate(self, node: exp.Expression) -> str | None:
        if self._is_fixed_length_string(node):
            column_def = node.find_ancestor(ColumnDef)
            column_name = column_def.this.name if column_def else "UNKNOWN"
            return f"Fixed-length strings are not supported for column '{column_name}'."
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if self._is_fixed_length_string(node):
            node.set("expressions", None)
        return node

    @property
    def adjustment_description(self) -> str:
        return "The length parameter will be removed."
