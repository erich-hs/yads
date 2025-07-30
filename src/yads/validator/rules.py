from __future__ import annotations

from typing import TypeGuard

from sqlglot import exp

from yads.validator.core import Rule


class NoFixedLengthStringRule(Rule):
    """Validation rule to disallow fixed-length strings."""

    def _is_fixed_length_string(self, node: exp.Expression) -> TypeGuard[exp.DataType]:
        return (
            isinstance(node, exp.DataType)
            and node.this in exp.DataType.TEXT_TYPES
            and bool(node.expressions)
        )

    def validate(self, node: exp.Expression) -> str | None:
        if self._is_fixed_length_string(node):
            column_def = node.find_ancestor(exp.ColumnDef)
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
