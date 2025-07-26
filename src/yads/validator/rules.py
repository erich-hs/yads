from __future__ import annotations

from sqlglot import exp

from yads.validator.core import Rule


class NoFixedLengthStringRule(Rule):
    """Validation rule to disallow fixed-length strings."""

    def validate(self, node: exp.Expression) -> str | None:
        """Checks for fixed-length string data types."""
        if (
            isinstance(node, exp.DataType)
            and node.this == exp.DataType.Type.TEXT
            and node.expressions
        ):
            column_def = node.find_ancestor(exp.ColumnDef)
            column_name = column_def.this.name if column_def else "UNKNOWN"
            return f"Fixed-length strings are not supported for column '{column_name}'."
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        """Removes the length parameter from a string type."""
        if (
            isinstance(node, exp.DataType)
            and node.this == exp.DataType.Type.TEXT
            and node.expressions
        ):
            node.set("expressions", None)
        return node
