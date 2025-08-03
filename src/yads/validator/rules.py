"""Built-in validation rules for common SQL dialect limitations.

This module contains pre-built validation rules that handle common compatibility
issues across different SQL dialects. These rules serve as both practical
utilities and examples for developers implementing custom validation logic.

The rules in this module address widespread dialect differences such as:
    - Type system variations and limitations
    - Unsupported syntax features
    - Different constraint handling approaches

Example:
    >>> import yads
    >>> from yads.converters import SQLGlotConverter
    >>> from yads.validator import AstValidator, NoFixedLengthStringRule
    >>>
    >>> my_spec = yads.from_yaml("my_spec.yaml")
    >>> original_ast = SQLGlotConverter().convert(my_spec)
    >>>
    >>> validator = AstValidator(rules=[NoFixedLengthStringRule()])
    >>> fixed_ast = validator.validate(original_ast, mode="warn")
    >>>
    >>> print(original_ast)
    >>> print(fixed_ast)
    CREATE TABLE my.schema (name TEXT(255), email TEXT(100))
    CREATE TABLE my.schema (name TEXT, email TEXT)
"""

from __future__ import annotations

from typing import TypeGuard

from sqlglot import exp

from yads.validator.core import Rule


class NoFixedLengthStringRule(Rule):
    """Remove fixed-length specifications from string data types.

    The rule preserves the string type but removes length constraints, converting
    types like STRING(255) to STRING or VARCHAR(100) to a generic sqlglot
    DataType.Type.TEXT.
    """

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
