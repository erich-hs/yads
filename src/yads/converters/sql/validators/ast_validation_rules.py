"""Built-in validation rules for the AstValidator.

This module contains pre-built validation rules that handle common compatibility
issues across different SQL dialects.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypeGuard

from ...._dependencies import ensure_dependency

ensure_dependency("sqlglot")

from sqlglot.expressions import (  # noqa: E402
    DataType,
    ColumnDef,
    ColumnConstraint,
    GeneratedAsIdentityColumnConstraint,
    PrimaryKey,
    Ordered,
    Literal,
    DataTypeParam,
    Neg,
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


class DisallowUserDefinedType(AstValidationRule):
    """Disallow USERDEFINED types and replace them with a fallback type.

    This rule flags any occurrence of a `sqlglot.expressions.DataType` with
    `this=DataType.Type.USERDEFINED`. When adjustment is requested, the
    offending type is replaced by the provided fallback type (defaults to
    `TEXT`).

    Args:
        disallow_type: The `sqlglot.expressions.DataType` `kind` that identifies
            the user-defined type to be disallowed.
        fallback_type: The `sqlglot.expressions.DataType.Type` enum member to
            use when replacing the disallowed type during adjustment. Defaults
            to `DataType.Type.TEXT`.
    """

    def __init__(
        self, disallow_type: str, fallback_type: DataType.Type = DataType.Type.TEXT
    ):
        self.disallow_type: str = disallow_type
        self.fallback_type: DataType.Type = fallback_type

    def _is_disallowed_type(self, node: exp.Expression) -> TypeGuard[exp.DataType]:
        return (
            isinstance(node, DataType)
            and node.this == DataType.Type.USERDEFINED
            and node.args.get("kind") == self.disallow_type
        )

    def validate(self, node: exp.Expression) -> str | None:
        if self._is_disallowed_type(node):
            column_name = _get_ancestor_column_name(node)
            return (
                f"Data type '{node.args.get('kind')}' is not supported for column "
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


class DisallowFixedLengthBinary(AstValidationRule):
    """Remove fixed-length BINARY types such as BINARY(50)."""

    def _is_fixed_length_binary(self, node: exp.Expression) -> TypeGuard[exp.DataType]:
        return (
            isinstance(node, DataType)
            and node.this == DataType.Type.BINARY
            and bool(node.expressions)
        )

    def validate(self, node: exp.Expression) -> str | None:
        if self._is_fixed_length_binary(node):
            column_name = _get_ancestor_column_name(node)
            return f"Fixed-length binary is not supported for column '{column_name}'."
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if self._is_fixed_length_binary(node):
            node.set("expressions", None)
        return node

    @property
    def adjustment_description(self) -> str:
        return "The length parameter will be removed."


class DisallowNegativeScaleDecimal(AstValidationRule):
    """Disallow negative scale decimals and coerce them to positive scale.

    This rule flags any occurrence of a `sqlglot.expressions.DataType` with
    `this=DataType.Type.DECIMAL` that has a negative scale. When adjustment is
    requested, the precision is increased by the absolute value of the negative
    scale, and the scale is set to 0.

    For example:
        - DECIMAL(10, -2) becomes DECIMAL(12, 0)
        - DECIMAL(10, -6) becomes DECIMAL(16, 0)
    """

    def _int_from_param(self, expr: exp.Expression | None) -> int | None:
        """Extract an integer from a DECIMAL parameter expression.

        Supports Literal(2), Literal(-2), and Neg(Literal(2)). Returns None for
        non-integer or unsupported forms.
        """
        if expr is None:
            return None
        # Unwrap DataTypeParam(this=...)
        if isinstance(expr, DataTypeParam):
            return self._int_from_param(expr.this)
        # Literal number (tests may construct Literal without is_string flag)
        if isinstance(expr, Literal):
            value = expr.this
            if isinstance(value, int):
                return value
            # For int params stored as string
            try:
                return int(value)  # type: ignore[arg-type]
            except Exception:
                return None
        # Negative wrapper: Neg(this=Literal(2)) -> -2
        if isinstance(expr, Neg):
            inner = expr.args.get("this")
            inner_val = self._int_from_param(inner)
            return -inner_val if inner_val is not None else None
        return None

    def _parse_decimal_params(
        self, node: exp.Expression
    ) -> tuple[int | None, int | None]:
        """Return (precision, scale) if node is DECIMAL else (None, None)."""
        if not (isinstance(node, DataType) and node.this == DataType.Type.DECIMAL):
            return None, None
        params = getattr(node, "expressions", None) or []
        if len(params) < 2:
            return None, None
        precision_expr = params[0]
        scale_expr = params[1]
        precision = self._int_from_param(precision_expr)
        scale = self._int_from_param(scale_expr)
        return precision, scale

    def _is_negative_scale_decimal(self, node: exp.Expression) -> TypeGuard[exp.DataType]:
        _, scale = self._parse_decimal_params(node)
        return scale is not None and scale < 0

    def validate(self, node: exp.Expression) -> str | None:
        if self._is_negative_scale_decimal(node):
            column_name = _get_ancestor_column_name(node)
            precision, scale = self._parse_decimal_params(node)
            return (
                f"Negative scale decimals are not supported for column '{column_name}'. "
                f"Found DECIMAL({precision}, {scale})."
            )
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if self._is_negative_scale_decimal(node):
            precision_val, scale_val = self._parse_decimal_params(node)
            if precision_val is None or scale_val is None:
                return node  # Incomplete params; nothing to do
            # Calculate new precision: add absolute value of negative scale
            new_precision = precision_val + abs(scale_val)
            # Create new DataType with adjusted precision and scale = 0
            return DataType(
                this=DataType.Type.DECIMAL,
                expressions=[
                    DataTypeParam(this=Literal(this=new_precision, is_string=False)),
                    DataTypeParam(this=Literal(this=0, is_string=False)),
                ],
            )
        return node

    @property
    def adjustment_description(self) -> str:
        return "The precision will be increased by the absolute value of the negative scale, and the scale will be set to 0."


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
