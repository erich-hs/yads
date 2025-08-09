"""AST validation engine for dialect compatibility.

Applies a set of validation/adjustment rules to a sqlglot AST in three modes:
- "raise": collect all errors and raise
- "warn": apply adjustments and emit warnings
- "ignore": do nothing
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Literal, cast

from sqlglot.expressions import Create

from ....exceptions import AstValidationError
from .ast_validation_rules import AstValidationRule

if TYPE_CHECKING:
    from sqlglot import exp


class ValidationWarning(UserWarning):
    """Warning category emitted when adjustments are applied in warn mode."""

    pass


class AstValidator:
    """Apply a list of `AstValidationRule` instances to a sqlglot AST.

    AstValidator applies a set of validation/adjustment rules to a sqlglot
    AST in three modes:
    - "raise": collect all errors and raise
    - "warn": apply adjustments and emit warnings
    - "ignore": do nothing

    The validator traverses the AST recursively, applying each rule to every
    node.

    Args:
        rules: List of `AstValidationRule` instances to apply during validation.

    Example:
        >>> from yads.converters.sql import AstValidator, NoFixedLengthStringRule
        >>>
        >>> # Create validator with built-in rules
        >>> rules = [NoFixedLengthStringRule()]
        >>> validator = AstValidator(rules=rules)
        >>>
        >>> # Apply validation in different modes
        >>> try:
        ...     ast = validator.validate(ast, mode="raise")
        ... except AstValidationError as e:
        ...     print(f"Validation failed: {e}")
        >>>
        >>> # Auto-fix with warnings
        >>> ast = validator.validate(ast, mode="warn")
        >>>
        >>> # Silently ignore incompatible features
        >>> ast = validator.validate(ast, mode="ignore")
    """

    def __init__(self, rules: list[AstValidationRule]):
        self.rules = rules

    def validate(
        self, ast: exp.Create, mode: Literal["raise", "warn", "ignore"]
    ) -> exp.Create:
        errors: list[str] = []

        def transformer(node: exp.Expression) -> exp.Expression:
            for rule in self.rules:
                error = rule.validate(node)
                if not error:
                    continue
                match mode:
                    case "raise":
                        errors.append(f"{error}")
                    case "warn":
                        warnings.warn(
                            f"{error} {rule.adjustment_description}",
                            ValidationWarning,
                            stacklevel=3,
                        )
                        node = rule.adjust(node)
                    case "ignore":
                        pass
                    case _:
                        raise AstValidationError(f"Invalid mode: {mode}.")
            return node

        processed_ast = ast.transform(transformer, copy=False)

        if errors:
            error_summary = "\n".join(f"- {e}" for e in errors)
            raise AstValidationError(
                "Validation for the target dialect failed with the following errors:\n"
                f"{error_summary}"
            )

        return cast(Create, processed_ast)
