"""AST validation engine for dialect compatibility."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

from ...._dependencies import ensure_dependency
from ....exceptions import AstValidationError, validation_warning
from .ast_validation_rules import AstValidationRule

ensure_dependency("sqlglot")

from sqlglot.expressions import Create  # noqa: E402

if TYPE_CHECKING:
    from sqlglot import exp


class AstValidator:
    """Apply a list of `AstValidationRule` instances to a sqlglot AST.

    AstValidator applies a set of validation/adjustment rules to a sqlglot
    AST in two modes:
    - "raise": collect all errors and raise
    - "coerce": apply adjustments and emit warnings

    The validator traverses the AST recursively, applying each rule to every
    node.

    Args:
        rules: List of `AstValidationRule` instances to apply during validation.

    Example:
        >>> from yads.converters.sql import AstValidator, DisallowFixedLengthString
        >>>
        >>> # Create validator with built-in rules
        >>> rules = [DisallowFixedLengthString()]
        >>> validator = AstValidator(rules=rules)
        >>>
        >>> # Apply validation in different modes
        >>> try:
        ...     ast = validator.validate(ast, mode="raise")
        ... except AstValidationError as e:
        ...     print(f"Validation failed: {e}")
        >>>
        >>> # Auto-fix with warnings
        >>> ast = validator.validate(ast, mode="coerce")
    """

    def __init__(self, rules: list[AstValidationRule]):
        self.rules = rules

    def validate(self, ast: exp.Create, mode: Literal["raise", "coerce"]) -> exp.Create:
        errors: list[str] = []

        def transformer(node: exp.Expression) -> exp.Expression:
            for rule in self.rules:
                error = rule.validate(node)
                if not error:
                    continue
                match mode:
                    case "raise":
                        errors.append(f"{error}")
                    case "coerce":
                        validation_warning(
                            message=f"{error} {rule.adjustment_description}",
                            filename="yads.converters.sql.validators",
                            module=__name__,
                        )
                        node = rule.adjust(node)
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
