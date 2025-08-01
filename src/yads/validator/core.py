"""Core validation framework for AST processing and dialect compliance.

This module provides the infrastructure for validating and adjusting sqlglot
Abstract Syntax Trees (ASTs) to ensure compatibility with specific SQL dialects.

The validation process operates on sqlglot AST nodes and can either report
errors (in strict mode) or automatically adjust the AST to ensure compatibility
(in fix mode). This enables yads to generate valid SQL DDL for dialects with
varying feature support while maintaining a single, expressive schema specification.

Example:
    >>> from yads.validator import AstValidator, Rule
    >>> from sqlglot import exp
    >>>
    >>> class CustomRule(Rule):
    ...     def validate(self, node):
    ...         # Custom validation logic
    ...         return None  # or error message
    ...
    ...     def adjust(self, node):
    ...         # Custom adjustment logic
    ...         return node
    ...
    ...     @property
    ...     def adjustment_description(self):
    ...         return "Custom adjustment applied"
    >>>
    >>> validator = AstValidator(rules=[CustomRule()])
    >>> processed_ast = validator.validate(ast, mode="fix")
"""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from typing import List, Literal, cast

from sqlglot import exp

from ..exceptions import ValidationRuleError


class Rule(ABC):
    """Abstract base class for AST validation and adjustment rules.

    Rules are the core components of the validation framework. Each rule is
    responsible for identifying a specific type of incompatibility or limitation
    in a sqlglot AST and providing both error reporting and automatic fixes.

    Rules operate on individual AST nodes and are applied recursively throughout
    the entire AST tree. This allows for fine-grained control over validation
    and adjustment logic while maintaining simplicity in rule implementation.

    Rules are typically created to handle dialect-specific limitations such as:
    - Unsupported data types or type features
    - Missing SQL clauses or keywords
    - Incompatible constraint types
    - Dialect-specific syntax requirements

    When implementing a custom rule, consider:
    - Performance: Rules are applied to every node in the AST
    - Idempotency: Adjustments should be safe to apply multiple times
    - Clarity: Error messages should be actionable for users

    Example:
        >>> class DisallowArraysRule(Rule):
        ...     def validate(self, node):
        ...         if isinstance(node, exp.DataType) and node.this == exp.DataType.Type.ARRAY:
        ...             return "Array types are not supported in this dialect"
        ...         return None
        ...
        ...     def adjust(self, node):
        ...         if isinstance(node, exp.DataType) and node.this == exp.DataType.Type.ARRAY:
        ...             # Convert array to text representation
        ...             node.set("this", exp.DataType.Type.TEXT)
        ...         return node
        ...
        ...     @property
        ...     def adjustment_description(self):
        ...         return "Array types converted to TEXT"
    """

    @abstractmethod
    def validate(self, node: exp.Expression) -> str | None:
        """Validate an AST node and return an error message if invalid.

        This method examines a single AST node to determine if it represents
        a feature or construct that is incompatible with the target dialect.
        If the node is valid, return None. If invalid, return a descriptive
        error message that will help users understand the issue.

        Args:
            node: The sqlglot expression node to validate.

        Returns:
            None if the node is valid, otherwise a descriptive error message
            string explaining what is incompatible and why.

        Example:
            >>> def validate(self, node):
            ...     if isinstance(node, exp.DataType) and node.expressions:
            ...         return f"Fixed-length types not supported: {node}"
            ...     return None
        """
        pass

    @abstractmethod
    def adjust(self, node: exp.Expression) -> exp.Expression:
        """Adjust an invalid AST node to make it compatible with the target dialect.

        This method modifies the AST node in-place to resolve the incompatibility
        identified by the validate() method. The adjustment should transform the
        node into a valid equivalent that preserves the intended semantics as
        much as possible.

        Adjustments should be:
        - Safe to apply multiple times (idempotent)
        - Semantically equivalent or as close as possible
        - Well-documented through the adjustment_description property

        Args:
            node: The sqlglot expression node to adjust.

        Returns:
            The adjusted sqlglot expression node. This may be the same node
            modified in-place or a replacement node.

        Example:
            >>> def adjust(self, node):
            ...     if isinstance(node, exp.DataType) and node.expressions:
            ...         # Remove length specification
            ...         node.set("expressions", None)
            ...     return node
        """
        pass

    @property
    @abstractmethod
    def adjustment_description(self) -> str:
        """Provide a human-readable description of what the adjust() method does.

        This property should return a clear, concise description of the
        modification that the adjust() method applies to invalid nodes.
        The description is used in warning messages to inform users about
        automatic changes made to their schema.

        Returns:
            A descriptive string explaining the adjustment behavior.

        Example:
            >>> @property
            ... def adjustment_description(self):
            ...     return "Fixed-length type parameters removed for compatibility"
        """
        pass


class AstValidator:
    """Validator for applying rules to sqlglot ASTs for dialect compliance.

    AstValidator orchestrates the application of validation rules across an
    entire AST, providing different modes of operation for various use cases.
    It supports both strict validation (fail-fast) and automatic adjustment
    (best-effort compatibility) approaches.

    The validator traverses the AST recursively, applying each rule to every
    node. Depending on the mode, it will either collect errors, apply fixes,
    or issue warnings about potential incompatibilities.

    This class is typically used by SQL converters to ensure that generated
    DDL is compatible with specific database dialects while providing clear
    feedback about any transformations applied.

    Args:
        rules: List of Rule instances to apply during validation.

    Example:
        >>> from yads.validator import AstValidator, NoFixedLengthStringRule
        >>>
        >>> # Create validator with built-in rules
        >>> rules = [NoFixedLengthStringRule()]
        >>> validator = AstValidator(rules=rules)
        >>>
        >>> # Apply validation in different modes
        >>> try:
        ...     strict_ast = validator.validate(ast, mode="strict")
        ... except ValidationRuleError as e:
        ...     print(f"Validation failed: {e}")
        >>>
        >>> # Auto-fix with warnings
        >>> fixed_ast = validator.validate(ast, mode="fix")
        >>>
        >>> # Check compatibility without changes
        >>> checked_ast = validator.validate(ast, mode="warn")
    """

    def __init__(self, rules: list[Rule]):
        self.rules = rules

    def validate(
        self, ast: exp.Create, mode: Literal["strict", "fix", "warn"]
    ) -> exp.Create:
        """Apply validation rules to an AST with the specified mode.

        Processes the entire AST by applying each validation rule to every node.
        The behavior depends on the mode parameter, allowing for different
        validation strategies based on the use case.

        The validation process:
        1. Traverses the AST recursively using sqlglot's transform mechanism
        2. Applies each rule to every node in the tree
        3. Handles rule violations according to the specified mode
        4. Returns the processed (and possibly modified) AST

        Args:
            ast: The sqlglot CREATE TABLE AST to validate and process.
            mode: Validation mode determining how to handle rule violations:
                - "strict": Collect all errors and raise ValidationRuleError
                  if any violations are found. The AST is not modified.
                - "fix": Apply automatic adjustments for each violation and
                  issue warnings about the changes made.
                - "warn": Issue warnings about violations but don't modify
                  the AST. Useful for compatibility checking.

        Returns:
            The processed sqlglot AST. In "fix" mode, this may be modified
            from the original. In other modes, it should be unchanged.

        Raises:
            ValidationRuleError: In "strict" mode, if any validation rules
                               detect incompatibilities.
            ValidationRuleError: If an invalid mode is specified.

        Example:
            >>> validator = AstValidator(rules=[MyCustomRule()])
            >>>
            >>> # Strict validation - fail on any issues
            >>> validated_ast = validator.validate(ast, mode="strict")
            >>>
            >>> # Auto-fix mode - apply corrections automatically
            >>> fixed_ast = validator.validate(ast, mode="fix")
            >>>
            >>> # Warning mode - report issues without changes
            >>> warned_ast = validator.validate(ast, mode="warn")
        """
        errors: List[str] = []

        def transformer(node: exp.Expression) -> exp.Expression:
            for rule in self.rules:
                error = rule.validate(node)
                if not error:
                    continue

                match mode:
                    case "strict":
                        errors.append(f"{error}")
                    case "fix":
                        warnings.warn(f"{error} {rule.adjustment_description}")
                        node = rule.adjust(node)
                    case "warn":
                        warnings.warn(
                            f"{error}\n"
                            "Set mode to 'fix' to automatically adjust the AST. "
                            "The validator will proceed with no adjustment."
                        )
                    case _:
                        raise ValidationRuleError(f"Invalid mode: {mode}.")
            return node

        # https://sqlglot.com/sqlglot/expressions.html#Expression.transform
        processed_ast = ast.transform(transformer, copy=False)

        if errors:
            error_summary = "\n".join(f"- {e}" for e in errors)
            raise ValidationRuleError(
                f"Validation for the target dialect failed with the following errors:\n{error_summary}."
            )

        return cast(exp.Create, processed_ast)
