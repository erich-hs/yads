from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from typing import List, Literal, cast

from sqlglot import exp


class Rule(ABC):
    """An abstract base class for a validation and adjustment rule.

    Each rule is responsible for:
    1. Identifying a specific unsupported feature in a sqlglot AST node.
    2. Providing an error message for the identified issue.
    3. Adjusting the AST node to make it compatible, if applicable.
    """

    @abstractmethod
    def validate(self, node: exp.Expression) -> str | None:
        """Validates a node. If the node is invalid, it returns an error message.

        Args:
            node: The sqlglot expression to validate.

        Returns:
            An error message string if validation fails, otherwise None.
        """
        pass

    @abstractmethod
    def adjust(self, node: exp.Expression) -> exp.Expression:
        """Adjusts an invalid node to make it compatible.

        Args:
            node: The sqlglot expression to adjust.

        Returns:
            The adjusted sqlglot expression.
        """
        pass


class AstValidator:
    """Processes a sqlglot AST to validate and adjust it for a specific dialect."""

    def __init__(self, rules: list[Rule]):
        """
        Args:
            rules: A list of validation rules to apply.
        """
        self.rules = rules

    def validate(
        self, ast: exp.Create, mode: Literal["strict", "warn", "ignore"]
    ) -> exp.Create:
        """Applies validation and adjustment rules to an AST.

        This method traverses the AST and applies each rule. Depending on the mode,
        it will either collect errors, warn and adjust, or silently adjust.

        Args:
            ast: The sqlglot AST to process.
            mode: The validation mode ("strict", "warn", or "ignore").

        Returns:
            The processed (and possibly adjusted) sqlglot AST.

        Raises:
            ValueError: In "strict" mode, if any validation errors are found.
        """
        if mode == "ignore":
            return ast

        errors: List[str] = []

        def transformer(node: exp.Expression) -> exp.Expression:
            for rule in self.rules:
                error = rule.validate(node)
                if not error:
                    continue

                if mode == "strict":
                    errors.append(error)
                elif mode == "warn":
                    warnings.warn(
                        f"{error} The converter will proceed by ignoring this feature."
                    )
                    node = rule.adjust(node)
            return node

        processed_ast = ast.transform(transformer, copy=False)

        if errors:
            error_summary = "\n".join(f"- {e}" for e in errors)
            raise ValueError(
                f"Validation for the target dialect failed with the following errors:\n{error_summary}"
            )

        return cast(exp.Create, processed_ast)
