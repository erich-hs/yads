"""SQL converters orchestrating AST conversion and SQL generation.

This module contains high-level SQL converters that:
- Build an Abstract Syntax Tree (AST) from a `SchemaSpec` using an AST converter
- Optionally validate/adjust the AST using AST validation rules
- Serialize the final AST to a SQL string for a target dialect
"""

from __future__ import annotations

from typing import Any, Literal

from sqlglot import ErrorLevel

from ...spec import SchemaSpec
from ..base import BaseConverter
from .ast_converter import SQLGlotConverter  # type: ignore[reportMissingImports]
from .validators.ast_validator import AstValidator  # type: ignore[reportMissingImports]
from .validators.ast_validation_rules import (  # type: ignore[reportMissingImports]
    AstValidationRule,
    NoFixedLengthStringRule,
)


class SQLConverter:
    """Base class for SQL DDL generation.

    The converter composes:
    - An AST converter.
    - An optional AST validator that enforces or adjusts dialect compatibility.

    Args:
        dialect: Target SQL dialect name accepted by the AST converter.
        ast_converter: AST builder. Defaults to `SQLGlotConverter`.
        ast_validator: Optional validator for dialect-specific adjustments.
        **convert_options: Default AST converter generation options.

    Example:
        >>> from yads.converters.sql import SQLConverter, AstValidator, NoFixedLengthStringRule
        >>>
        >>> # Create converter with custom validation
        >>> converter = SQLConverter(
        ...     dialect="snowflake",
        ...     ast_validator=AstValidator(rules=[NoFixedLengthStringRule()]),
        ...     pretty=True
        ... )
        >>> ddl = converter.convert(spec, mode="raise")
    """

    def __init__(
        self,
        dialect: str,
        ast_converter: BaseConverter | None = None,
        ast_validator: AstValidator | None = None,
        **convert_options: Any,
    ):
        self._ast_converter = ast_converter or SQLGlotConverter()
        self._dialect = dialect
        self._ast_validator = ast_validator
        self._convert_options = convert_options

    def convert(
        self,
        spec: SchemaSpec,
        if_not_exists: bool = False,
        or_replace: bool = False,
        ignore_catalog: bool = False,
        ignore_database: bool = False,
        mode: Literal["raise", "warn", "ignore"] = "warn",
        **kwargs: Any,
    ) -> str:
        """Convert a yads SchemaSpec into a SQL DDL string.

        This method orchestrates the conversion pipeline from SchemaSpec to SQL DDL.
        It first converts the spec to an intermediate AST, applies any configured
        validation rules, and finally serializes to a SQL DDL string.

        Args:
            spec: The yads specification as a SchemaSpec object.
            if_not_exists: If True, add `IF NOT EXISTS` clause to the DDL statement.
            or_replace: If True, add `OR REPLACE` clause to the DDL statement.
            ignore_catalog: If True, omits the catalog from the table name.
            ignore_database: If True, omits the database from the table name.
            mode: Validation mode when an ast_validator is configured:
                - "raise": Raise ValidationRuleError for any unsupported features.
                - "warn": Log warnings and automatically adjust AST for compatibility.
                - "ignore": Silently ignore unsupported features without warnings.
                            The generated SQL DDL might still contain modifications
                            done by the AST converter.
            **kwargs: Additional options for SQL DDL string serialization, overriding
                      defaults. For a SQLGlotConverter, see sqlglot's documentation
                      for supported options:
                      https://sqlglot.com/sqlglot/generator.html#Generator

        Returns:
            SQL DDL CREATE TABLE statement as a string.

        Raises:
            ValidationRuleError: In raise mode when unsupported features are detected.
            ConversionError: When the underlying conversion process fails.

        Example:
            >>> ddl = converter.convert(
            ...     spec,
            ...     if_not_exists=True,
            ...     pretty=True
            ... )
        """
        ast = self._ast_converter.convert(
            spec,
            if_not_exists=if_not_exists,
            or_replace=or_replace,
            ignore_catalog=ignore_catalog,
            ignore_database=ignore_database,
        )

        if self._ast_validator:
            ast = self._ast_validator.validate(ast, mode=mode)

        if isinstance(self._ast_converter, SQLGlotConverter):
            match mode:
                case "raise":
                    self._convert_options["unsupported_level"] = ErrorLevel.RAISE
                case "warn":
                    self._convert_options["unsupported_level"] = ErrorLevel.WARN
                case "ignore":
                    self._convert_options["unsupported_level"] = ErrorLevel.IGNORE

        options = {**self._convert_options, **kwargs}
        return ast.sql(dialect=self._dialect, **options)


class SparkSQLConverter(SQLConverter):
    """Spark SQL converter with built-in validation rules.

    Configured with:
    - dialect="spark"
    - `NoFixedLengthStringRule` to remove fixed-length text parameters
    """

    def __init__(self, **convert_options: Any):
        rules: list[AstValidationRule] = [NoFixedLengthStringRule()]
        validator = AstValidator(rules=rules)
        super().__init__(dialect="spark", ast_validator=validator, **convert_options)
