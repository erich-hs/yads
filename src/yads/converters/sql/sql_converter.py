"""SQL converters orchestrating AST conversion and SQL generation.

This module contains high-level SQL converters that:
- Build an Abstract Syntax Tree (AST) from a `YadsSpec` using an AST converter
- Optionally validate/adjust the AST using AST validation rules
- Serialize the final AST to a SQL string for a target dialect
"""

from __future__ import annotations

from typing import Any, Literal

from sqlglot import ErrorLevel
from sqlglot.expressions import DataType

from ...spec import YadsSpec
from ..base import BaseConverter
from .ast_converter import SQLGlotConverter
from .validators.ast_validator import AstValidator
from .validators.ast_validation_rules import (
    AstValidationRule,
    DisallowType,
    DisallowParameterizedGeometry,
    DisallowUserDefinedType,
    DisallowColumnConstraintGeneratedIdentity,
    DisallowTableConstraintPrimaryKeyNullsFirst,
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
        >>> from yads.converters.sql import SQLConverter, AstValidator, DisallowFixedLengthString
        >>>
        >>> # Create converter with custom validation
        >>> converter = SQLConverter(
        ...     dialect="snowflake",
        ...     ast_validator=AstValidator(rules=[DisallowFixedLengthString()]),
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
        spec: YadsSpec,
        if_not_exists: bool = False,
        or_replace: bool = False,
        ignore_catalog: bool = False,
        ignore_database: bool = False,
        mode: Literal["raise", "coerce"] = "coerce",
        **kwargs: Any,
    ) -> str:
        """Convert a yads `YadsSpec` into a SQL DDL string.

        This method orchestrates the conversion pipeline from `YadsSpec` to SQL DDL.
        It first converts the spec to an intermediate AST, applies any configured
        validation rules, and finally serializes to a SQL DDL string.

        Args:
            spec: The yads specification as a `YadsSpec` object.
            if_not_exists: If True, add `IF NOT EXISTS` clause to the DDL statement.
            or_replace: If True, add `OR REPLACE` clause to the DDL statement.
            ignore_catalog: If True, omits the catalog from the table name.
            ignore_database: If True, omits the database from the table name.
            mode: Validation mode when an ast_validator is configured:
                - "raise": Raise on any unsupported features.
                - "coerce": Apply adjustments to produce a valid AST and emit warnings.
            **kwargs: Additional options for SQL DDL string serialization, overriding
                      defaults. For a `SQLGlotConverter`, see sqlglot's documentation
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
                case "coerce":
                    self._convert_options["unsupported_level"] = ErrorLevel.WARN

        options = {**self._convert_options, **kwargs}
        return ast.sql(dialect=self._dialect, **options)


class SparkSQLConverter(SQLConverter):
    """Spark SQL converter with built-in validation rules.

    Configured with:
    - dialect="spark"
    - Rules:
      - Disallow JSON → replace with STRING
      - Disallow GEOMETRY → replace with STRING
      - Disallow GEOGRAPHY → replace with STRING
    """

    def __init__(self, **convert_options: Any):
        rules: list[AstValidationRule] = [
            DisallowType(
                disallow_type=DataType.Type.JSON,
            ),
            DisallowType(disallow_type=DataType.Type.GEOMETRY),
            DisallowType(disallow_type=DataType.Type.GEOGRAPHY),
        ]
        validator = AstValidator(rules=rules)
        super().__init__(dialect="spark", ast_validator=validator, **convert_options)


class DuckdbSQLConverter(SQLConverter):
    """DuckDB SQL converter with built-in validation rules.

    Configured with:
    - dialect="duckdb"
    - Rules:
      - Disallow TimestampLTZ → replace with TimestampTZ
      - Disallow VOID → replace with STRING
      - Disallow GEOGRAPHY → replace with STRING
      - Disallow parametrized GEOMETRY → strip parameters
      - Disallow VARIANT → replace with STRING
      - Disallow column-level IDENTITY → remove constraint
      - Disallow NULLS FIRST in table-level PRIMARY KEY constraints → remove NULLS FIRST
    """

    def __init__(self, **convert_options: Any):
        rules: list[AstValidationRule] = [
            DisallowType(
                disallow_type=DataType.Type.TIMESTAMPLTZ,
                fallback_type=DataType.Type.TIMESTAMPTZ,
            ),
            DisallowUserDefinedType(disallow_type="VOID"),
            DisallowType(disallow_type=DataType.Type.GEOGRAPHY),
            DisallowParameterizedGeometry(),
            DisallowType(disallow_type=DataType.Type.VARIANT),
            DisallowColumnConstraintGeneratedIdentity(),
            DisallowTableConstraintPrimaryKeyNullsFirst(),
        ]
        validator = AstValidator(rules=rules)
        super().__init__(dialect="duckdb", ast_validator=validator, **convert_options)
