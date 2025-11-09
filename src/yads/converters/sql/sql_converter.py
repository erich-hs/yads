"""SQL converters orchestrating AST conversion and SQL generation.

This module contains high-level SQL converters that:
- Build an Abstract Syntax Tree (AST) from a `YadsSpec` using an AST converter
- Optionally validate/adjust the AST using AST validation rules
- Serialize the final AST to a SQL string for a target dialect

Supported SQL dialects:
- Spark
- DuckDB

Other SQL dialects can be extended via the `SQLConverter` class.
"""

from __future__ import annotations

from typing import Any, Literal, TYPE_CHECKING
from dataclasses import dataclass, replace

from ..base import BaseConverter, BaseConverterConfig
from ..._dependencies import requires_dependency, ensure_dependency

if TYPE_CHECKING:
    from ...spec import YadsSpec
    from .ast_converter import AstConverter, SQLGlotConverterConfig
    from .validators.ast_validator import AstValidator
    from .validators.ast_validation_rules import AstValidationRule


# %% ---- Configuration --------------------------------------------------------------
@dataclass(frozen=True)
class SQLConverterConfig(BaseConverterConfig):
    """Configuration for SQLConverter.

    Args:
        mode: Conversion mode. One of "raise" or "coerce". Inherited from
            BaseConverterConfig. Defaults to "coerce".
        ignore_columns: Column names to exclude from conversion. Inherited from
            BaseConverterConfig. Defaults to empty.
        include_columns: If provided, only these columns are included. Inherited
            from BaseConverterConfig. Defaults to None.
        column_overrides: Mapping of column name to a callable that returns a
            custom AST column definition for the chosen AST converter. Inherited
            from BaseConverterConfig. Defaults to empty mapping.
        dialect: Target SQL dialect name for SQL generation.
        ast_converter: AST converter instance to use for spec-to-AST transformation.
            If None, uses default SQLGlotConverter.
        ast_validator: Optional validator for dialect-specific adjustments.
    """

    dialect: str = "spark"
    ast_converter: AstConverter | None = None
    ast_validator: AstValidator | None = None
    # Optional configuration for the default AST converter when one is not supplied.
    # This allows routing user-facing options (e.g., if_not_exists, or_replace,
    # ignore_catalog, ignore_database, fallback_type, and column filters) into the
    # SQLGlotConverter while keeping SQLConverter decoupled from AST details.
    ast_converter_config: SQLGlotConverterConfig | None = None


# %% ---- Converters -----------------------------------------------------------------
class SQLConverter(BaseConverter):
    """Base class for SQL DDL generation.

    The converter composes:
    - An AST converter that transforms YadsSpec to dialect-agnostic AST.
    - An optional AST validator that enforces or adjusts dialect compatibility.

    The SQL converter accepts dialect-specific options via **kwargs in the convert()
    method, which are passed to the AST's sql() method for SQL generation.

    Example:
        >>> from yads.converters.sql import SQLConverter, SQLConverterConfig, AstValidator
        >>> from yads.converters.sql.validators.ast_validation_rules import DisallowType
        >>>
        >>> # Create converter with custom validation
        >>> config = SQLConverterConfig(
        ...     dialect="spark",
        ...     ast_validator=AstValidator(rules=[DisallowType(...)]),
        ... )
        >>> converter = SQLConverter(config)
        >>> ddl = converter.convert(spec, pretty=True, mode="raise")
    """

    def __init__(self, config: SQLConverterConfig | None = None):
        """Initialize the SQLConverter.

        Args:
            config: Configuration object. If None, uses default SQLConverterConfig.
        """
        from .ast_converter import SQLGlotConverter, SQLGlotConverterConfig

        self.config: SQLConverterConfig = config or SQLConverterConfig()
        super().__init__(self.config)

        # Use provided AST converter or create default SQLGlotConverter
        if self.config.ast_converter:
            self._ast_converter = self.config.ast_converter
        else:
            # Prefer the provided ast_converter_config; ensure mode consistency.
            base_ast_config = self.config.ast_converter_config or SQLGlotConverterConfig()
            ast_config = replace(base_ast_config, mode=self.config.mode)
            self._ast_converter = SQLGlotConverter(ast_config)

    def convert(
        self,
        spec: YadsSpec,
        *,
        mode: Literal["raise", "coerce"] | None = None,
        **kwargs: Any,
    ) -> str:
        """Convert a yads `YadsSpec` into a SQL DDL string.

        This method orchestrates the conversion pipeline from `YadsSpec` to SQL DDL.
        It first converts the spec to an intermediate AST, applies any configured
        validation rules, and finally serializes to a SQL DDL string.

        Args:
            spec: The yads specification as a `YadsSpec` object.
            mode: Optional validation mode override for this call. When not
                provided, the converter's configured mode is used. If provided:
                - "raise": Raise on any unsupported features.
                - "coerce": Apply adjustments to produce a valid AST and emit warnings.
            **kwargs: Additional options for SQL DDL string serialization.
                      Available options depend on the AST converter implementation.
                      For a `SQLGlotConverter`, see sqlglot's documentation for supported
                      options: https://sqlglot.com/sqlglot/generator.html#Generator

        Returns:
            SQL DDL CREATE TABLE statement as a string.

        Raises:
            ValidationRuleError: In raise mode when unsupported features are detected.
            ConversionError: When the underlying conversion process fails.

        Example:
            >>> from yads.converters.sql import SQLGlotConverter, SQLGlotConverterConfig
            >>> ast_converter = SQLGlotConverter(SQLGlotConverterConfig(if_not_exists=True))
            >>> config = SQLConverterConfig(ast_converter=ast_converter)
            >>> converter = SQLConverter(config)
            >>> ddl = converter.convert(spec, pretty=True)
        """
        from .ast_converter import SQLGlotConverter

        # Determine effective mode for this conversion call
        effective_mode: Literal["raise", "coerce"] = (
            mode if mode is not None else self.config.mode
        )

        # Convert spec to AST using the configured AST converter
        with self._ast_converter.conversion_context(mode=effective_mode):
            ast = self._ast_converter.convert(spec)

        # Apply optional AST validation
        if self.config.ast_validator:
            ast = self.config.ast_validator.validate(ast, mode=effective_mode)

        # Configure SQL generation options based on mode and AST converter type
        sql_options = dict(kwargs)
        if isinstance(self._ast_converter, SQLGlotConverter):
            ensure_dependency("sqlglot")

            from sqlglot import ErrorLevel

            match effective_mode:
                case "raise":
                    sql_options["unsupported_level"] = ErrorLevel.RAISE
                case "coerce":
                    sql_options["unsupported_level"] = ErrorLevel.WARN

        return ast.sql(dialect=self.config.dialect, **sql_options)


class SparkSQLConverter(SQLConverter):
    """Spark SQL converter with built-in validation rules.

    This converter is preconfigured for Spark SQL with:
    - dialect="spark"
    - Built-in validation rules:
      - Disallow unsigned integers → replace with signed integers
      - Disallow JSON → replace with STRING
      - Disallow GEOMETRY → replace with STRING
      - Disallow GEOGRAPHY → replace with STRING
      - Disallow UUID → replace with STRING
      - Disallow negative scale decimal → replace with a sum of the absolute value of the scale and the precision, with a scale of 0
      - Disallow fixed length binary → replace with BINARY
    """

    @requires_dependency("sqlglot", import_name="sqlglot.expressions")
    def __init__(
        self,
        *,
        mode: Literal["raise", "coerce"] = "coerce",
        ast_config: SQLGlotConverterConfig | None = None,
    ):
        """Initialize SparkSQLConverter with built-in Spark-specific settings.

        Args:
            mode: Conversion mode. "raise" will raise exceptions on unsupported features,
                  "coerce" will attempt to coerce unsupported features to supported ones
                  with warnings. Defaults to "coerce".
        """
        from sqlglot.expressions import DataType
        from .validators.ast_validator import AstValidator
        from .validators.ast_validation_rules import (
            DisallowType,
            DisallowNegativeScaleDecimal,
            DisallowFixedLengthBinary,
        )

        # Define Spark-specific validation rules
        rules: list[AstValidationRule] = [
            DisallowType(
                disallow_type=DataType.Type.UTINYINT,
                fallback_type=DataType.Type.TINYINT,
            ),
            DisallowType(
                disallow_type=DataType.Type.USMALLINT,
                fallback_type=DataType.Type.SMALLINT,
            ),
            DisallowType(
                disallow_type=DataType.Type.UINT,
                fallback_type=DataType.Type.INT,
            ),
            DisallowType(
                disallow_type=DataType.Type.UBIGINT,
                fallback_type=DataType.Type.BIGINT,
            ),
            DisallowType(
                disallow_type=DataType.Type.JSON,
            ),
            DisallowType(disallow_type=DataType.Type.GEOMETRY),
            DisallowType(disallow_type=DataType.Type.GEOGRAPHY),
            DisallowType(disallow_type=DataType.Type.UUID),
            DisallowNegativeScaleDecimal(),
            DisallowFixedLengthBinary(),
        ]
        validator = AstValidator(rules=rules)

        # Build config with Spark-specific settings
        spark_config = SQLConverterConfig(
            mode=mode,
            dialect="spark",
            ast_validator=validator,
            ast_converter_config=ast_config,
        )

        super().__init__(spark_config)


class DuckdbSQLConverter(SQLConverter):
    """DuckDB SQL converter with built-in validation rules.

    This converter is preconfigured for DuckDB with:
    - dialect="duckdb"
    - Built-in validation rules:
      - Disallow TimestampLTZ → replace with TimestampTZ
      - Disallow VOID → replace with STRING
      - Disallow GEOGRAPHY → replace with STRING
      - Disallow parametrized GEOMETRY → strip parameters
      - Disallow VARIANT → replace with STRING
      - Disallow column-level IDENTITY → remove constraint
      - Disallow NULLS FIRST in table-level PRIMARY KEY constraints → remove NULLS FIRST
      - Disallow fixed length binary → replace with BLOB
    """

    @requires_dependency("sqlglot", import_name="sqlglot.expressions")
    def __init__(
        self,
        *,
        mode: Literal["raise", "coerce"] = "coerce",
        ast_config: SQLGlotConverterConfig | None = None,
    ):
        """Initialize DuckdbSQLConverter with built-in DuckDB-specific settings.

        Args:
            mode: Conversion mode. "raise" will raise exceptions on unsupported features,
                  "coerce" will attempt to coerce unsupported features to supported ones
                  with warnings. Defaults to "coerce".
        """
        from sqlglot.expressions import DataType
        from .validators.ast_validator import AstValidator
        from .validators.ast_validation_rules import (
            DisallowType,
            DisallowUserDefinedType,
            DisallowParameterizedGeometry,
            DisallowColumnConstraintGeneratedIdentity,
            DisallowTableConstraintPrimaryKeyNullsFirst,
            DisallowNegativeScaleDecimal,
            DisallowFixedLengthBinary,
        )

        # Define DuckDB-specific validation rules
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
            DisallowNegativeScaleDecimal(),
            DisallowFixedLengthBinary(),
        ]
        validator = AstValidator(rules=rules)

        # Build config with DuckDB-specific settings
        duckdb_config = SQLConverterConfig(
            mode=mode,
            dialect="duckdb",
            ast_validator=validator,
            ast_converter_config=ast_config,
        )

        super().__init__(duckdb_config)
