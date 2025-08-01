"""SQL DDL converters for transforming yads specifications into SQL statements.

This module provides converters that transform yads SchemaSpec objects into SQL DDL
(Data Definition Language) statements for various database dialects. The conversion
process follows a multi-stage pipeline designed to handle dialect-specific features
and constraints while maintaining the expressiveness of the original specification.

The conversion process follows a "wide-to-narrow" approach, from the most expressive
yads specification to a constrained SQL DDL string:
    - SQLGLotConverter: Core converter that transforms the canonical yads specification
        into a sqlglot AST (Abstract Syntax Tree). The AST is the intermediate representation
        that is used by all other converters in this module. It's the only converter in this
        module that returns something other than a SQL DDL string.
    - SQLConverter: Base class for all SQL converters, it's responsible for orchestrating
        the conversion and validation process. It's also able to generate loosely validated
        SQL DDL strings in any dialect supported by the available version of sqlglot.
    - SparkSQLConverter: Specialized converter for Apache Spark SQL with strict validation.

Example:
    >>> import yads
    >>> from yads.converters.sql import SparkSQLConverter
    >>>
    >>> # Load schema specification
    >>> spec = yads.from_yaml("table_spec.yaml")
    >>>
    >>> # Convert to Spark SQL DDL
    >>> converter = SparkSQLConverter()
    >>> ddl = converter.convert(spec, pretty=True, if_not_exists=True)
    >>> print(ddl)
    CREATE TABLE IF NOT EXISTS my_table (
        id BIGINT NOT NULL,
        name STRING,
        created_at TIMESTAMP
    )
"""

from __future__ import annotations

from functools import singledispatchmethod
from typing import Any, List, Literal

from sqlglot import exp
from sqlglot.expressions import convert

from yads.constraints import (
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
)
from yads.converters.base import BaseConverter
from yads.exceptions import ConversionError, UnsupportedFeatureError
from yads.spec import Field, SchemaSpec, Storage, TransformedColumn
from yads.types import (
    Array,
    Decimal,
    Float,
    Integer,
    Interval,
    Map,
    String,
    Struct,
    Type,
)
from yads.validator import AstValidator, NoFixedLengthStringRule, Rule


class SQLConverter:
    """Base class for all SQL converters.

    SQLConverter orchestrates the conversion pipeline by using an AST converter
    to generate a sqlglot AST, applying optional validation and adjustment rules,
    and finally serializing to a SQL string.

    The converter supports any SQL dialect supported by the available version of
    sqlglot and allows for custom validation rules to handle dialect-specific
    limitations. The validation can operate in different modes to either raise
    errors, automatically fix issues, or warn about incompatibilities.

    Attributes:
        _ast_converter: The intermediate AST converter. Defaults to SQLGlotConverter.
        _dialect: Target SQL dialect name for sqlglot serialization.
        _ast_validator: Optional validator for dialect-specific AST processing.
        _convert_options: Default options for SQL string generation.

    Example:
        >>> from yads.converters.sql import SQLConverter
        >>> from yads.validator import AstValidator
        >>>
        >>> # Create converter with custom validation
        >>> converter = SQLConverter(
        ...     dialect="snowflake",
        ...     ast_validator=AstValidator(rules=[CustomRule()]),
        ...     pretty=True
        ... )
        >>> ddl = converter.convert(spec)
    """

    def __init__(
        self,
        dialect: str,
        ast_converter: BaseConverter | None = None,
        ast_validator: AstValidator | None = None,
        **convert_options: Any,
    ):
        """Initialize the SQL converter with an optional AST converter and validator
        and default options for sqlglot's SQL generator.

        Args:
            dialect: Target sqlglot SQL dialect name. See sqlglot's documentation for
                     supported dialects: https://sqlglot.com/sqlglot/dialects.html
            ast_converter: AST converter for transforming SchemaSpec to sqlglot AST.
                          Defaults to SQLGlotConverter.
            ast_validator: Validator for applying dialect-specific rules and adjustments.
                          If None, no validation or adjustment will be performed.
            **convert_options: Default options passed to sqlglot's SQL generator.
                              See sqlglot's documentation for supported options:
                              https://sqlglot.com/sqlglot/generator.html#Generator

        Example:
            >>> # Basic converter for Spark SQL
            >>> converter = SQLConverter("spark")
            >>>
            >>> # Converter with custom formatting
            >>> converter = SQLConverter(
            ...     "snowflake",
            ...     pretty=True,
            ...     identify=True
            ... )
        """
        self._ast_converter = ast_converter or SQLGlotConverter()
        self._dialect = dialect
        self._ast_validator = ast_validator
        self._convert_options = convert_options

    def convert(
        self,
        spec: SchemaSpec,
        if_not_exists: bool = False,
        or_replace: bool = False,
        mode: Literal["strict", "fix", "warn"] = "fix",
        **kwargs: Any,
    ) -> str:
        """Convert a yads SchemaSpec into a SQL DDL string.

        This method orchestrates the complete conversion pipeline from SchemaSpec
        to SQL DDL. It first converts the spec to a sqlglot AST, applies any
        configured validation rules, and finally serializes to a SQL string.

        Args:
            spec: The yads specification as a SchemaSpec object.
            if_not_exists: If True, add `IF NOT EXISTS` clause to the DDL statement.
            or_replace: If True, add `OR REPLACE` clause to the DDL statement.
            mode: Validation mode when an ast_validator is configured:
                - "strict": Raise ValidationRuleError for any unsupported features.
                - "fix": Log warnings and automatically adjust AST for compatibility.
                - "warn": Log warnings without modifying the AST.
            **kwargs: Additional options for SQL generation, overriding defaults.
                      See sqlglot's documentation for supported options:
                      https://sqlglot.com/sqlglot/generator.html#Generator

        Returns:
            SQL DDL CREATE TABLE statement as a string.

        Raises:
            ValidationRuleError: In strict mode when unsupported features are detected.
            ConversionError: When the underlying conversion process fails.

        Example:
            >>> ddl = converter.convert(
            ...     spec,
            ...     if_not_exists=True,
            ...     pretty=True
            ... )
        """
        ast = self._ast_converter.convert(
            spec, if_not_exists=if_not_exists, or_replace=or_replace
        )
        if self._ast_validator:
            ast = self._ast_validator.validate(ast, mode=mode)
        options = {**self._convert_options, **kwargs}
        return ast.sql(dialect=self._dialect, **options)


class SparkSQLConverter(SQLConverter):
    """Specialized SQL converter for Apache Spark SQL with built-in validation.

    SparkSQLConverter extends SQLConverter to provide Spark-specific validation
    rules to ensure compatibility with Spark SQL's feature set.

    The converter applies the following validation rules:
    - NoFixedLengthStringRule: Disallow fixed-length string types (CHAR/VARCHAR with length).

    Example:
        >>> from yads.converters.sql import SparkSQLConverter
        >>>
        >>> # Create Spark-specific converter
        >>> converter = SparkSQLConverter(pretty=True)
        >>> ddl = converter.convert(spec, if_not_exists=True)
        >>>
        >>> # Result will be valid Spark SQL with appropriate adjustments
        >>> print(ddl)
        CREATE TABLE IF NOT EXISTS my_table (
            id BIGINT,
            name STRING  -- Fixed-length strings converted to STRING
        ) USING DELTA
    """

    def __init__(self, **convert_options: Any):
        """Initialize Spark SQL converter with built-in validation rules.

        Creates a SparkSQLConverter configured with Spark-specific validation
        rules and the "spark" dialect.

        Args:
            **convert_options: Options for SQL generation passed to sqlglot.
                              See sqlglot's documentation for supported options:
                              https://sqlglot.com/sqlglot/generator.html#Generator

        Example:
            >>> # Basic Spark converter
            >>> converter = SparkSQLConverter()
            >>>
            >>> # Converter with formatted output
            >>> converter = SparkSQLConverter(pretty=True)
        """
        rules: List[Rule] = [NoFixedLengthStringRule()]
        validator = AstValidator(rules=rules)
        super().__init__(dialect="spark", ast_validator=validator, **convert_options)


class SQLGlotConverter(BaseConverter):
    """Core converter that transforms yads specifications into sqlglot AST expressions.

    SQLGlotConverter is the foundational converter that handles the transformation
    from yads' high-level schema specifications to sqlglot's Abstract Syntax Tree
    representation. This AST serves as a dialect-agnostic intermediate representation
    that can then be serialized into SQL for specific database systems.

    The converter uses single dispatch methods to handle different yads types,
    constraints, and schema elements, providing extensible type mapping and
    constraint conversion. It maintains the full expressiveness of the yads
    specification while producing valid sqlglot AST nodes.

    Key responsibilities:
    - Transform yads types to sqlglot `exp.DataType` expressions
    - Convert column and table constraints to sqlglot `exp.ColumnConstraint` and
      `exp.Constraint` nodes.
    - Handle complex types (arrays, structs, maps) with proper nesting.
    - Process table properties, partitioning, and storage configurations.
    - Generate appropriate `exp.Create` AST structures.

    The converter supports all yads type system features including primitive types,
    complex nested types, constraints, generated columns, partitioning transforms,
    and storage properties. It serves as the core engine for all SQL DDL generation
    in yads.

    Example:
        >>> converter = SQLGlotConverter()
        >>> ast = converter.convert(spec, if_not_exists=True)
        >>> print(type(ast))
        <class 'sqlglot.expressions.Create'>
        >>> sql = ast.sql(dialect="spark")
    """

    # Class-level constants for transform handlers
    _TRANSFORM_HANDLERS: dict[str, str] = {
        "bucket": "_handle_bucket_transform",
        "truncate": "_handle_truncate_transform",
        "cast": "_handle_cast_transform",
    }

    def convert(self, spec: SchemaSpec, **kwargs: Any) -> exp.Create:
        """Convert a yads SchemaSpec into a sqlglot `exp.Create` AST expression.

        This method performs the core transformation from yads' specification format
        to sqlglot's AST representation. It processes all aspects of the schema
        including columns, constraints, storage properties, and table metadata.

        The resulting AST is dialect-agnostic and can be serialized to SQL for
        any database system supported by sqlglot. The conversion preserves all
        schema information and applies appropriate sqlglot expression types.

        Args:
            spec: The yads specification as a SchemaSpec object.
            **kwargs: Optional conversion modifiers:
                if_not_exists: If True, sets the `exists` property of the `exp.Create`
                    node to `True`.
                or_replace: If True, sets the `replace` property of the `exp.Create`
                    node to `True`.

        Returns:
            sqlglot `exp.Create` expression representing a CREATE TABLE statement.
            The AST includes table schema, constraints, properties, and metadata
            from the yads specification.

        Example:
            >>> converter = SQLGlotConverter()
            >>> ast = converter.convert(spec, if_not_exists=True)
            >>> print(type(ast))
            <class 'sqlglot.expressions.Create'>
            >>> print(ast.sql(dialect="spark"))
            CREATE TABLE IF NOT EXISTS ...
        """
        table = self._parse_full_table_name(spec.name)
        properties = self._collect_properties(spec)
        expressions = self._collect_expressions(spec)

        return exp.Create(
            this=exp.Schema(this=table, expressions=expressions),
            kind="TABLE",
            exists=kwargs.get("if_not_exists", False) or None,
            replace=kwargs.get("or_replace", False) or None,
            properties=(exp.Properties(expressions=properties) if properties else None),
        )

    @singledispatchmethod
    def _convert_type(self, yads_type: Type) -> exp.DataType:
        """Convert a yads Type to a sqlglot `exp.DataType` expression.

        This is the main dispatch method for type conversion. It uses single
        dispatch to delegate to specialized handlers for each yads type.
        The fallback implementation delegates to sqlglot's DataType.build
        method for basic type conversion.

        Args:
            yads_type: The yads type to convert.

        Returns:
            sqlglot `exp.DataType` expression representing the equivalent SQL type.

        Note:
            This method serves as the base case for single dispatch. Specific
            type handlers are registered using `@_convert_type.register(TypeClass)`.
        """
        # Fallback to default sqlglot DataType.build method.
        # https://sqlglot.com/sqlglot/expressions.html#DataType.build
        return exp.DataType.build(str(yads_type))

    @_convert_type.register(Integer)
    def _(self, yads_type: Integer) -> exp.DataType:
        if yads_type.bits == 8:
            return exp.DataType(this=exp.DataType.Type.TINYINT)
        if yads_type.bits == 16:
            return exp.DataType(this=exp.DataType.Type.SMALLINT)
        if yads_type.bits == 64:
            return exp.DataType(this=exp.DataType.Type.BIGINT)
        # Default to INT for 32-bit or unspecified
        return exp.DataType(this=exp.DataType.Type.INT)

    @_convert_type.register(Float)
    def _(self, yads_type: Float) -> exp.DataType:
        if yads_type.bits == 64:
            return exp.DataType(this=exp.DataType.Type.DOUBLE)
        return exp.DataType(this=exp.DataType.Type.FLOAT)

    @_convert_type.register(String)
    def _(self, yads_type: String) -> exp.DataType:
        expressions = []
        if yads_type.length:
            expressions.append(exp.DataTypeParam(this=convert(yads_type.length)))
        return exp.DataType(
            this=exp.DataType.Type.TEXT,
            expressions=expressions if expressions else None,
        )

    @_convert_type.register(Decimal)
    def _(self, yads_type: Decimal) -> exp.DataType:
        expressions = []
        if yads_type.precision is not None:
            expressions.append(exp.DataTypeParam(this=convert(yads_type.precision)))
            expressions.append(exp.DataTypeParam(this=convert(yads_type.scale)))

        return exp.DataType(
            this=exp.DataType.Type.DECIMAL,
            expressions=expressions if expressions else None,
        )

    @_convert_type.register(Interval)
    def _(self, yads_type: Interval) -> exp.DataType:
        if (
            yads_type.interval_end
            and yads_type.interval_start != yads_type.interval_end
        ):
            return exp.DataType(
                this=exp.Interval(
                    unit=exp.IntervalSpan(
                        this=exp.Var(this=yads_type.interval_start.value),
                        expression=exp.Var(this=yads_type.interval_end.value),
                    )
                )
            )
        return exp.DataType(
            this=exp.Interval(unit=exp.Var(this=yads_type.interval_start.value))
        )

    @_convert_type.register(Array)
    def _(self, yads_type: Array) -> exp.DataType:
        element_type = self._convert_type(yads_type.element)
        return exp.DataType(
            this=exp.DataType.Type.ARRAY,
            expressions=[element_type],
            nested=exp.DataType.Type.ARRAY in exp.DataType.NESTED_TYPES,
        )

    @_convert_type.register(Struct)
    def _(self, yads_type: Struct) -> exp.DataType:
        return exp.DataType(
            this=exp.DataType.Type.STRUCT,
            expressions=[self._convert_field(field) for field in yads_type.fields],
            nested=exp.DataType.Type.STRUCT in exp.DataType.NESTED_TYPES,
        )

    @_convert_type.register(Map)
    def _(self, yads_type: Map) -> exp.DataType:
        key_type = self._convert_type(yads_type.key)
        value_type = self._convert_type(yads_type.value)
        return exp.DataType(
            this=exp.DataType.Type.MAP,
            expressions=[key_type, value_type],
            nested=exp.DataType.Type.MAP in exp.DataType.NESTED_TYPES,
        )

    @singledispatchmethod
    def _convert_column_constraint(self, constraint: Any) -> exp.ColumnConstraint:
        """Convert a yads column constraint to a sqlglot `exp.ColumnConstraint` expression.

        This is the main dispatch method for column constraint conversion. It uses
        single dispatch to delegate to specialized handlers for each constraint type.
        The base implementation raises an error for unsupported constraints.

        Args:
            constraint: The yads column constraint to convert.

        Returns:
            sqlglot ColumnConstraint expression representing the SQL constraint.

        Raises:
            UnsupportedFeatureError: When the constraint type is not supported
                                   by this converter.

        Note:
            Specific constraint handlers are registered using
            `@_convert_column_constraint.register(ConstraintClass)`.
        """
        # TODO: Revisit this after implementing a global setting to either
        # raise or warn when the spec is more expressive than the handlers
        # available in the converter.
        raise UnsupportedFeatureError(
            f"SQLGlotConverter does not support constraint: {type(constraint)}."
        )

    @_convert_column_constraint.register(NotNullConstraint)
    def _(self, constraint: NotNullConstraint) -> exp.ColumnConstraint:
        return exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())

    @_convert_column_constraint.register(PrimaryKeyConstraint)
    def _(self, constraint: PrimaryKeyConstraint) -> exp.ColumnConstraint:
        return exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint())

    @_convert_column_constraint.register(DefaultConstraint)
    def _(self, constraint: DefaultConstraint) -> exp.ColumnConstraint:
        return exp.ColumnConstraint(
            kind=exp.DefaultColumnConstraint(this=convert(constraint.value))
        )

    @_convert_column_constraint.register(IdentityConstraint)
    def _(self, constraint: IdentityConstraint) -> exp.ColumnConstraint:
        start_expr: exp.Expression | None = None
        if constraint.start is not None:
            if constraint.start < 0:
                start_expr = exp.Neg(this=convert(abs(constraint.start)))
            else:
                start_expr = convert(constraint.start)

        increment_expr: exp.Expression | None = None
        if constraint.increment is not None:
            if constraint.increment < 0:
                increment_expr = exp.Neg(this=convert(abs(constraint.increment)))
            else:
                increment_expr = convert(constraint.increment)

        return exp.ColumnConstraint(
            kind=exp.GeneratedAsIdentityColumnConstraint(
                this=constraint.always,
                start=start_expr,
                increment=increment_expr,
            )
        )

    @_convert_column_constraint.register(ForeignKeyConstraint)
    def _(self, constraint: ForeignKeyConstraint) -> exp.ColumnConstraint:
        reference_expression = exp.Reference(
            this=self._parse_full_table_name(
                constraint.references.table, constraint.references.columns
            ),
        )

        if constraint.name:
            return exp.ColumnConstraint(
                this=exp.Identifier(this=constraint.name),
                kind=reference_expression,
            )

        return exp.ColumnConstraint(kind=reference_expression)

    @singledispatchmethod
    def _convert_table_constraint(self, constraint: Any) -> exp.Expression:
        """Convert a yads table constraint to a sqlglot `exp.Expression` expression.

        This is the main dispatch method for table-level constraint conversion.
        Table constraints affect multiple columns and are defined at the table
        level rather than on individual columns.

        Args:
            constraint: The yads table constraint to convert.

        Returns:
            sqlglot Expression representing the SQL table constraint.

        Raises:
            UnsupportedFeatureError: When the constraint type is not supported
                                   by this converter.

        Note:
            Specific table constraint handlers are registered using
            `@_convert_table_constraint.register(ConstraintClass)`.
        """
        # TODO: Revisit this after implementing a global setting to either
        # raise or warn when the spec is more expressive than the handlers
        # available in the converter.
        raise UnsupportedFeatureError(
            f"SQLGlotConverter does not support table constraint: {type(constraint)}."
        )

    @_convert_table_constraint.register(PrimaryKeyTableConstraint)
    def _(self, constraint: PrimaryKeyTableConstraint) -> exp.Expression:
        pk_expression = exp.PrimaryKey(
            expressions=[
                exp.Ordered(
                    this=exp.Column(this=exp.Identifier(this=c)),
                    nulls_first=True,
                )
                for c in constraint.columns
            ]
        )
        if constraint.name:
            return exp.Constraint(
                this=exp.Identifier(this=constraint.name), expressions=[pk_expression]
            )
        return pk_expression

    @_convert_table_constraint.register(ForeignKeyTableConstraint)
    def _(self, constraint: ForeignKeyTableConstraint) -> exp.Expression:
        reference_expression = exp.Reference(
            this=self._parse_full_table_name(
                constraint.references.table, constraint.references.columns
            ),
        )
        fk_expression = exp.ForeignKey(
            expressions=[exp.Identifier(this=c) for c in constraint.columns],
            reference=reference_expression,
        )

        if constraint.name:
            return exp.Constraint(
                this=exp.Identifier(this=constraint.name),
                expressions=[fk_expression],
            )
        return fk_expression

    # Property handlers
    def _handle_storage_properties(self, storage: Storage | None) -> list[exp.Property]:
        if not storage:
            return []

        properties: list[exp.Property] = []
        if storage.location:
            properties.append(self._handle_location_property(storage.location))
        if storage.format:
            properties.append(self._handle_file_format_property(storage.format))
        if storage.tbl_properties:
            for key, value in storage.tbl_properties.items():
                properties.append(self._handle_generic_property(key, value))

        return properties

    def _handle_partitioned_by_property(
        self, value: list[TransformedColumn]
    ) -> exp.PartitionedByProperty:
        schema_expressions = []
        for col in value:
            expression: exp.Expression
            if col.transform:
                expression = self._handle_transformation(
                    col.column, col.transform, col.transform_args
                )
            else:
                expression = exp.Identifier(this=col.column)
            schema_expressions.append(expression)

        return exp.PartitionedByProperty(
            this=exp.Schema(expressions=schema_expressions)
        )

    def _handle_location_property(self, value: str) -> exp.LocationProperty:
        return exp.LocationProperty(this=convert(value))

    def _handle_file_format_property(self, value: str) -> exp.FileFormatProperty:
        return exp.FileFormatProperty(this=exp.Var(this=value))

    def _handle_external_property(self) -> exp.ExternalProperty:
        return exp.ExternalProperty()

    def _handle_generic_property(self, key: str, value: Any) -> exp.Property:
        return exp.Property(this=convert(key), value=convert(value))

    def _collect_properties(self, spec: SchemaSpec) -> list[exp.Property]:
        properties: list[exp.Property] = []
        if spec.external:
            properties.append(self._handle_external_property())
        properties.extend(self._handle_storage_properties(spec.storage))
        if spec.partitioned_by:
            properties.append(self._handle_partitioned_by_property(spec.partitioned_by))
        return properties

    # Transform handlers
    def _handle_transformation(
        self, column: str, transform: str, transform_args: list
    ) -> exp.Expression:
        if handler_method_name := self._TRANSFORM_HANDLERS.get(transform):
            handler_method = getattr(self, handler_method_name)
            return handler_method(column, transform_args)

        # Fallback to a generic function expression for all other transforms.
        # https://sqlglot.com/sqlglot/expressions.html#func
        return exp.func(
            transform,
            exp.column(column),
            *(exp.convert(arg) for arg in transform_args),
        )

    def _handle_cast_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        if len(transform_args) != 1:
            raise ConversionError(
                f"The 'cast' transform requires exactly one argument. Got {len(transform_args)}."
            )
        return exp.Cast(
            this=exp.column(column),
            to=exp.DataType(this=exp.DataType.Type[transform_args[0]]),
        )

    def _handle_bucket_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        if len(transform_args) != 1:
            raise ConversionError(
                f"The 'bucket' transform requires exactly one argument. Got {len(transform_args)}."
            )
        return exp.PartitionedByBucket(
            this=exp.column(column),
            expression=exp.convert(transform_args[0]),
        )

    def _handle_truncate_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        if len(transform_args) != 1:
            raise ConversionError(
                f"The 'truncate' transform requires exactly one argument. Got {len(transform_args)}."
            )
        return exp.PartitionByTruncate(
            this=exp.column(column),
            expression=exp.convert(transform_args[0]),
        )

    # Field and expression collection
    def _convert_field(self, field: Field) -> exp.ColumnDef:
        constraints = []

        # Handle generated columns
        if field.generated_as and field.generated_as.transform:
            expression = self._handle_transformation(
                field.generated_as.column,
                field.generated_as.transform,
                field.generated_as.transform_args,
            )
            constraints.append(
                exp.ColumnConstraint(
                    kind=exp.GeneratedAsIdentityColumnConstraint(
                        this=True, expression=expression
                    )
                )
            )

        # Handle field constraints using single dispatch
        for constraint in field.constraints:
            constraints.append(self._convert_column_constraint(constraint))

        return exp.ColumnDef(
            this=exp.Identifier(this=field.name),
            kind=self._convert_type(field.type),
            constraints=constraints if constraints else None,
        )

    def _collect_expressions(self, spec: SchemaSpec) -> list[exp.Expression]:
        expressions: list[exp.Expression] = [
            self._convert_field(col) for col in spec.columns
        ]
        for constraint in spec.table_constraints:
            expressions.append(self._convert_table_constraint(constraint))
        return expressions

    def _parse_full_table_name(
        self, full_name: str, columns: list[str] | None = None
    ) -> exp.Table | exp.Schema:
        """Parse a qualified table name into a sqlglot `exp.Table` or `exp.Schema` expression.

        Parses table names in the format 'catalog.database.table' or 'database.table'
        or 'table' and creates appropriate sqlglot expressions. When columns are
        provided, returns a `exp.Schema` expression suitable for constraint references.

        Args:
            full_name: Qualified table name with optional catalog and database.
                      Supports formats: 'table', 'database.table', 'catalog.database.table'.
            columns: Column names to include in Schema expression. When provided,
                    returns a `exp.Schema` expression instead of `exp.Table` expression.

        Returns:
            sqlglot `exp.Table` expression when columns is None, or `exp.Schema` expression
            when columns are provided. The expression includes proper catalog,
            database, and table identifiers.

        Example:
            >>> converter._parse_full_table_name("prod.sales.orders")
            Table(catalog=Identifier(this='prod'), db=Identifier(this='sales'),
                  this=Identifier(this='orders'))
            >>> converter._parse_full_table_name("orders", ["id", "name"])
            Schema(this=Table(...), expressions=[Identifier(this='id'), ...])
        """
        parts = full_name.split(".")
        table_name = parts[-1]
        db_name = parts[-2] if len(parts) > 1 else None
        catalog_name = parts[-3] if len(parts) > 2 else None

        table_expression = exp.Table(
            this=exp.Identifier(this=table_name),
            db=exp.Identifier(this=db_name) if db_name else None,
            catalog=exp.Identifier(this=catalog_name) if catalog_name else None,
        )
        if columns:
            return exp.Schema(
                this=table_expression,
                expressions=[exp.Identifier(this=c) for c in columns],
            )
        return table_expression
