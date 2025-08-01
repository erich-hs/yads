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
    """Converts a SchemaSpec into a SQL DDL string for a specific dialect.

    High-level convenience converter that uses a core converter
    (e.g. SQLGlotConverter) to generate an Abstract Syntax Tree (AST)
    before serializing it to a SQL DDL string.
    """

    def __init__(
        self,
        dialect: str,
        ast_converter: BaseConverter | None = None,
        ast_validator: AstValidator | None = None,
        **convert_options: Any,
    ):
        """
        Args:
            dialect: The target SQL dialect (e.g., "spark", "snowflake", "duckdb").
            ast_converter: Optional. An AST converter to use. If None, a default
                           SQLGlotConverter will be used.
            ast_validator: Optional. A processor for dialect-specific
                               validation and adjustments.
            convert_options: Keyword arguments to be passed to the AST converter.
                             These can be overridden in the `convert` method. See
                             sqlglot's documentation for available options:
                             https://sqlglot.com/sqlglot/generator.html#Generator
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
        """Converts a yads SchemaSpec into a SQL DDL string.

        Args:
            spec: The SchemaSpec object.
            if_not_exists: If True, adds `IF NOT EXISTS` to the DDL statement.
            or_replace: If True, adds `OR REPLACE` to the DDL statement.
            mode: The validation mode for the dialect processor.
                - "strict": Raises an error for any unsupported feature.
                - "fix": Logs a warning and adjusts the AST to be compatible.
                - "warn": Logs a warning for any unsupported feature without
                          adjusting the AST.
            kwargs: Keyword arguments for the AST converter, overriding any
                    options from initialization. See sqlglot's documentation for
                    available options:
                    https://sqlglot.com/sqlglot/generator.html#Generator

        Returns:
            A SQL DDL string formatted for the specified dialect.
        """
        ast = self._ast_converter.convert(
            spec, if_not_exists=if_not_exists, or_replace=or_replace
        )
        if self._ast_validator:
            ast = self._ast_validator.validate(ast, mode=mode)
        options = {**self._convert_options, **kwargs}
        return ast.sql(dialect=self._dialect, **options)


class SparkSQLConverter(SQLConverter):
    """Converter for generating Spark SQL DDL.

    This converter adds Spark-specific validation before generating the DDL.
    """

    def __init__(self, **convert_options: Any):
        """
        Args:
            convert_options: Keyword arguments to be passed to the AST converter.
                             These can be overridden in the `convert` method.
        """
        rules: List[Rule] = [NoFixedLengthStringRule()]
        validator = AstValidator(rules=rules)
        super().__init__(dialect="spark", ast_validator=validator, **convert_options)


class SQLGlotConverter(BaseConverter):
    """Converts a yads SchemaSpec into a sqlglot Abstract Syntax Tree (AST)."""

    # Class-level constants for transform handlers
    _TRANSFORM_HANDLERS: dict[str, str] = {
        "bucket": "_handle_bucket_transform",
        "truncate": "_handle_truncate_transform",
        "cast": "_handle_cast_transform",
    }

    def convert(self, spec: SchemaSpec, **kwargs: Any) -> exp.Create:
        """Converts a yads SchemaSpec into a sqlglot Create AST.

        Args:
            spec: The yads specification.
            kwargs: Additional keyword arguments for the converter.
                    `if_not_exists` (bool): If True, adds `IF NOT EXISTS`.
                    `or_replace` (bool): If True, adds `OR REPLACE`.

        Returns:
            A sqlglot Create AST.
            https://sqlglot.com/sqlglot/expressions.html#Create
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
        """Converts a yads type to a sqlglot DataType expression.

        Fallback method for simple types that use their string representation.
        """
        # https://sqlglot.com/sqlglot/expressions.html#DataType.build
        return exp.DataType.build(str(yads_type))

    @_convert_type.register(Integer)
    def _(self, yads_type: Integer) -> exp.DataType:
        """Convert Integer types with bit-specific handling."""
        if yads_type.bits == 8:
            return exp.DataType(this=exp.DataType.Type.TINYINT)
        if yads_type.bits == 16:
            return exp.DataType(this=exp.DataType.Type.SMALLINT)
        if yads_type.bits == 64:
            return exp.DataType(this=exp.DataType.Type.BIGINT)
        return exp.DataType(this=exp.DataType.Type.INT)

    @_convert_type.register(Float)
    def _(self, yads_type: Float) -> exp.DataType:
        """Convert Float types with bit-specific handling."""
        if yads_type.bits == 64:
            return exp.DataType(this=exp.DataType.Type.DOUBLE)
        return exp.DataType(this=exp.DataType.Type.FLOAT)

    @_convert_type.register(String)
    def _(self, yads_type: String) -> exp.DataType:
        """Convert String types with optional length parameter."""
        expressions = []
        if yads_type.length:
            expressions.append(exp.DataTypeParam(this=convert(yads_type.length)))
        return exp.DataType(
            this=exp.DataType.Type.TEXT,
            expressions=expressions if expressions else None,
        )

    @_convert_type.register(Decimal)
    def _(self, yads_type: Decimal) -> exp.DataType:
        """Convert Decimal types with precision and scale parameters."""
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
        """Convert Interval types with time unit handling."""
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
        """Convert Array types with recursive element type conversion."""
        element_type = self._convert_type(yads_type.element)
        return exp.DataType(
            this=exp.DataType.Type.ARRAY,
            expressions=[element_type],
            nested=exp.DataType.Type.ARRAY in exp.DataType.NESTED_TYPES,
        )

    @_convert_type.register(Struct)
    def _(self, yads_type: Struct) -> exp.DataType:
        """Convert Struct types with recursive field conversion."""
        return exp.DataType(
            this=exp.DataType.Type.STRUCT,
            expressions=[self._convert_field(field) for field in yads_type.fields],
            nested=exp.DataType.Type.STRUCT in exp.DataType.NESTED_TYPES,
        )

    @_convert_type.register(Map)
    def _(self, yads_type: Map) -> exp.DataType:
        """Convert Map types with recursive key/value type conversion."""
        key_type = self._convert_type(yads_type.key)
        value_type = self._convert_type(yads_type.value)
        return exp.DataType(
            this=exp.DataType.Type.MAP,
            expressions=[key_type, value_type],
            nested=exp.DataType.Type.MAP in exp.DataType.NESTED_TYPES,
        )

    # Constraint handling using singledispatchmethod
    @singledispatchmethod
    def _convert_column_constraint(self, constraint: Any) -> exp.ColumnConstraint:
        """Convert a column constraint to a sqlglot ColumnConstraint expression.

        Fallback method for unsupported constraints.
        """
        # TODO: Revisit this after implementing a global setting to either
        # raise or warn when the spec is more expressive than the handlers
        # available in the converter.
        raise UnsupportedFeatureError(
            f"SQLGlotConverter does not support constraint: {type(constraint)}."
        )

    @_convert_column_constraint.register(NotNullConstraint)
    def _(self, constraint: NotNullConstraint) -> exp.ColumnConstraint:
        """Convert NotNull constraints."""
        return exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())

    @_convert_column_constraint.register(PrimaryKeyConstraint)
    def _(self, constraint: PrimaryKeyConstraint) -> exp.ColumnConstraint:
        """Convert PrimaryKey constraints."""
        return exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint())

    @_convert_column_constraint.register(DefaultConstraint)
    def _(self, constraint: DefaultConstraint) -> exp.ColumnConstraint:
        """Convert Default constraints."""
        return exp.ColumnConstraint(
            kind=exp.DefaultColumnConstraint(this=convert(constraint.value))
        )

    @_convert_column_constraint.register(IdentityConstraint)
    def _(self, constraint: IdentityConstraint) -> exp.ColumnConstraint:
        """Convert Identity constraints with start and increment handling."""
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
        """Convert ForeignKey constraints."""
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

    # Table constraint handling using singledispatchmethod
    @singledispatchmethod
    def _convert_table_constraint(self, constraint: Any) -> exp.Expression:
        """Convert a table constraint to a sqlglot Expression.

        Fallback method for unsupported table constraints.
        """
        # TODO: Revisit this after implementing a global setting to either
        # raise or warn when the spec is more expressive than the handlers
        # available in the converter.
        raise UnsupportedFeatureError(
            f"SQLGlotConverter does not support table constraint: {type(constraint)}."
        )

    @_convert_table_constraint.register(PrimaryKeyTableConstraint)
    def _(self, constraint: PrimaryKeyTableConstraint) -> exp.Expression:
        """Convert PrimaryKey table constraints."""
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
        """Convert ForeignKey table constraints."""
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
        """Convert storage configuration to a list of Property expressions."""
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
        """Convert partitioned-by configuration to a PartitionedByProperty expression."""
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
        """Convert location string to a LocationProperty expression."""
        return exp.LocationProperty(this=convert(value))

    def _handle_file_format_property(self, value: str) -> exp.FileFormatProperty:
        """Convert file format string to a FileFormatProperty expression."""
        return exp.FileFormatProperty(this=exp.Var(this=value))

    def _handle_external_property(self) -> exp.ExternalProperty:
        """Create an ExternalProperty expression."""
        return exp.ExternalProperty()

    def _handle_generic_property(self, key: str, value: Any) -> exp.Property:
        """Convert key-value pair to a generic Property expression."""
        return exp.Property(this=convert(key), value=convert(value))

    def _collect_properties(self, spec: SchemaSpec) -> list[exp.Property]:
        """Gathers all table-level properties from the spec."""
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
        """Handles a transformed column by dispatching to a specific handler or
        falling back to a generic function expression.
        """
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
        """Handle 'cast' transform with type conversion."""
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
        """Handle 'bucket' transform for partitioning."""
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
        """Handle 'truncate' transform for partitioning."""
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
        """Convert a Field to a ColumnDef expression with constraints."""
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
        """Gathers all table-level expressions from the spec."""
        expressions: list[exp.Expression] = [
            self._convert_field(col) for col in spec.columns
        ]
        for constraint in spec.table_constraints:
            expressions.append(self._convert_table_constraint(constraint))
        return expressions

    def _parse_full_table_name(
        self, full_name: str, columns: list[str] | None = None
    ) -> exp.Table | exp.Schema:
        """Parses a qualified table name into a sqlglot Table or Schema expression.
        If columns are provided, a Schema expression is returned.

        Args:
            full_name: The qualified table name.
            columns: Optional. The columns to include in the Schema expression.

        Returns:
            A sqlglot Table or Schema expression.
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
