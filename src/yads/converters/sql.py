from __future__ import annotations

from typing import Any, Callable

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
from yads.spec import Field, SchemaSpec, Storage, TransformedColumn
from yads.types import (
    Array,
    Decimal,
    Map,
    String,
    Struct,
    Type,
)


class SqlConverter:
    """Converts a SchemaSpec into a SQL DDL string for a specific dialect.

    High-level convenience converter that uses a core converter
    (e.g. SqlglotConverter) to generate an Abstract Syntax Tree (AST)
    before serializing it to a SQL DDL string.
    """

    def __init__(
        self,
        dialect: str,
        ast_converter: BaseConverter | None = None,
        **convert_options: Any,
    ):
        """
        Args:
            dialect: The target SQL dialect (e.g., "spark", "snowflake", "duckdb").
            ast_converter: Optional. An AST converter to use. If None, a default
                           SqlglotConverter will be used.
            convert_options: Keyword arguments to be passed to the AST converter.
                             These can be overridden in the `convert` method. See
                             sqlglot's documentation for available options:
                             https://sqlglot.com/sqlglot/generator.html#Generator
        """
        self._ast_converter = ast_converter or SqlglotConverter()
        self._dialect = dialect
        self._convert_options = convert_options

    def convert(self, spec: SchemaSpec, **kwargs: Any) -> str:
        """Converts a yads SchemaSpec into a SQL DDL string.

        Args:
            spec: The SchemaSpec object.
            kwargs: Keyword arguments for the AST converter, overriding any
                    options from initialization. See sqlglot's documentation for
                    available options:
                    https://sqlglot.com/sqlglot/generator.html#Generator

        Returns:
            A SQL DDL string formatted for the specified dialect.
        """
        self._validate(spec)
        ast = self._ast_converter.convert(spec)
        options = {**self._convert_options, **kwargs}
        return ast.sql(dialect=self._dialect, **options)

    def _validate(self, spec: SchemaSpec) -> None:
        """A hook for dialect-specific validation.

        Subclasses should override this method to implement dialect-specific
        validation logic.

        Args:
            spec: The SchemaSpec object to validate.
        """
        pass


class SparkSQLConverter(SqlConverter):
    """Converter for generating Spark SQL DDL.

    This converter adds Spark-specific validation before generating the DDL.
    """

    def __init__(self, **convert_options: Any):
        """
        Args:
            convert_options: Keyword arguments to be passed to the AST converter.
                             These can be overridden in the `convert` method.
        """
        super().__init__(dialect="spark", **convert_options)

    def _validate(self, spec: SchemaSpec) -> None:
        """Validates the SchemaSpec against Spark SQL compatibility.

        Raises:
            ValueError: If the spec contains features not supported by Spark.
        """
        # TODO: Add more validation logic here for types, other constraints, etc.
        pass


class SqlglotConverter(BaseConverter):
    """Converts a yads SchemaSpec into a sqlglot Abstract Syntax Tree (AST)."""

    def __init__(self) -> None:
        self._type_handlers: dict[type[Type], Callable[[Any], exp.DataType]] = {
            String: self._handle_string_type,
            Decimal: self._handle_decimal_type,
            Array: self._handle_array_type,
            Struct: self._handle_struct_type,
            Map: self._handle_map_type,
        }
        self._transform_handlers: dict[str, Callable[..., exp.Expression]] = {
            "bucket": self._handle_bucket_transform,
            "truncate": self._handle_truncate_transform,
            "cast": self._handle_cast_transform,
        }
        self._constraint_handlers: dict[type, Callable[[Any], exp.ColumnConstraint]] = {
            NotNullConstraint: self._handle_not_null_constraint,
            PrimaryKeyConstraint: self._handle_primary_key_constraint,
            DefaultConstraint: self._handle_default_constraint,
            ForeignKeyConstraint: self._handle_foreign_key_constraint,
            IdentityConstraint: self._handle_identity_constraint,
        }
        self._table_constraint_handlers: dict[type, Callable[[Any], exp.Expression]] = {
            PrimaryKeyTableConstraint: self._handle_primary_key_table_constraint,
            ForeignKeyTableConstraint: self._handle_foreign_key_table_constraint,
        }

    def convert(self, spec: SchemaSpec) -> exp.Create:
        """Converts a yads SchemaSpec into a sqlglot Create AST.

        Args:
            spec: The yads specification.

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
            exists=spec.options.if_not_exists,
            replace=spec.options.or_replace,
            properties=(exp.Properties(expressions=properties) if properties else None),
        )

    # Type handlers
    def _convert_type(self, yads_type: Type) -> exp.DataType:
        """Converts a yads type to a sqlglot DataType expression."""
        if handler := self._type_handlers.get(type(yads_type)):
            return handler(yads_type)
        # https://sqlglot.com/sqlglot/expressions.html#DataType.build
        return exp.DataType.build(str(yads_type))

    def _handle_string_type(self, yads_type: String) -> exp.DataType:
        if yads_type.length:
            return exp.DataType(
                this=exp.DataType.Type.VARCHAR,
                expressions=[exp.DataTypeParam(this=convert(yads_type.length))],
            )
        return exp.DataType(this=exp.DataType.Type.TEXT)

    def _handle_decimal_type(self, yads_type: Decimal) -> exp.DataType:
        expressions = []
        if yads_type.precision is not None:
            expressions.append(exp.DataTypeParam(this=convert(yads_type.precision)))
            expressions.append(exp.DataTypeParam(this=convert(yads_type.scale)))

        return exp.DataType(
            this=exp.DataType.Type.DECIMAL,
            expressions=expressions if expressions else None,
        )

    def _handle_array_type(self, yads_type: Array) -> exp.DataType:
        element_type = self._convert_type(yads_type.element)
        return exp.DataType(
            this=exp.DataType.Type.ARRAY,
            expressions=[element_type],
            nested=exp.DataType.Type.ARRAY in exp.DataType.NESTED_TYPES,
        )

    def _handle_struct_type(self, yads_type: Struct) -> exp.DataType:
        return exp.DataType(
            this=exp.DataType.Type.STRUCT,
            expressions=[self._convert_field(field) for field in yads_type.fields],
            nested=exp.DataType.Type.STRUCT in exp.DataType.NESTED_TYPES,
        )

    def _handle_map_type(self, yads_type: Map) -> exp.DataType:
        key_type = self._convert_type(yads_type.key)
        value_type = self._convert_type(yads_type.value)
        return exp.DataType(
            this=exp.DataType.Type.MAP,
            expressions=[key_type, value_type],
            nested=exp.DataType.Type.MAP in exp.DataType.NESTED_TYPES,
        )

    # Property handlers
    def _handle_storage_properties(self, storage: Storage | None) -> list[exp.Property]:
        if not storage:
            return []

        properties: list[exp.Property] = []
        if storage.format:
            properties.append(exp.FileFormatProperty(this=exp.Var(this=storage.format)))
        if storage.location:
            properties.append(exp.LocationProperty(this=convert(storage.location)))
        if storage.tbl_properties:
            for key, value in storage.tbl_properties.items():
                properties.append(exp.Property(this=convert(key), value=convert(value)))

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

    def _handle_transformation(
        self, column: str, transform: str, transform_args: list
    ) -> exp.Expression:
        """Handles a transformed column by dispatching to a specific handler or
        falling back to a generic function expression.
        """
        if handler := self._transform_handlers.get(transform):
            return handler(column, transform_args)

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
            raise ValueError("The 'cast' transform requires exactly one argument")
        return exp.Cast(
            this=exp.column(column),
            to=exp.DataType(this=exp.DataType.Type[transform_args[0]]),
        )

    def _handle_bucket_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        if len(transform_args) != 1:
            raise ValueError("The 'bucket' transform requires exactly one argument")
        return exp.PartitionedByBucket(
            this=exp.column(column),
            expression=exp.convert(transform_args[0]),
        )

    def _handle_truncate_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        if len(transform_args) != 1:
            raise ValueError("The 'truncate' transform requires exactly one argument")
        return exp.PartitionByTruncate(
            this=exp.column(column),
            expression=exp.convert(transform_args[0]),
        )

    def _handle_location_property(self, value: str) -> exp.LocationProperty:
        return exp.LocationProperty(this=exp.Literal(this=convert(value)))

    def _handle_external_property(self) -> exp.ExternalProperty:
        return exp.ExternalProperty()

    def _handle_generic_property(self, key: str, value: Any) -> exp.Property:
        return exp.Property(
            this=exp.Literal(this=convert(key)),
            value=exp.Literal(this=convert(value)),
        )

    def _collect_properties(self, spec: SchemaSpec) -> list[exp.Property]:
        """Gathers all table-level properties from the spec."""
        properties: list[exp.Property] = []

        if spec.options.is_external:
            properties.append(self._handle_external_property())

        properties.extend(self._handle_storage_properties(spec.storage))
        if spec.partitioned_by:
            properties.append(self._handle_partitioned_by_property(spec.partitioned_by))
        return properties

    # Field and constraint handlers
    def _convert_field(self, field: Field) -> exp.ColumnDef:
        constraints = []
        if field.generated_as:
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

        for constraint in field.constraints:
            if handler := self._constraint_handlers.get(type(constraint)):
                constraints.append(handler(constraint))
            else:
                # TODO: Revisit this after implementing a global setting to either
                # raise or warn when the spec is more expressive than the handlers
                # available in a core converter.
                raise NotImplementedError(
                    f"No handler implemented for constraint: {type(constraint)}"
                )

        return exp.ColumnDef(
            this=exp.Identifier(this=field.name),
            kind=self._convert_type(field.type),
            constraints=constraints if constraints else None,
        )

    def _handle_not_null_constraint(
        self, constraint: NotNullConstraint
    ) -> exp.ColumnConstraint:
        return exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())

    def _handle_primary_key_constraint(
        self, constraint: PrimaryKeyConstraint
    ) -> exp.ColumnConstraint:
        return exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint())

    def _handle_default_constraint(
        self, constraint: DefaultConstraint
    ) -> exp.ColumnConstraint:
        return exp.ColumnConstraint(
            kind=exp.DefaultColumnConstraint(this=convert(constraint.value))
        )

    def _handle_identity_constraint(
        self, constraint: IdentityConstraint
    ) -> exp.ColumnConstraint:
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

    def _handle_foreign_key_constraint(
        self, constraint: ForeignKeyConstraint
    ) -> exp.ColumnConstraint:
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

    def _handle_primary_key_table_constraint(
        self, constraint: PrimaryKeyTableConstraint
    ) -> exp.Expression:
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

    def _handle_foreign_key_table_constraint(
        self, constraint: ForeignKeyTableConstraint
    ) -> exp.Expression:
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

    def _collect_expressions(self, spec: SchemaSpec) -> list[exp.Expression]:
        """Gathers all table-level expressions from the spec."""
        expressions: list[exp.Expression] = [
            self._convert_field(col) for col in spec.columns
        ]
        for constraint in spec.table_constraints:
            if handler := self._table_constraint_handlers.get(type(constraint)):
                expressions.append(handler(constraint))
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
