"""AST converter from yads `SchemaSpec` to sqlglot AST expressions.

Contains the `SQLGlotConverter`, which is responsible for producing a
dialect-agnostic `sqlglot` AST representing a CREATE TABLE statement
from the canonical `SchemaSpec` format.
"""

from __future__ import annotations

from functools import singledispatchmethod
from typing import Any

from sqlglot import exp
from sqlglot.expressions import convert

from ...constraints import (
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
)
from ...exceptions import ConversionError, UnsupportedFeatureError
from ...spec import Column, Field, SchemaSpec, Storage, TransformedColumnReference
from ...types import Array, Decimal, Float, Integer, Interval, Map, String, Struct, Type
from ..base import BaseConverter


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

    _TRANSFORM_HANDLERS: dict[str, str] = {
        "bucket": "_handle_bucket_transform",
        "truncate": "_handle_truncate_transform",
        "cast": "_handle_cast_transform",
        "date_trunc": "_handle_date_trunc_transform",
        "trunc": "_handle_date_trunc_transform",
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
                ignore_catalog: If True, omits the catalog from the table name.
                ignore_database: If True, omits the database from the table name.

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
        table = self._parse_full_table_name(
            spec.name,
            ignore_catalog=kwargs.get("ignore_catalog", False),
            ignore_database=kwargs.get("ignore_database", False),
        )
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
        # Fallback to default sqlglot DataType.build method.
        # The following non-parametrized yads types are handled via the fallback:
        # - Boolean
        # - Date
        # - Timestamp
        # - TimestampTZ
        # - TimestampLTZ
        # - TimestampNTZ
        # - Binary
        # - JSON
        # - UUID
        # https://sqlglot.com/sqlglot/expressions.html#DataType.build
        return exp.DataType.build(str(yads_type))

    @_convert_type.register(String)
    def _(self, yads_type: String) -> exp.DataType:
        expressions = []
        if yads_type.length:
            expressions.append(exp.DataTypeParam(this=convert(yads_type.length)))
        return exp.DataType(
            this=exp.DataType.Type.TEXT,
            expressions=expressions if expressions else None,
        )

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
        if yads_type.interval_end and yads_type.interval_start != yads_type.interval_end:
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
            start_expr = (
                exp.Neg(this=convert(abs(constraint.start)))
                if constraint.start < 0
                else convert(constraint.start)
            )

        increment_expr: exp.Expression | None = None
        if constraint.increment is not None:
            increment_expr = (
                exp.Neg(this=convert(abs(constraint.increment)))
                if constraint.increment < 0
                else convert(constraint.increment)
            )

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
                this=exp.Identifier(this=constraint.name), kind=reference_expression
            )
        return exp.ColumnConstraint(kind=reference_expression)

    @singledispatchmethod
    def _convert_table_constraint(self, constraint: Any) -> exp.Expression:
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
                    this=exp.Column(this=exp.Identifier(this=c)), nulls_first=True
                )
                for c in constraint.columns
            ]
        )
        if constraint.name:
            return exp.Constraint(
                this=exp.Identifier(this=constraint.name), expressions=[pk_expression]
            )
        raise ConversionError("Primary key constraint must have a name.")

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
                this=exp.Identifier(this=constraint.name), expressions=[fk_expression]
            )
        raise ConversionError("Foreign key constraint must have a name.")

    # Properties
    def _handle_storage_properties(self, storage: Storage | None) -> list[exp.Property]:
        if not storage:
            return []
        properties: list[exp.Property] = []
        if storage.format:
            properties.append(self._handle_file_format_property(storage.format))
        if storage.location:
            properties.append(self._handle_location_property(storage.location))
        if storage.tbl_properties:
            for key, value in storage.tbl_properties.items():
                properties.append(self._handle_generic_property(key, value))
        return properties

    def _handle_partitioned_by_property(
        self, value: list[TransformedColumnReference]
    ) -> exp.PartitionedByProperty:
        schema_expressions = []
        for col in value:
            if col.transform:
                expression = self._handle_transformation(
                    col.column, col.transform, col.transform_args
                )
            else:
                expression = exp.Identifier(this=col.column)
            schema_expressions.append(expression)
        return exp.PartitionedByProperty(this=exp.Schema(expressions=schema_expressions))

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
        # Most direct or parametrized transformation functions are supported
        # via the fallback. I.e.
        # - `day(original_col)`
        # - `month(original_col)`
        # - `year(original_col)`
        # - `date_format(original_col, 'yyyy-MM-dd')`
        # https://sqlglot.com/sqlglot/expressions.html#func
        return exp.func(
            transform, exp.column(column), *(exp.convert(arg) for arg in transform_args)
        )

    def _handle_cast_transform(self, column: str, transform_args: list) -> exp.Expression:
        self._validate_transform_args("cast", len(transform_args), 1)
        cast_to_type = transform_args[0].upper()
        if cast_to_type not in exp.DataType.Type:
            raise UnsupportedFeatureError(
                f"Transform type '{cast_to_type}' is not a valid sqlglot Type"
            )
        return exp.Cast(
            this=exp.column(column),
            to=exp.DataType(this=exp.DataType.Type[cast_to_type]),
        )

    def _handle_bucket_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        self._validate_transform_args("bucket", len(transform_args), 1)
        return exp.PartitionedByBucket(
            this=exp.column(column), expression=exp.convert(transform_args[0])
        )

    def _handle_truncate_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        self._validate_transform_args("truncate", len(transform_args), 1)
        return exp.PartitionByTruncate(
            this=exp.column(column), expression=exp.convert(transform_args[0])
        )

    def _handle_date_trunc_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        self._validate_transform_args("date_trunc", len(transform_args), 1)
        return exp.DateTrunc(unit=exp.convert(transform_args[0]), this=exp.column(column))

    def _validate_transform_args(
        self, transform: str, received_args_len: int, required_args_len: int
    ) -> None:
        if received_args_len != required_args_len:
            raise ConversionError(
                f"The '{transform}' transform requires exactly {required_args_len} argument(s)."
                f" Got {received_args_len}."
            )

    # Field/column helpers
    def _convert_field(self, field: Field) -> exp.ColumnDef:
        return exp.ColumnDef(
            this=exp.Identifier(this=field.name),
            kind=self._convert_type(field.type),
            constraints=None,
        )

    def _convert_column(self, column: Column) -> exp.ColumnDef:
        constraints = []
        if column.generated_as and column.generated_as.transform:
            expression = self._handle_transformation(
                column.generated_as.column,
                column.generated_as.transform,
                column.generated_as.transform_args,
            )
            constraints.append(
                exp.ColumnConstraint(
                    kind=exp.GeneratedAsIdentityColumnConstraint(
                        this=True, expression=expression
                    )
                )
            )
        for constraint in column.constraints:
            constraints.append(self._convert_column_constraint(constraint))
        return exp.ColumnDef(
            this=exp.Identifier(this=column.name),
            kind=self._convert_type(column.type),
            constraints=constraints if constraints else None,
        )

    def _collect_expressions(self, spec: SchemaSpec) -> list[exp.Expression]:
        expressions: list[exp.Expression] = [
            self._convert_column(col) for col in spec.columns
        ]
        for constraint in spec.table_constraints:
            expressions.append(self._convert_table_constraint(constraint))
        return expressions

    def _parse_full_table_name(
        self,
        full_name: str,
        columns: list[str] | None = None,
        ignore_catalog: bool = False,
        ignore_database: bool = False,
    ) -> exp.Table | exp.Schema:
        parts = full_name.split(".")
        table_name = parts[-1]
        db_name = None
        catalog_name = None
        if not ignore_database and len(parts) > 1:
            db_name = parts[-2]
        if not ignore_catalog and len(parts) > 2:
            catalog_name = parts[-3]

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
