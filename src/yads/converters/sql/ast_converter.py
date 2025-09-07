"""AST converter from yads `YadsSpec` to sqlglot AST expressions.

This module provides the abstract base for AST converters and the
SQLGlotConverter implementation. AST converters are responsible for
producing dialect-agnostic AST representations from YadsSpec objects.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import singledispatchmethod
from typing import Any, Literal, Generator, Callable
from dataclasses import dataclass, field

from sqlglot import exp
from sqlglot.expressions import convert
from sqlglot.errors import ParseError

from ...constraints import (
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
)
from ...exceptions import (
    ConversionError,
    UnsupportedFeatureError,
    validation_warning,
)
from ...spec import Column, Field, YadsSpec, Storage, TransformedColumnReference
from ...types import (
    YadsType,
    String,
    Integer,
    Float,
    Decimal,
    Binary,
    Date,
    Time,
    Timestamp,
    TimestampTZ,
    TimestampLTZ,
    TimestampNTZ,
    Interval,
    Array,
    Struct,
    Map,
    Geometry,
    Geography,
    Void,
)
from ..base import BaseConverter, BaseConverterConfig


class AstConverter(ABC):
    """Abstract base class for AST converters.

    AST converters transform YadsSpec objects into dialect-agnostic AST
    representations that can be serialized to SQL DDL for specific databases.
    """

    @abstractmethod
    def convert(self, spec: YadsSpec) -> Any: ...

    @abstractmethod
    @contextmanager
    def conversion_context(
        self,
        *,
        mode: Literal["raise", "coerce"] | None = None,
        field: str | None = None,
    ) -> Generator[None, None, None]: ...


@dataclass(frozen=True)
class SQLGlotConverterConfig(BaseConverterConfig[exp.ColumnDef]):
    """Configuration for SQLGlotConverter.

    Args:
        if_not_exists: If True, sets the `exists` property of the `exp.Create`
            node to `True`. Defaults to False.
        or_replace: If True, sets the `replace` property of the `exp.Create`
            node to `True`. Defaults to False.
        ignore_catalog: If True, omits the catalog from the table name. Defaults to False.
        ignore_database: If True, omits the database from the table name. Defaults to False.
        fallback_type: SQL data type to use for unsupported types in coerce mode.
            Must be one of: exp.DataType.Type.TEXT, exp.DataType.Type.BINARY, exp.DataType.Type.BLOB.
            Defaults to exp.DataType.Type.TEXT.
    """

    if_not_exists: bool = False
    or_replace: bool = False
    ignore_catalog: bool = False
    ignore_database: bool = False
    fallback_type: exp.DataType.Type = exp.DataType.Type.TEXT
    column_overrides: dict[str, Callable[[Field, SQLGlotConverter], exp.ColumnDef]] = (
        field(default_factory=dict)
    )  # type: ignore[assignment]

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        super().__post_init__()

        # Validate fallback_type
        valid_fallback_types = {
            exp.DataType.Type.TEXT,
            exp.DataType.Type.BINARY,
            exp.DataType.Type.BLOB,
        }
        if self.fallback_type not in valid_fallback_types:
            raise UnsupportedFeatureError(
                f"fallback_type must be one of: exp.DataType.Type.TEXT, "
                f"exp.DataType.Type.BINARY, exp.DataType.Type.BLOB. Got: {self.fallback_type}"
            )


class SQLGlotConverter(BaseConverter, AstConverter):
    """Core converter that transforms yads specs into sqlglot AST expressions.

    SQLGlotConverter is the foundational converter that handles the transformation
    from yads' high-level canonical spec to sqlglot's Abstract Syntax Tree
    representation. This AST serves as a dialect-agnostic intermediate representation
    that can then be serialized into SQL for specific database systems.

    The converter uses single dispatch methods to handle different yads types,
    constraints, and spec elements, providing extensible type mapping and
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

    def __init__(self, config: SQLGlotConverterConfig | None = None) -> None:
        """Initialize the SQLGlotConverter.

        Args:
            config: Configuration object. If None, uses default SQLGlotConverterConfig.
        """
        self.config: SQLGlotConverterConfig = config or SQLGlotConverterConfig()
        super().__init__(self.config)

    _TRANSFORM_HANDLERS: dict[str, str] = {
        "bucket": "_handle_bucket_transform",
        "truncate": "_handle_truncate_transform",
        "cast": "_handle_cast_transform",
        "date_trunc": "_handle_date_trunc_transform",
        "trunc": "_handle_date_trunc_transform",
    }

    def convert(
        self,
        spec: YadsSpec,
        *,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> exp.Create:
        """Convert a yads `YadsSpec` into a sqlglot `exp.Create` AST expression.

        The resulting AST is dialect-agnostic and can be serialized to SQL for
        any database system supported by sqlglot. The conversion preserves all
        spec information and applies appropriate sqlglot expression types.

        Args:
            spec: The yads spec as a `YadsSpec` object.
            mode: Optional conversion mode override for this call. When not
                provided, the converter's configured mode is used. If provided:
                - "raise": Raise on any unsupported features.
                - "coerce": Apply adjustments to produce a valid AST and emit warnings.

        Returns:
            sqlglot `exp.Create` expression representing a CREATE TABLE statement.
            The AST includes table schema, constraints, properties, and metadata
            from the yads spec.

        Example:
            >>> config = SQLGlotConverterConfig(if_not_exists=True)
            >>> converter = SQLGlotConverter(config)
            >>> ast = converter.convert(spec)
            >>> print(type(ast))
            <class 'sqlglot.expressions.Create'>
            >>> print(ast.sql(dialect="spark"))
            CREATE TABLE IF NOT EXISTS ...
        """
        # Set mode for this conversion call
        with self.conversion_context(mode=mode):
            self._validate_column_filters(spec)
            table = self._parse_full_table_name(
                spec.name,
                ignore_catalog=self.config.ignore_catalog,
                ignore_database=self.config.ignore_database,
            )
            properties = self._collect_properties(spec)
            expressions = self._collect_expressions(spec)

        return exp.Create(
            this=exp.Schema(this=table, expressions=expressions),
            kind="TABLE",
            exists=self.config.if_not_exists or None,
            replace=self.config.or_replace or None,
            properties=(exp.Properties(expressions=properties) if properties else None),
        )

    @singledispatchmethod
    def _convert_type(self, yads_type: YadsType) -> exp.DataType:
        # Fallback to default sqlglot DataType.build method.
        # The following non-parametrized yads types are handled via the fallback:
        # - Boolean
        # - JSON
        # - UUID
        # - Variant
        # https://sqlglot.com/sqlglot/expressions.html#DataType.build
        try:
            return exp.DataType.build(str(yads_type))
        except ParseError:
            # Currently unsupported in sqlglot:
            # - Duration
            if self.config.mode == "coerce":
                validation_warning(
                    message=(
                        f"SQLGlotConverter does not support type: {yads_type}"
                        f" for column '{self._current_field_name or '<unknown>'}'."
                        f" The data type will be replaced with {self.config.fallback_type.name}."
                    ),
                    filename="yads.converters.sql.ast_converter",
                    module=__name__,
                )
                return exp.DataType(this=self.config.fallback_type)
            raise UnsupportedFeatureError(
                f"SQLGlotConverter does not support type: {yads_type}"
                f" for column '{self._current_field_name or '<unknown>'}'."
            )

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
        bits = yads_type.bits or 32
        if yads_type.signed:
            if bits == 8:
                return exp.DataType(this=exp.DataType.Type.TINYINT)
            elif bits == 16:
                return exp.DataType(this=exp.DataType.Type.SMALLINT)
            elif bits == 32:
                return exp.DataType(this=exp.DataType.Type.INT)
            elif bits == 64:
                return exp.DataType(this=exp.DataType.Type.BIGINT)
        else:
            if bits == 8:
                return exp.DataType(this=exp.DataType.Type.UTINYINT)
            elif bits == 16:
                return exp.DataType(this=exp.DataType.Type.USMALLINT)
            elif bits == 32:
                return exp.DataType(this=exp.DataType.Type.UINT)
            elif bits == 64:
                return exp.DataType(this=exp.DataType.Type.UBIGINT)
        raise UnsupportedFeatureError(
            f"Unsupported Integer bits: {bits}. Expected 8/16/32/64."
        )

    @_convert_type.register(Float)
    def _(self, yads_type: Float) -> exp.DataType:
        bits = yads_type.bits or 32
        if bits == 16:
            if self.config.mode == "coerce":
                validation_warning(
                    message=(
                        f"SQLGlotConverter does not support half-precision Float (bits={bits})."
                        f" The data type will be replaced with Float (bits=32)."
                    ),
                    filename="yads.converters.sql.ast_converter",
                    module=__name__,
                )
                return exp.DataType(this=exp.DataType.Type.FLOAT)
            raise UnsupportedFeatureError(
                f"SQLGlotConverter does not support half-precision Float (bits={bits})."
            )
        elif bits == 32:
            return exp.DataType(this=exp.DataType.Type.FLOAT)
        elif bits == 64:
            return exp.DataType(this=exp.DataType.Type.DOUBLE)
        raise UnsupportedFeatureError(
            f"Unsupported Float bits: {bits}. Expected 16/32/64."
        )

    @_convert_type.register(Decimal)
    def _(self, yads_type: Decimal) -> exp.DataType:
        expressions = []
        if yads_type.precision is not None:
            expressions.append(exp.DataTypeParam(this=convert(yads_type.precision)))
            expressions.append(exp.DataTypeParam(this=convert(yads_type.scale)))
        # Ignore bit-width parameter
        return exp.DataType(
            this=exp.DataType.Type.DECIMAL,
            expressions=expressions if expressions else None,
        )

    # Explicit mappings for parametrized temporal types
    @_convert_type.register(Timestamp)
    def _(self, yads_type: Timestamp) -> exp.DataType:
        # Ignore unit parameter
        return exp.DataType(this=exp.DataType.Type.TIMESTAMP)

    @_convert_type.register(TimestampTZ)
    def _(self, yads_type: TimestampTZ) -> exp.DataType:
        # Ignore unit parameter
        # Ignore tz parameter
        return exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ)

    @_convert_type.register(TimestampLTZ)
    def _(self, yads_type: TimestampLTZ) -> exp.DataType:
        # Ignore unit parameter
        return exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ)

    @_convert_type.register(TimestampNTZ)
    def _(self, yads_type: TimestampNTZ) -> exp.DataType:
        # Ignore unit parameter
        return exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ)

    @_convert_type.register(Time)
    def _(self, yads_type: Time) -> exp.DataType:
        # Ignore bit-width parameter
        # Ignore unit parameter
        return exp.DataType(this=exp.DataType.Type.TIME)

    @_convert_type.register(Date)
    def _(self, yads_type: Date) -> exp.DataType:
        # Ignore bit-width parameter
        return exp.DataType(this=exp.DataType.Type.DATE)

    @_convert_type.register(Binary)
    def _(self, yads_type: Binary) -> exp.DataType:
        expressions = []
        if yads_type.length is not None:
            expressions.append(exp.DataTypeParam(this=convert(yads_type.length)))
        return exp.DataType(
            this=exp.DataType.Type.BINARY, expressions=expressions or None
        )

    @_convert_type.register(Void)
    def _(self, yads_type: Void) -> exp.DataType:
        # VOID is not a valid sqlglot type, but can be defined as a Spark type.
        # https://docs.databricks.com/aws/en/sql/language-manual/data-types/null-type
        return exp.DataType(
            this=exp.DataType.Type.USERDEFINED,
            kind="VOID",
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
        # Ignore size parameter
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
        # Ignore keys_sorted parameter
        return exp.DataType(
            this=exp.DataType.Type.MAP,
            expressions=[key_type, value_type],
            nested=exp.DataType.Type.MAP in exp.DataType.NESTED_TYPES,
        )

    @_convert_type.register(Geometry)
    def _(self, yads_type: Geometry) -> exp.DataType:
        expressions = (
            [exp.DataTypeParam(this=convert(yads_type.srid))]
            if yads_type.srid is not None
            else None
        )
        return exp.DataType(this=exp.DataType.Type.GEOMETRY, expressions=expressions)

    @_convert_type.register(Geography)
    def _(self, yads_type: Geography) -> exp.DataType:
        expressions = (
            [exp.DataTypeParam(this=convert(yads_type.srid))]
            if yads_type.srid is not None
            else None
        )
        return exp.DataType(this=exp.DataType.Type.GEOGRAPHY, expressions=expressions)

    @singledispatchmethod
    def _convert_column_constraint(self, constraint: Any) -> exp.ColumnConstraint | None:
        if self.config.mode == "coerce":
            validation_warning(
                message=(
                    f"SQLGlotConverter does not support constraint: {type(constraint)}"
                    f" for column '{self._current_field_name or '<unknown>'}'."
                    f" The constraint will be omitted."
                ),
                filename="yads.converters.sql.ast_converter",
                module=__name__,
            )
            return None
        raise UnsupportedFeatureError(
            f"SQLGlotConverter does not support constraint: {type(constraint)}"
            f" for column '{self._current_field_name or '<unknown>'}'."
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
    def _convert_table_constraint(self, constraint: Any) -> exp.Expression | None:
        if self.config.mode == "coerce":
            validation_warning(
                message=(
                    f"SQLGlotConverter does not support table constraint: {type(constraint)}"
                    f" The constraint will be omitted."
                ),
                filename="yads.converters.sqlglot",
                module=__name__,
            )
            return None
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
            with self.conversion_context(field=col.column):
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

    def _collect_properties(self, spec: YadsSpec) -> list[exp.Property]:
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
            if self.config.mode == "coerce":
                validation_warning(
                    message=(
                        f"Transform type '{cast_to_type}' is not a valid sqlglot Type"
                        f" for column '{self._current_field_name or '<unknown>'}'."
                        f" The expression will be coerced to CAST(... AS {self.config.fallback_type.name})."
                    ),
                    filename="yads.converters.sqlglot",
                    module=__name__,
                )
                return exp.Cast(
                    this=exp.column(column),
                    to=exp.DataType(this=self.config.fallback_type),
                )
            raise UnsupportedFeatureError(
                f"Transform type '{cast_to_type}' is not a valid sqlglot Type"
                f" for column '{self._current_field_name or '<unknown>'}'."
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
        with self.conversion_context(field=column.name):
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
                converted = self._convert_column_constraint(constraint)
                if converted is not None:
                    constraints.append(converted)
            return exp.ColumnDef(
                this=exp.Identifier(this=column.name),
                kind=self._convert_type(column.type),
                constraints=constraints if constraints else None,
            )

    def _convert_field_default(self, field: Field) -> exp.ColumnDef:
        if not isinstance(field, Column):  # Overrides happen on column level
            raise TypeError(f"Expected Column, got {type(field)}")
        return self._convert_column(field)

    def _collect_expressions(self, spec: YadsSpec) -> list[exp.Expression]:
        expressions: list[exp.Expression] = []
        for col in self._filter_columns(spec):
            # Set field context during conversion
            with self.conversion_context(field=col.name):
                # Use centralized override resolution
                column_expr = self._convert_field_with_overrides(col)
                expressions.append(column_expr)

        for tbl_constraint in spec.table_constraints:
            converted_constraint = self._convert_table_constraint(tbl_constraint)
            if converted_constraint is not None:
                expressions.append(converted_constraint)
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
