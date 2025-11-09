"""AST converter from yads `YadsSpec` to sqlglot AST expressions.

This module provides the abstract base for AST converters and the
SQLGlotConverter implementation. AST converters are responsible for
producing dialect-agnostic AST representations from YadsSpec objects.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import singledispatchmethod
from typing import Any, Literal, Generator, Callable, Mapping, TYPE_CHECKING
from dataclasses import dataclass, field
from types import MappingProxyType

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
from ..._dependencies import requires_dependency
from ... import spec as yspec
from ... import types as ytypes
from ..base import BaseConverter, BaseConverterConfig

if TYPE_CHECKING:
    from sqlglot import exp


class AstConverter(ABC):
    """Abstract base class for AST converters.

    AST converters transform YadsSpec objects into dialect-agnostic AST
    representations that can be serialized to SQL DDL for specific databases.
    """

    @abstractmethod
    def convert(self, spec: yspec.YadsSpec) -> Any: ...

    @abstractmethod
    @contextmanager
    def conversion_context(
        self,
        *,
        mode: Literal["raise", "coerce"] | None = None,
        field: str | None = None,
    ) -> Generator[None, None, None]: ...


@dataclass(frozen=True)
# %% ---- Configuration --------------------------------------------------------------
class SQLGlotConverterConfig(BaseConverterConfig):
    """Configuration for SQLGlotConverter.

    Args:
        mode: Conversion mode. One of "raise" or "coerce". Inherited from
            BaseConverterConfig. Defaults to "coerce".
        ignore_columns: Column names to exclude from conversion. Inherited from
            BaseConverterConfig. Defaults to empty.
        include_columns: If provided, only these columns are included. Inherited
            from BaseConverterConfig. Defaults to None.
        column_overrides: Mapping of column name to a callable that returns a
            custom sqlglot `exp.ColumnDef`. Inherited from BaseConverterConfig.
            Defaults to empty mapping.
        if_not_exists: If True, sets the `exists` property of the `exp.Create`
            node to `True`. Defaults to False.
        or_replace: If True, sets the `replace` property of the `exp.Create`
            node to `True`. Defaults to False.
        ignore_catalog: If True, omits the catalog from the table name. Defaults to False.
        ignore_database: If True, omits the database from the table name. Defaults to False.
        fallback_type: SQL data type to use for unsupported types in coerce mode.
            Must be one of: exp.DataType.Type.TEXT, exp.DataType.Type.BINARY, exp.DataType.Type.BLOB, or None.
            Defaults to None.
    """

    if_not_exists: bool = False
    or_replace: bool = False
    ignore_catalog: bool = False
    ignore_database: bool = False
    fallback_type: exp.DataType.Type | None = None
    column_overrides: Mapping[
        str, Callable[[yspec.Field, SQLGlotConverter], exp.ColumnDef]
    ] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        super().__post_init__()

        # Validate fallback_type if provided
        if self.fallback_type is not None:
            from sqlglot import exp

            valid_fallback_types = {
                exp.DataType.Type.TEXT,
                exp.DataType.Type.BINARY,
                exp.DataType.Type.BLOB,
            }
            if self.fallback_type not in valid_fallback_types:
                raise UnsupportedFeatureError(
                    f"fallback_type must be one of: exp.DataType.Type.TEXT, "
                    f"exp.DataType.Type.BINARY, exp.DataType.Type.BLOB, or None. Got: {self.fallback_type}"
                )


# %% ---- Converter ------------------------------------------------------------------
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

    def _format_type_for_display(self, type_obj: Any) -> str:
        """Format sqlglot DataType for display in warnings.

        Extracts the type name from sqlglot's exp.DataType for cleaner
        display in warning messages.
        """
        from sqlglot import exp

        if isinstance(type_obj, exp.DataType) and hasattr(type_obj.this, "name"):
            return type_obj.this.name
        return str(type_obj)

    _TRANSFORM_HANDLERS: dict[str, str] = {
        "bucket": "_handle_bucket_transform",
        "truncate": "_handle_truncate_transform",
        "cast": "_handle_cast_transform",
        "date_trunc": "_handle_date_trunc_transform",
        "trunc": "_handle_date_trunc_transform",
    }

    @requires_dependency("sqlglot", import_name="sqlglot")
    def convert(
        self,
        spec: yspec.YadsSpec,
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
        from sqlglot import exp

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

    # %% ---- Type conversion ---------------------------------------------------------
    @singledispatchmethod
    def _convert_type(self, yads_type: ytypes.YadsType) -> exp.DataType:
        from sqlglot import exp
        from sqlglot.errors import ParseError

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
            # - Tensor
            return self.raise_or_coerce(
                yads_type,
                coerce_type=exp.DataType(this=self.config.fallback_type),
            )

    @_convert_type.register(ytypes.String)
    def _(self, yads_type: ytypes.String) -> exp.DataType:
        from sqlglot import exp
        from sqlglot.expressions import convert

        expressions = []
        if yads_type.length:
            expressions.append(exp.DataTypeParam(this=convert(yads_type.length)))
        return exp.DataType(
            this=exp.DataType.Type.TEXT,
            expressions=expressions if expressions else None,
        )

    @_convert_type.register(ytypes.Integer)
    def _(self, yads_type: ytypes.Integer) -> exp.DataType:
        from sqlglot import exp

        bits = yads_type.bits or 32
        signed_map = {
            8: exp.DataType.Type.TINYINT,
            16: exp.DataType.Type.SMALLINT,
            32: exp.DataType.Type.INT,
            64: exp.DataType.Type.BIGINT,
        }
        unsigned_map = {
            8: exp.DataType.Type.UTINYINT,
            16: exp.DataType.Type.USMALLINT,
            32: exp.DataType.Type.UINT,
            64: exp.DataType.Type.UBIGINT,
        }
        mapping = signed_map if yads_type.signed else unsigned_map
        try:
            return exp.DataType(this=mapping[bits])
        except KeyError as e:
            raise UnsupportedFeatureError(
                f"Unsupported Integer bits: {bits}. Expected 8/16/32/64"
                f" for '{self._field_context}'."
            ) from e

    @_convert_type.register(ytypes.Float)
    def _(self, yads_type: ytypes.Float) -> exp.DataType:
        from sqlglot import exp

        bits = yads_type.bits or 32
        if bits == 16:
            return self.raise_or_coerce(
                coerce_type=exp.DataType(this=exp.DataType.Type.FLOAT),
                error_msg=(
                    f"SQLGlotConverter does not support half-precision Float (bits={bits})."
                ),
            )
        elif bits == 32:
            return exp.DataType(this=exp.DataType.Type.FLOAT)
        elif bits == 64:
            return exp.DataType(this=exp.DataType.Type.DOUBLE)
        raise UnsupportedFeatureError(
            f"Unsupported Float bits: {bits}. Expected 16/32/64"
            f" for '{self._field_context}'."
        )

    @_convert_type.register(ytypes.Decimal)
    def _(self, yads_type: ytypes.Decimal) -> exp.DataType:
        from sqlglot import exp
        from sqlglot.expressions import convert

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
    @_convert_type.register(ytypes.Timestamp)
    def _(self, yads_type: ytypes.Timestamp) -> exp.DataType:
        from sqlglot import exp

        # Ignore unit parameter
        return exp.DataType(this=exp.DataType.Type.TIMESTAMP)

    @_convert_type.register(ytypes.TimestampTZ)
    def _(self, yads_type: ytypes.TimestampTZ) -> exp.DataType:
        from sqlglot import exp

        # Ignore unit parameter
        # Ignore tz parameter
        return exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ)

    @_convert_type.register(ytypes.TimestampLTZ)
    def _(self, yads_type: ytypes.TimestampLTZ) -> exp.DataType:
        from sqlglot import exp

        # Ignore unit parameter
        return exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ)

    @_convert_type.register(ytypes.TimestampNTZ)
    def _(self, yads_type: ytypes.TimestampNTZ) -> exp.DataType:
        from sqlglot import exp

        # Ignore unit parameter
        return exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ)

    @_convert_type.register(ytypes.Time)
    def _(self, yads_type: ytypes.Time) -> exp.DataType:
        from sqlglot import exp

        # Ignore bit-width parameter
        # Ignore unit parameter
        return exp.DataType(this=exp.DataType.Type.TIME)

    @_convert_type.register(ytypes.Date)
    def _(self, yads_type: ytypes.Date) -> exp.DataType:
        from sqlglot import exp

        # Ignore bit-width parameter
        return exp.DataType(this=exp.DataType.Type.DATE)

    @_convert_type.register(ytypes.Binary)
    def _(self, yads_type: ytypes.Binary) -> exp.DataType:
        from sqlglot import exp
        from sqlglot.expressions import convert

        expressions = []
        if yads_type.length is not None:
            expressions.append(exp.DataTypeParam(this=convert(yads_type.length)))
        return exp.DataType(
            this=exp.DataType.Type.BINARY, expressions=expressions or None
        )

    @_convert_type.register(ytypes.Void)
    def _(self, yads_type: ytypes.Void) -> exp.DataType:
        from sqlglot import exp

        # VOID is not a valid sqlglot type, but can be defined as a Spark type.
        # https://docs.databricks.com/aws/en/sql/language-manual/data-types/null-type
        return exp.DataType(
            this=exp.DataType.Type.USERDEFINED,
            kind="VOID",
        )

    @_convert_type.register(ytypes.Interval)
    def _(self, yads_type: ytypes.Interval) -> exp.DataType:
        from sqlglot import exp

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

    @_convert_type.register(ytypes.Array)
    def _(self, yads_type: ytypes.Array) -> exp.DataType:
        from sqlglot import exp

        element_type = self._convert_type(yads_type.element)
        # Ignore size parameter
        return exp.DataType(
            this=exp.DataType.Type.ARRAY,
            expressions=[element_type],
            nested=exp.DataType.Type.ARRAY in exp.DataType.NESTED_TYPES,
        )

    @_convert_type.register(ytypes.Struct)
    def _(self, yads_type: ytypes.Struct) -> exp.DataType:
        from sqlglot import exp

        return exp.DataType(
            this=exp.DataType.Type.STRUCT,
            expressions=[self._convert_field(field) for field in yads_type.fields],
            nested=exp.DataType.Type.STRUCT in exp.DataType.NESTED_TYPES,
        )

    @_convert_type.register(ytypes.Map)
    def _(self, yads_type: ytypes.Map) -> exp.DataType:
        from sqlglot import exp

        key_type = self._convert_type(yads_type.key)
        value_type = self._convert_type(yads_type.value)
        # Ignore keys_sorted parameter
        return exp.DataType(
            this=exp.DataType.Type.MAP,
            expressions=[key_type, value_type],
            nested=exp.DataType.Type.MAP in exp.DataType.NESTED_TYPES,
        )

    @_convert_type.register(ytypes.Geometry)
    def _(self, yads_type: ytypes.Geometry) -> exp.DataType:
        from sqlglot import exp
        from sqlglot.expressions import convert

        expressions = (
            [exp.DataTypeParam(this=convert(yads_type.srid))]
            if yads_type.srid is not None
            else None
        )
        return exp.DataType(this=exp.DataType.Type.GEOMETRY, expressions=expressions)

    @_convert_type.register(ytypes.Geography)
    def _(self, yads_type: ytypes.Geography) -> exp.DataType:
        from sqlglot import exp
        from sqlglot.expressions import convert

        expressions = (
            [exp.DataTypeParam(this=convert(yads_type.srid))]
            if yads_type.srid is not None
            else None
        )
        return exp.DataType(this=exp.DataType.Type.GEOGRAPHY, expressions=expressions)

    # %% ---- Column constraints ------------------------------------------------------
    @singledispatchmethod
    def _convert_column_constraint(self, constraint: Any) -> exp.ColumnConstraint | None:
        error_msg = (
            f"SQLGlotConverter does not support constraint: {type(constraint)}"
            f" for '{self._field_context}'."
        )

        if self.config.mode == "coerce":
            validation_warning(
                message=f"{error_msg} The constraint will be omitted.",
                filename="yads.converters.sql.ast_converter",
                module=__name__,
            )
            return None
        else:
            raise UnsupportedFeatureError(error_msg)

    @_convert_column_constraint.register(NotNullConstraint)
    def _(self, constraint: NotNullConstraint) -> exp.ColumnConstraint:
        from sqlglot import exp

        return exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())

    @_convert_column_constraint.register(PrimaryKeyConstraint)
    def _(self, constraint: PrimaryKeyConstraint) -> exp.ColumnConstraint:
        from sqlglot import exp

        return exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint())

    @_convert_column_constraint.register(DefaultConstraint)
    def _(self, constraint: DefaultConstraint) -> exp.ColumnConstraint:
        from sqlglot import exp
        from sqlglot.expressions import convert

        return exp.ColumnConstraint(
            kind=exp.DefaultColumnConstraint(this=convert(constraint.value))
        )

    @_convert_column_constraint.register(IdentityConstraint)
    def _(self, constraint: IdentityConstraint) -> exp.ColumnConstraint:
        from sqlglot import exp
        from sqlglot.expressions import convert

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
        from sqlglot import exp

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

    # %% ---- Table constraints -------------------------------------------------------
    @singledispatchmethod
    def _convert_table_constraint(self, constraint: Any) -> exp.Expression | None:
        error_msg = (
            f"SQLGlotConverter does not support table constraint: {type(constraint)}"
        )

        if self.config.mode == "coerce":
            validation_warning(
                message=f"{error_msg} The constraint will be omitted.",
                filename="yads.converters.sql.ast_converter",
                module=__name__,
            )
            return None
        else:
            raise UnsupportedFeatureError(error_msg)

    @_convert_table_constraint.register(PrimaryKeyTableConstraint)
    def _(self, constraint: PrimaryKeyTableConstraint) -> exp.Expression:
        from sqlglot import exp

        pk_expression = exp.PrimaryKey(
            expressions=[
                exp.Ordered(
                    this=exp.Column(this=exp.Identifier(this=c)), nulls_first=True
                )
                for c in constraint.columns
            ],
            include=exp.IndexParameters(),
        )
        if constraint.name:
            return exp.Constraint(
                this=exp.Identifier(this=constraint.name), expressions=[pk_expression]
            )
        raise ConversionError("Primary key constraint must have a name.")

    @_convert_table_constraint.register(ForeignKeyTableConstraint)
    def _(self, constraint: ForeignKeyTableConstraint) -> exp.Expression:
        from sqlglot import exp

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

    # %% ---- Properties --------------------------------------------------------------
    def _handle_storage_properties(
        self, storage: yspec.Storage | None
    ) -> list[exp.Property]:
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
        self, value: list[yspec.TransformedColumnReference]
    ) -> exp.PartitionedByProperty:
        from sqlglot import exp

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
        from sqlglot import exp
        from sqlglot.expressions import convert

        return exp.LocationProperty(this=convert(value))

    def _handle_file_format_property(self, value: str) -> exp.FileFormatProperty:
        from sqlglot import exp

        return exp.FileFormatProperty(this=exp.Var(this=value))

    def _handle_external_property(self) -> exp.ExternalProperty:
        from sqlglot import exp

        return exp.ExternalProperty()

    def _handle_generic_property(self, key: str, value: Any) -> exp.Property:
        from sqlglot import exp
        from sqlglot.expressions import convert

        return exp.Property(this=convert(key), value=convert(value))

    def _collect_properties(self, spec: yspec.YadsSpec) -> list[exp.Property]:
        properties: list[exp.Property] = []
        if spec.external:
            properties.append(self._handle_external_property())
        properties.extend(self._handle_storage_properties(spec.storage))
        if spec.partitioned_by:
            properties.append(self._handle_partitioned_by_property(spec.partitioned_by))
        return properties

    # %% ---- Transform handlers ------------------------------------------------------
    def _handle_transformation(
        self, column: str, transform: str, transform_args: list
    ) -> exp.Expression:
        from sqlglot import exp

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
        from sqlglot import exp

        self._validate_transform_args("cast", len(transform_args), 1)
        cast_to_type = transform_args[0].upper()
        try:
            target_type = exp.DataType.Type[cast_to_type]
        except KeyError:
            return self.raise_or_coerce(
                coerce_type=exp.Cast(
                    this=exp.column(column),
                    to=exp.DataType(this=self.config.fallback_type),
                ),
                error_msg=(
                    f"Transform type '{cast_to_type}' is not a valid sqlglot Type"
                    f" for '{self._field_context}'."
                ),
            )
        return exp.Cast(
            this=exp.column(column),
            to=exp.DataType(this=target_type),
        )

    def _handle_bucket_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        from sqlglot import exp

        self._validate_transform_args("bucket", len(transform_args), 1)
        return exp.PartitionedByBucket(
            this=exp.column(column), expression=exp.convert(transform_args[0])
        )

    def _handle_truncate_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        from sqlglot import exp

        self._validate_transform_args("truncate", len(transform_args), 1)
        return exp.PartitionByTruncate(
            this=exp.column(column), expression=exp.convert(transform_args[0])
        )

    def _handle_date_trunc_transform(
        self, column: str, transform_args: list
    ) -> exp.Expression:
        from sqlglot import exp

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

    # %% ---- Helpers -----------------------------------------------------------------
    def _convert_field(self, field: yspec.Field) -> exp.ColumnDef:
        from sqlglot import exp

        return exp.ColumnDef(
            this=exp.Identifier(this=field.name),
            kind=self._convert_type(field.type),
            constraints=None,
        )

    def _convert_column(self, column: yspec.Column) -> exp.ColumnDef:
        from sqlglot import exp

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

    def _convert_field_default(self, field: yspec.Field) -> exp.ColumnDef:
        if not isinstance(field, yspec.Column):  # Overrides happen on column level
            raise TypeError(f"Expected Column, got {type(field)}")
        return self._convert_column(field)

    def _collect_expressions(self, spec: yspec.YadsSpec) -> list[exp.Expression]:
        expressions: list[exp.Expression] = []
        for col in self._filter_columns(spec):
            with self.conversion_context(field=col.name):
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
        from sqlglot import exp

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
