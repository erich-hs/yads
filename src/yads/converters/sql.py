from __future__ import annotations

from typing import Any, Callable

from sqlglot import exp
from sqlglot.expressions import convert

from yads.constraints import (
    DefaultConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
)
from yads.converters.base import BaseConverter
from yads.spec import Field, PartitionColumn, Properties, SchemaSpec
from yads.types import (
    Array,
    Date,
    Decimal,
    Integer,
    Map,
    String,
    Struct,
    TimestampTZ,
    Type,
    UUID,
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
                             These can be overridden in the `convert` method.
                             https://sqlglot.com/sqlglot/generator.html#Generator
        """
        self._ast_converter = ast_converter or SqlglotConverter()
        self._dialect = dialect
        self._convert_options = convert_options

    def convert(self, spec: SchemaSpec, **kwargs: Any) -> str:
        """
        Converts a yads SchemaSpec object into a SQL DDL string.

        Args:
            spec: The yads specification as a SchemaSpec object.
            kwargs: Additional keyword arguments to be passed to the AST converter,
                    overriding any options set during initialization.
                    https://sqlglot.com/sqlglot/generator.html#Generator

        Returns:
            A SQL DDL string formatted for the specified dialect.
        """
        ast = self._ast_converter.convert(spec)
        options = {**self._convert_options, **kwargs}
        return ast.sql(dialect=self._dialect, **options)


class SqlglotConverter(BaseConverter):
    """
    Converts a yads SchemaSpec object into a sqlglot Abstract
    Syntax Tree (AST).
    """

    def __init__(self) -> None:
        self._type_handlers: dict[type[Type], Callable[[Any], exp.DataType]] = {
            UUID: self._handle_uuid_type,
            Integer: self._handle_integer_type,
            Date: self._handle_date_type,
            TimestampTZ: self._handle_timestamptz_type,
            Decimal: self._handle_decimal_type,
            String: self._handle_string_type,
            Array: self._handle_array_type,
            Struct: self._handle_struct_type,
            Map: self._handle_map_type,
        }
        self._constraint_handlers: dict[type, Callable[[Any], exp.ColumnConstraint]] = {
            NotNullConstraint: self._handle_not_null_constraint,
            PrimaryKeyConstraint: self._handle_primary_key_constraint,
            DefaultConstraint: self._handle_default_constraint,
        }
        self._table_constraint_handlers: dict[type, Callable[[Any], exp.Expression]] = {
            PrimaryKeyTableConstraint: self._handle_primary_key_table_constraint,
        }
        self._property_handlers: dict[str, Callable[..., exp.Property]] = {
            "partitioned_by": self._handle_partitioned_by_property,
            "location": self._handle_location_property,
        }
        self._transform_handlers: dict[str, type[exp.Func]] = {
            "month": exp.Month,
            "year": exp.Year,
            "day": exp.Day,
        }

    def convert(self, spec: SchemaSpec) -> exp.Create:
        """
        Converts a yads SchemaSpec object into a sqlglot Create AST.

        Args:
            spec: The yads specification as a SchemaSpec object.

        Returns:
            A sqlglot Create Abstract Syntax Tree (AST).
            https://sqlglot.com/sqlglot/expressions.html#Create
        """
        namespace = spec.name.split(".")
        table_name = namespace[-1]
        db_name = namespace[-2] if len(namespace) > 1 else None
        catalog_name = namespace[-3] if len(namespace) > 2 else None

        properties = self._parse_properties(spec.properties)
        expressions: list[exp.Expression] = [
            self._convert_field(col) for col in spec.columns
        ]
        for constraint in spec.table_constraints:
            if handler := self._table_constraint_handlers.get(type(constraint)):
                expressions.append(handler(constraint))

        return exp.Create(
            this=exp.Schema(
                this=exp.Table(
                    this=exp.Identifier(this=table_name),
                    db=exp.Identifier(this=db_name) if db_name else None,
                    catalog=exp.Identifier(this=catalog_name) if catalog_name else None,
                ),
                expressions=expressions,
            ),
            kind="TABLE",
            exists=spec.options.if_not_exists,
            replace=spec.options.or_replace,
            properties=(exp.Properties(expressions=properties) if properties else None),
        )

    # Type handlers
    def _convert_type(self, yads_type: Type) -> exp.DataType:
        handler = self._type_handlers.get(type(yads_type))
        if not handler:
            raise NotImplementedError(
                f"No handler implemented for type: {type(yads_type)}"
            )
        return handler(yads_type)

    def _handle_uuid_type(self, yads_type: UUID) -> exp.DataType:
        return exp.DataType(this=exp.DataType.Type.UUID)

    def _handle_integer_type(self, yads_type: Integer) -> exp.DataType:
        return exp.DataType(this=exp.DataType.Type.INT)

    def _handle_date_type(self, yads_type: Date) -> exp.DataType:
        return exp.DataType(this=exp.DataType.Type.DATE)

    def _handle_timestamptz_type(self, yads_type: TimestampTZ) -> exp.DataType:
        return exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ)

    def _handle_decimal_type(self, yads_type: Decimal) -> exp.DataType:
        return exp.DataType(
            this=exp.DataType.Type.DECIMAL,
            expressions=[
                exp.DataTypeParam(this=convert(yads_type.precision)),
                exp.DataTypeParam(this=convert(yads_type.scale)),
            ],
        )

    def _handle_string_type(self, yads_type: String) -> exp.DataType:
        if yads_type.length:
            return exp.DataType(
                this=exp.DataType.Type.VARCHAR,
                expressions=[exp.DataTypeParam(this=convert(yads_type.length))],
            )
        return exp.DataType(this=exp.DataType.Type.TEXT)

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
    def _parse_properties(self, properties: Properties) -> list[exp.Property]:
        properties_expressions = []
        # Handled properties
        for key, value in properties.__dict__.items():
            if not value:
                continue
            if handler := self._property_handlers.get(key):
                expression = handler(value)
                properties_expressions.append(expression)

        # Generic properties
        generic_properties = {
            k: v
            for k, v in properties.__dict__.items()
            if k not in self._property_handlers and v is not None
        }
        for key, value in generic_properties.items():
            properties_expressions.append(self._handle_generic_property(key, value))

        return properties_expressions

    def _handle_partitioned_by_property(
        self, value: list[PartitionColumn]
    ) -> exp.PartitionedByProperty:
        schema_expressions = []
        for col in value:
            column_identifier = exp.Identifier(this=col.column)
            expression: exp.Expression = column_identifier
            if col.transform:
                transform_func = self._transform_handlers[col.transform]
                expression = transform_func(this=exp.Column(this=column_identifier))
            schema_expressions.append(expression)

        return exp.PartitionedByProperty(
            this=exp.Schema(expressions=schema_expressions)
        )

    def _handle_location_property(self, value: str) -> exp.LocationProperty:
        return exp.LocationProperty(this=exp.Literal(this=value, is_string=True))

    def _handle_generic_property(self, key: str, value: Any) -> exp.Property:
        return exp.Property(
            this=exp.Literal(this=key, is_string=True),
            value=exp.Literal(this=str(value), is_string=True),
        )

    # Field and constraint handlers
    def _convert_field(self, field: Field) -> exp.ColumnDef:
        constraints = []
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
