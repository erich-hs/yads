from __future__ import annotations

from typing import Any, Callable

from sqlglot import exp
from sqlglot.expressions import convert

from yads.constraints import (
    DefaultConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
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


class SqlglotConverter(BaseConverter):
    """
    Converts a YADS SchemaSpec object into a sqlglot Create Abstract
    Syntax Tree (AST).
    """

    def __init__(self) -> None:
        self._type_handlers: dict[type[Type], Callable[[Any], exp.DataType]] = {
            UUID: lambda t: exp.DataType(this=exp.DataType.Type.UUID),
            Integer: lambda t: exp.DataType(this=exp.DataType.Type.INT),
            Date: lambda t: exp.DataType(this=exp.DataType.Type.DATE),
            TimestampTZ: lambda t: exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ),
            Decimal: self._handle_decimal_type,
            String: self._handle_string_type,
            Array: self._handle_array_type,
            Struct: self._handle_struct_type,
            Map: self._handle_map_type,
        }
        self._constraint_handlers: dict[str, Callable[[Any], exp.ColumnConstraint]] = {
            "not_null": self._handle_not_null_constraint,
            "primary_key": self._handle_primary_key_constraint,
            "default": self._handle_default_constraint,
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
        Converts a YADS SchemaSpec object into a sqlglot Create AST.

        Args:
            spec: The YADS specification as a SchemaSpec object.

        Returns:
            A sqlglot Create expression.
        """
        namespace = spec.name.split(".")
        table_name = namespace[-1]
        db_name = namespace[-2] if len(namespace) > 1 else None
        catalog_name = namespace[-3] if len(namespace) > 2 else None

        properties = self._parse_properties(spec.properties)

        return exp.Create(
            this=exp.Schema(
                this=exp.Table(
                    this=exp.Identifier(this=table_name),
                    db=exp.Identifier(this=db_name) if db_name else None,
                    catalog=exp.Identifier(this=catalog_name) if catalog_name else None,
                ),
                expressions=[self._convert_field(col) for col in spec.columns],
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
            if isinstance(constraint, NotNullConstraint):
                constraints.append(self._handle_not_null_constraint(constraint))
            elif isinstance(constraint, PrimaryKeyConstraint):
                constraints.append(self._handle_primary_key_constraint(constraint))
            elif isinstance(constraint, DefaultConstraint):
                constraints.append(self._handle_default_constraint(constraint))

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
