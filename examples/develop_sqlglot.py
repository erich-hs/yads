from __future__ import annotations

from collections.abc import Callable
from typing import Any

import yaml
from sqlglot import exp, parse_one
from sqlglot.expressions import convert


# %% Loading YADS spec


_YADS_TO_SQLGLOT_TYPE_MAP: dict[str, exp.DataType.Type] = {
    "uuid": exp.DataType.Type.UUID,
    "integer": exp.DataType.Type.INT,
    "date": exp.DataType.Type.DATE,
    "decimal": exp.DataType.Type.DECIMAL,
    "string": exp.DataType.Type.TEXT,
    "timestamp_tz": exp.DataType.Type.TIMESTAMPTZ,
    "array": exp.DataType.Type.ARRAY,
    "struct": exp.DataType.Type.STRUCT,
    "map": exp.DataType.Type.MAP,
}

_YADS_TO_SQLGLOT_TRANSFORM_MAP: dict[str, type[exp.Func]] = {
    "month": exp.Month,
    "year": exp.Year,
    "day": exp.Day,
}

_YADS_TO_SQLGLOT_CONSTRAINT_MAP: dict[str, type[exp.ColumnConstraintKind]] = {
    "not_null": exp.NotNullColumnConstraint,
    "primary_key": exp.PrimaryKeyColumnConstraint,
    "default": exp.DefaultColumnConstraint,
}


def yads_to_sqlglot_ast(spec: dict) -> exp.Create:
    """
    Converts a YADS spec dictionary into a sqlglot Create AST.

    Args:
        spec: The YADS specification as a dictionary.

    Returns:
        A sqlglot Create expression.
    """
    # Namespace
    namespace = spec["name"].split(".")
    table_name = namespace[-1]
    db_name = namespace[-2] if len(namespace) > 1 else None
    catalog_name = namespace[-3] if len(namespace) > 2 else None

    # Options
    options = spec.get("options", {})

    # Properties
    properties = spec.get("properties", {})

    # Columns
    columns = spec.get("columns", [])

    return exp.Create(
        this=exp.Schema(
            this=exp.Table(
                this=exp.Identifier(this=table_name),
                db=exp.Identifier(this=db_name) if db_name else None,
                catalog=exp.Identifier(this=catalog_name) if catalog_name else None,
            ),
            expressions=[_parse_column_def(col) for col in columns],
        ),
        kind="TABLE",
        exists=options.get("if_not_exists", False),
        replace=options.get("or_replace", False),
        properties=(
            exp.Properties(expressions=_parse_properties(properties))
            if properties
            else None
        ),
    )


# Property handlers
PropertyHandler = Callable[[Any], exp.Property]


def _create_partition_expression(col_spec: dict) -> exp.Expression:
    column_identifier = exp.Identifier(this=col_spec["column"])
    if transform_name := col_spec.get("transform"):
        transform_func = _YADS_TO_SQLGLOT_TRANSFORM_MAP[transform_name]
        return transform_func(this=exp.Column(this=column_identifier))
    return column_identifier


def _handle_partitioned_by_property(value: list) -> exp.PartitionedByProperty:
    schema_expressions = [_create_partition_expression(col) for col in value]
    return exp.PartitionedByProperty(this=exp.Schema(expressions=schema_expressions))


def _handle_location_property(value: str) -> exp.LocationProperty:
    return exp.LocationProperty(this=exp.Literal(this=value, is_string=True))


def _handle_generic_property(key: str, value: Any) -> exp.Property:
    return exp.Property(
        this=exp.Literal(this=key, is_string=True),
        value=exp.Literal(this=str(value), is_string=True),
    )


def _parse_properties(properties: dict) -> list[exp.Property]:
    property_handlers: dict[str, PropertyHandler] = {
        "partitioned_by": _handle_partitioned_by_property,
        "location": _handle_location_property,
    }

    properties_expressions = []
    for key, value in properties.items():
        if handler := property_handlers.get(key):
            try:
                expression = handler(value)
            except TypeError as e:
                raise ValueError(f"Invalid type for property '{key}': {e}") from e
        else:
            expression = _handle_generic_property(key, value)
        properties_expressions.append(expression)

    return properties_expressions


# Column definition handlers
def _handle_struct_column_data_type(col_spec: dict) -> exp.DataType:
    """Handles struct column data type."""
    try:
        fields = col_spec["fields"]
    except KeyError as e:
        raise ValueError("Struct column must have 'fields'") from e

    return exp.DataType(
        this=exp.DataType.Type.STRUCT,
        expressions=[_parse_column_def(field) for field in fields],
        nested=exp.DataType.Type.STRUCT in exp.DataType.NESTED_TYPES,
    )


def _handle_map_column_data_type(col_spec: dict) -> exp.DataType:
    """Handles map column data type."""
    try:
        key_spec = col_spec["key"]
        value_spec = col_spec["value"]
    except KeyError as e:
        raise ValueError("Map column must have 'key' and 'value'") from e

    key_type = _handle_column_data_type(key_spec)
    value_type = _handle_column_data_type(value_spec)
    return exp.DataType(
        this=exp.DataType.Type.MAP,
        expressions=[key_type, value_type],
        nested=exp.DataType.Type.MAP in exp.DataType.NESTED_TYPES,
    )


def _handle_array_column_data_type(col_spec: dict) -> exp.DataType:
    """Handles array column data type."""
    try:
        element_spec = col_spec["element"]
    except KeyError as e:
        raise ValueError("Array column must have an 'element'") from e
    element_type = _handle_column_data_type(element_spec)
    return exp.DataType(
        this=exp.DataType.Type.ARRAY,
        expressions=[element_type],
        nested=exp.DataType.Type.ARRAY in exp.DataType.NESTED_TYPES,
    )


def _handle_column_data_type(col_spec: dict) -> exp.DataType:
    yads_type_str = col_spec["type"]
    params = col_spec.get("params", {})

    type_handlers = {
        "array": _handle_array_column_data_type,
        "struct": _handle_struct_column_data_type,
        "map": _handle_map_column_data_type,
    }
    if handler := type_handlers.get(yads_type_str):
        return handler(col_spec)

    # For primitive types
    sqlglot_type = _YADS_TO_SQLGLOT_TYPE_MAP[yads_type_str]
    if yads_type_str == "string" and "length" in params:
        sqlglot_type = exp.DataType.Type.VARCHAR

    data_type_expressions = [
        exp.DataTypeParam(this=convert(value)) for value in params.values()
    ]

    return exp.DataType(
        this=sqlglot_type,
        expressions=data_type_expressions if data_type_expressions else None,
        nested=sqlglot_type in exp.DataType.NESTED_TYPES,
    )


def _parse_column_def(col_spec: dict) -> exp.ColumnDef:
    constraints = col_spec.get("constraints")
    return exp.ColumnDef(
        this=exp.Identifier(this=col_spec["name"]),
        kind=_handle_column_data_type(col_spec),
        constraints=_parse_column_constraints(constraints) if constraints else None,
    )


## Column constraint handlers
def _handle_default_column_constraint(value: Any) -> exp.ColumnConstraint:
    return exp.ColumnConstraint(kind=exp.DefaultColumnConstraint(this=convert(value)))


def _parse_column_constraints(constraints: dict) -> list[exp.ColumnConstraint]:
    column_constraints: list[exp.ColumnConstraint] = []
    for constraint, value in constraints.items():
        constraint_class = _YADS_TO_SQLGLOT_CONSTRAINT_MAP[constraint]
        if constraint_class == exp.DefaultColumnConstraint:
            column_constraints.append(_handle_default_column_constraint(value))
        elif value:  # value for a boolean constraint is always True
            column_constraints.append(exp.ColumnConstraint(kind=constraint_class()))
    return column_constraints


# %% AST from spec
# This section is for interactive development and will be skipped when imported.
if __name__ == "__main__":
    sql_ddl = """
CREATE TABLE warehouse.orders.customer_orders (
    order_id UUID
)
PARTITIONED BY (order_date, MONTH(created_at))
LOCATION '/warehouse/orders/customer_orders'
TBLPROPERTIES (
    'table_type'='iceberg',
    'format'='parquet',
    'write_compression'='snappy'
)
"""
    # PARTITIONED BY (order_date, MONTH(created_at))
    # LOCATION '/warehouse/orders/customer_orders'
    # TBLPROPERTIES (
    #     'table_type'='iceberg',
    #     'format'='parquet',
    #     'write_compression'='snappy'
    # )

    ast_from_sql = parse_one(sql_ddl)

    yaml_file = "examples/specs/yads_spec.yaml"

    with open(yaml_file, "r") as f:
        spec = yaml.safe_load(f)

    ast_from_spec = yads_to_sqlglot_ast(spec)

    # %% Validate
    # print("AST from SQL:")
    # print(repr(ast_from_sql))
    # print("\n" + "=" * 60)

    print("AST from spec:")
    print(repr(ast_from_spec))
    print("\n" + "=" * 60)

    # print(f"AST are equal: {ast_from_sql == ast_from_spec}")
    # print("\n" + "=" * 60)

    print("SQL from AST:\n")
    for dialect in [None, "spark"]:
        print(f"Dialect: {dialect or 'sqlglot'}\n")
        print(ast_from_spec.sql(dialect=dialect, pretty=True))
        print("\n" + "-" * 60)
