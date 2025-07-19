from __future__ import annotations

import yaml
from typing import Dict, Callable, Any
from sqlglot import exp, parse_one
from sqlglot.expressions import convert


# %% Loading YADS spec


_YADS_TO_SQLGLOT_TYPE_MAP: Dict[str, exp.DataType.Type] = {
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

_YADS_TO_SQLGLOT_TRANSFORM_MAP: Dict[str, type[exp.Func]] = {
    "month": exp.Month,
    "year": exp.Year,
    "day": exp.Day,
}

_YADS_TO_SQLGLOT_CONSTRAINT_MAP: Dict[str, type[exp.ColumnConstraintKind]] = {
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
    database_name = namespace[-2]
    catalog_name = namespace[-3]

    # Options
    options = spec.get("options", {})

    # Properties
    properties = spec.get("properties", {})

    # Columns
    columns = spec.get("columns", [])

    ast_from_spec = exp.Create(
        this=exp.Schema(
            this=exp.Table(
                this=exp.Identifier(this=table_name),
                db=exp.Identifier(this=database_name),
                catalog=exp.Identifier(this=catalog_name),
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
    return ast_from_spec


# %% Base SQL DDL


# %% AST from spec

## Property handlers
PropertyHandler = Callable[[Any], exp.Property]


def _create_partition_expression(col_spec: dict) -> exp.Expression:
    column_identifier = exp.Identifier(this=col_spec["column"])
    transform_name = col_spec.get("transform")
    if transform_name:
        transform_func = _YADS_TO_SQLGLOT_TRANSFORM_MAP[transform_name]
        return transform_func(this=exp.Column(this=column_identifier))
    return column_identifier


def _handle_partitioned_by_property(value: Any) -> exp.PartitionedByProperty:
    if not isinstance(value, list):
        raise TypeError(
            f"Expected a list for 'partitioned_by', but got {type(value).__name__}"
        )
    schema_expressions = [_create_partition_expression(col) for col in value]
    return exp.PartitionedByProperty(this=exp.Schema(expressions=schema_expressions))


def _handle_location_property(value: Any) -> exp.LocationProperty:
    if not isinstance(value, str):
        raise TypeError(
            f"Expected a string for 'location', but got {type(value).__name__}"
        )
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
        handler = property_handlers.get(key)
        if handler:
            expression = handler(value)
        else:
            expression = _handle_generic_property(key, value)
        properties_expressions.append(expression)
    return properties_expressions


## Column definition handlers
def _handle_struct_column_data_type(col_spec: dict) -> exp.DataType:
    """Handles struct column data type."""
    assert "fields" in col_spec, "Struct column must have fields"
    return exp.DataType(
        this=exp.DataType.Type.STRUCT,
        expressions=[_parse_column_def(field) for field in col_spec["fields"]],
        nested=exp.DataType.Type.STRUCT in exp.DataType.NESTED_TYPES,
    )


def _handle_map_column_data_type(col_spec: dict) -> exp.DataType:
    """Handles map column data type."""
    assert "key" in col_spec and "value" in col_spec, (
        "Map column must have key and value"
    )
    key_type = _handle_column_data_type(col_spec["key"])
    value_type = _handle_column_data_type(col_spec["value"])
    return exp.DataType(
        this=exp.DataType.Type.MAP,
        expressions=[key_type, value_type],
        nested=exp.DataType.Type.MAP in exp.DataType.NESTED_TYPES,
    )


def _handle_array_column_data_type(col_spec: dict) -> exp.DataType:
    assert "element" in col_spec, "Array column must have an element"
    element_type = _handle_column_data_type(col_spec["element"])
    return exp.DataType(
        this=exp.DataType.Type.ARRAY,
        expressions=[element_type],
        nested=exp.DataType.Type.ARRAY in exp.DataType.NESTED_TYPES,
    )


def _handle_column_data_type(col_spec: dict) -> exp.DataType:
    yads_type_str = col_spec["type"]
    params = col_spec.get("params", {})

    if yads_type_str == "array":
        return _handle_array_column_data_type(col_spec)
    if yads_type_str == "struct":
        return _handle_struct_column_data_type(col_spec)
    if yads_type_str == "map":
        return _handle_map_column_data_type(col_spec)

    # For primitive types
    sqlglot_type = _YADS_TO_SQLGLOT_TYPE_MAP[yads_type_str]
    if yads_type_str == "string" and "length" in params:
        sqlglot_type = exp.DataType.Type.VARCHAR

    data_type_expressions = []
    if params:
        for _, value in params.items():
            data_type_expressions.append(
                exp.DataTypeParam(this=convert(value)),
            )

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
    for constraint in constraints:
        constraint_class = _YADS_TO_SQLGLOT_CONSTRAINT_MAP[constraint]
        if constraint_class == exp.DefaultColumnConstraint:
            column_constraints.append(
                _handle_default_column_constraint(constraints[constraint])
            )
        else:
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
