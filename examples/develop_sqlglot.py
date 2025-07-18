import yaml
from typing import Dict, Callable, Any
from sqlglot import exp, parse_one

# %% Loading YADS spec
yaml_file = "examples/specs/yads_spec.yaml"

with open(yaml_file, "r") as f:
    spec = yaml.safe_load(f)

_YADS_TO_SQLGLOT_TYPE_MAP: Dict[str, exp.DataType.Type] = {
    "uuid": exp.DataType.Type.UUID,
    "integer": exp.DataType.Type.INT,
    "date": exp.DataType.Type.DATE,
    "decimal": exp.DataType.Type.DECIMAL,
    "string": exp.DataType.Type.VARCHAR,
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

# Namespace
namespace = spec["name"].split(".")
table_name = namespace[-1]
database_name = namespace[-2]
catalog_name = namespace[-3]

# Options
options = spec["options"]

# Properties
properties = spec["properties"]
print(properties)

# Metadata
metadata = spec["metadata"]

# Columns
columns = spec["columns"]

# %% Base SQL DDL
sql_ddl = """
CREATE TABLE warehouse.orders.customer_orders (
    order_id UUID NOT NULL
)
PARTITIONED BY (order_date, MONTH(created_at))
LOCATION '/warehouse/orders/customer_orders'
TBLPROPERTIES (
    'table_type'='iceberg',
    'format'='parquet',
    'write_compression'='snappy'
)
"""

ast_from_sql = parse_one(sql_ddl)

# %% AST from spec

"""
properties = {
    "partitioned_by": [ -> PartitionedByProperty
        {"column": "order_date"},
        {"column": "created_at", "transform": "month"}
    ],
    "location": "/warehouse/orders/customer_orders", -> LocationProperty
    "table_type": "iceberg", -> Property
    "format": "parquet", -> Property
    "write_compression": "snappy" -> Property
}
"""


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


PropertyHandler = Callable[[Any], exp.Property]


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


ast_from_spec = exp.Create(
    this=exp.Schema(
        this=exp.Table(
            this=exp.Identifier(this=table_name),
            db=exp.Identifier(this=database_name),
            catalog=exp.Identifier(this=catalog_name),
        ),
        expressions=[
            # for loop over columns here
            exp.ColumnDef(
                this=exp.Identifier(this=columns[0]["name"]),
                kind=exp.DataType(
                    this=_YADS_TO_SQLGLOT_TYPE_MAP[columns[0]["type"]],
                    nested=_YADS_TO_SQLGLOT_TYPE_MAP[columns[0]["type"]]
                    in exp.DataType.NESTED_TYPES,
                ),
                # for loop over column constraints here
                constraints=[exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())],
            )
        ],
    ),
    kind="TABLE",
    # exists=options.get("if_not_exists", False),
    # replace=options.get("or_replace", False),
    properties=exp.Properties(expressions=_parse_properties(properties)),
)

# %% Validate
print("AST from SQL:")
print(repr(ast_from_sql))
print("\n" + "=" * 60)

print("AST from spec:")
print(repr(ast_from_spec))
print("\n" + "=" * 60)

print(f"AST are equal: {ast_from_sql == ast_from_spec}")
print("SQL from AST:")
print(ast_from_spec.sql(dialect="athena", pretty=True))
