from sqlglot import parse_one

from yads.converters.sql import SqlglotConverter
from yads.loader import from_string


def assert_ast_equal(spec_yaml: str, sql: str):
    """
    Helper function to assert that the AST from a YADS spec is equal to the AST from a SQL query.
    """
    schema_spec = from_string(spec_yaml)
    converter = SqlglotConverter()
    ast_from_spec = converter.convert(schema_spec)
    ast_from_sql = parse_one(sql)

    assert ast_from_spec == ast_from_sql, (
        f"AST from spec and SQL are not equal.\n\n"
        f"Spec AST: {repr(ast_from_spec)}\n\n"
        f"SQL AST:  {repr(ast_from_sql)}"
    )


def test_create_table_with_single_column():
    """
    Tests creating a table with a single column.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_id"
    type: "uuid"
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_id UUID)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_parameterized_column():
    """
    Tests creating a table with a single parameterized column.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_total"
    type: "decimal"
    params:
      precision: 10
      scale: 2
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_total DECIMAL(10, 2))"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_location_property():
    """
    Tests creating a table with a location property.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
properties:
  location: "/path/to/table"
columns:
  - name: "order_id"
    type: "uuid"
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_id UUID) LOCATION '/path/to/table'"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_partition_by():
    """
    Tests creating a table with a PARTITIONED BY clause.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
properties:
  partitioned_by:
    - column: "order_date"
    - column: "created_at"
      transform: "month"
columns:
  - name: "order_id"
    type: "uuid"
  - name: "order_date"
    type: "date"
  - name: "created_at"
    type: "timestamp_tz"
"""
    sql = """
CREATE TABLE my_catalog.my_db.my_table (
  order_id UUID,
  order_date DATE,
  created_at TIMESTAMPTZ
)
PARTITIONED BY (order_date, MONTH(created_at))
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_generic_properties():
    """
    Tests creating a table with generic TBLPROPERTIES.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
properties:
  table_type: "iceberg"
  format: "parquet"
columns:
  - name: "order_id"
    type: "uuid"
"""
    sql = """
CREATE TABLE my_catalog.my_db.my_table (
  order_id UUID
)
TBLPROPERTIES (
  'table_type' = 'iceberg',
  'format' = 'parquet'
)
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_struct():
    """
    Tests creating a table with a struct column.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "line_items"
    type: "struct"
    fields:
      - name: "product_id"
        type: "integer"
      - name: "quantity"
        type: "integer"
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (line_items STRUCT<product_id INT, quantity INT>)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_map():
    """
    Tests creating a table with a map column.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "metadata_tags"
    type: "map"
    key:
      type: "string"
    value:
      type: "string"
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (metadata_tags MAP<TEXT, TEXT>)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_array_of_strings():
    """
    Tests creating a table with an array of strings.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "tags"
    type: "array"
    element:
      type: "string"
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (tags ARRAY<TEXT>)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_array_of_structs():
    """
    Tests creating a table with an array of structs.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
    - name: "line_items"
      type: "array"
      element:
        type: "struct"
        fields:
          - name: "product_id"
            type: "integer"
          - name: "quantity"
            type: "integer"
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (line_items ARRAY<STRUCT<product_id INT, quantity INT>>)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_not_null_constraint():
    """
    Tests creating a table with a NOT NULL constraint.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_id"
    type: "uuid"
    constraints:
      not_null: true
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_id UUID NOT NULL)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_primary_key_constraint():
    """
    Tests creating a table with a PRIMARY KEY constraint.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_id"
    type: "uuid"
    constraints:
      primary_key: true
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_id UUID PRIMARY KEY)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_default_value_constraint():
    """
    Tests creating a table with a DEFAULT value constraint.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_total"
    type: "decimal"
    params:
        precision: 10
        scale: 2
    constraints:
      default: 0.5
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_total DECIMAL(10, 2) DEFAULT 0.5)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_composite_primary_key():
    """
    Tests creating a table with a composite PRIMARY KEY constraint defined at the table level.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_id"
    type: "uuid"
  - name: "line_item_id"
    type: "integer"
table_constraints:
  - type: "primary_key"
    columns: ["order_id", "line_item_id"]
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_id UUID, line_item_id INT, PRIMARY KEY (order_id, line_item_id))"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_named_composite_primary_key():
    """
    Tests creating a table with a named composite PRIMARY KEY constraint.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_id"
    type: "uuid"
  - name: "line_item_id"
    type: "integer"
table_constraints:
  - type: "primary_key"
    name: "pk_orders"
    columns: ["order_id", "line_item_id"]
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_id UUID, line_item_id INT, CONSTRAINT pk_orders PRIMARY KEY (order_id, line_item_id))"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_single_column_table_primary_key():
    """
    Tests creating a table with a single-column PRIMARY KEY constraint defined at the table level.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_id"
    type: "uuid"
table_constraints:
  - type: "primary_key"
    columns: ["order_id"]
"""
    sql = (
        "CREATE TABLE my_catalog.my_db.my_table (order_id UUID, PRIMARY KEY (order_id))"
    )
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_array_of_parameterized_type():
    """
    Tests creating a table with an array of a parameterized type (e.g., DECIMAL).
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "prices"
    type: "array"
    element:
      type: "decimal"
      params:
        precision: 10
        scale: 2
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (prices ARRAY<DECIMAL(10, 2)>)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_struct_with_parameterized_field():
    """
    Tests creating a table with a struct that contains a parameterized field.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "product_info"
    type: "struct"
    fields:
      - name: "product_id"
        type: "integer"
      - name: "product_name"
        type: "string"
        params:
          length: 255
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (product_info STRUCT<product_id INT, product_name VARCHAR(255)>)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_map_with_parameterized_value():
    """
    Tests creating a table with a map that has a parameterized value type.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "metadata"
    type: "map"
    key:
      type: "string"
    value:
      type: "string"
      params:
        length: 100
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (metadata MAP<TEXT, VARCHAR(100)>)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_array_of_structs_with_parameterized_field():
    """
    Tests creating a table with an array of structs where the struct has a parameterized field.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "products"
    type: "array"
    element:
      type: "struct"
      fields:
        - name: "product_id"
          type: "integer"
        - name: "product_name"
          type: "string"
          params:
            length: 255
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (products ARRAY<STRUCT<product_id INT, product_name VARCHAR(255)>>)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_struct_with_array_of_parameterized_type():
    """
    Tests creating a table with a struct containing an array of a parameterized type.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "item"
    type: "struct"
    fields:
      - name: "item_id"
        type: "integer"
      - name: "variants"
        type: "array"
        element:
          type: "string"
          params:
            length: 20
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (item STRUCT<item_id INT, variants ARRAY<VARCHAR(20)>>)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_map_with_parameterized_key():
    """
    Tests creating a table with a map that has a parameterized key type.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "attributes"
    type: "map"
    key:
      type: "string"
      params:
        length: 50
    value:
      type: "integer"
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (attributes MAP<VARCHAR(50), INT>)"
    assert_ast_equal(spec_yaml, sql)


def test_full_spec_from_file():
    """
    Tests the full spec from the yads_spec.yaml file against the corresponding SQL file.
    """
    with open("examples/specs/yads_spec.yaml", "r") as f:
        spec_yaml = f.read()

    with open("examples/specs/yads_spec.sql", "r") as f:
        sql = f.read()

    assert_ast_equal(spec_yaml, sql)
