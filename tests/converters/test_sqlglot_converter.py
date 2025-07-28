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


def test_create_external_table():
    """
    Tests creating an external table.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
options:
  is_external: true
columns:
  - name: "order_id"
    type: "uuid"
"""
    sql = "CREATE EXTERNAL TABLE my_catalog.my_db.my_table (order_id UUID)"
    assert_ast_equal(spec_yaml, sql)


def test_create_external_table_with_location():
    """
    Tests creating an external table with a location property.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
options:
  is_external: true
storage:
  location: "/path/to/table"
columns:
  - name: "order_id"
    type: "uuid"
"""
    sql = "CREATE EXTERNAL TABLE my_catalog.my_db.my_table (order_id UUID) LOCATION '/path/to/table'"
    assert_ast_equal(spec_yaml, sql)


def test_create_external_table_with_file_format():
    """
    Tests creating an external table with a file format.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
options:
  is_external: true
storage:
  format: "parquet"
columns:
  - name: "order_id"
    type: "uuid"
"""
    sql = (
        "CREATE EXTERNAL TABLE my_catalog.my_db.my_table (order_id UUID) USING parquet"
    )
    assert_ast_equal(spec_yaml, sql)


def test_create_external_table_with_location_and_file_format():
    """
    Tests creating an external table with a location and file format.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
options:
  is_external: true
storage:
  location: "/path/to/table"
  format: "parquet"
columns:
  - name: "order_id"
    type: "uuid"
"""
    sql = "CREATE EXTERNAL TABLE my_catalog.my_db.my_table (order_id UUID) USING parquet LOCATION '/path/to/table'"
    assert_ast_equal(spec_yaml, sql)


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


def test_create_table_with_decimal_params_out_of_order():
    """
    Tests creating a table with a decimal column where params are out of order.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_total"
    type: "decimal"
    params:
      scale: 2
      precision: 10
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_total DECIMAL(10, 2))"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_decimal_no_params():
    """
    Tests creating a table with a decimal column with no params.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
columns:
  - name: "order_total"
    type: "decimal"
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_total DECIMAL)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_location_property():
    """
    Tests creating a table with a location property.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
storage:
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
storage:
  format: "iceberg"
  tbl_properties:
    "my_prop": "my_value"
columns:
  - name: "order_id"
    type: "uuid"
"""
    sql = """
CREATE TABLE my_catalog.my_db.my_table (
  order_id UUID
)
USING iceberg
TBLPROPERTIES (
  'my_prop' = 'my_value'
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
    sql = "CREATE TABLE my_catalog.my_db.my_table (product_info STRUCT<product_id INT, product_name TEXT(255)>)"
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
    sql = "CREATE TABLE my_catalog.my_db.my_table (metadata MAP<TEXT, TEXT(100)>)"
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
    sql = "CREATE TABLE my_catalog.my_db.my_table (products ARRAY<STRUCT<product_id INT, product_name TEXT(255)>>)"
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
    sql = "CREATE TABLE my_catalog.my_db.my_table (item STRUCT<item_id INT, variants ARRAY<TEXT(20)>>)"
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
    sql = "CREATE TABLE my_catalog.my_db.my_table (attributes MAP<TEXT(50), INT>)"
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


def test_create_table_with_table_foreign_key():
    """
    Tests creating a table with a FOREIGN KEY constraint defined at the table level.
    """
    spec_yaml = """
name: "my_catalog.my_db.pets"
version: "1.0"
columns:
  - name: "name"
    type: "string"
  - name: "owner_first_name"
    type: "string"
  - name: "owner_last_name"
    type: "string"
table_constraints:
  - type: "foreign_key"
    name: "pets_persons_fk"
    columns: ["owner_first_name", "owner_last_name"]
    references:
      table: "my_catalog.my_db.persons"
"""
    sql = """
CREATE TABLE my_catalog.my_db.pets (
  name TEXT,
  owner_first_name TEXT,
  owner_last_name TEXT,
  CONSTRAINT pets_persons_fk FOREIGN KEY (owner_first_name, owner_last_name) REFERENCES my_catalog.my_db.persons
)
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_table_foreign_key_with_ref_columns():
    """
    Tests creating a table with a FOREIGN KEY constraint with referenced columns.
    """
    spec_yaml = """
name: "my_catalog.my_db.pets"
version: "1.0"
columns:
  - name: "name"
    type: "string"
  - name: "owner_first_name"
    type: "string"
  - name: "owner_last_name"
    type: "string"
table_constraints:
  - type: "foreign_key"
    name: "pets_persons_fk"
    columns: ["owner_first_name", "owner_last_name"]
    references:
      table: "my_catalog.my_db.persons"
      columns: ["first_name", "last_name"]
"""
    sql = """
CREATE TABLE my_catalog.my_db.pets (
  name TEXT,
  owner_first_name TEXT,
  owner_last_name TEXT,
  CONSTRAINT pets_persons_fk FOREIGN KEY (owner_first_name, owner_last_name) REFERENCES my_catalog.my_db.persons (first_name, last_name)
)
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_column_foreign_key():
    """
    Tests creating a table with a FOREIGN KEY constraint defined at the column level.
    """
    spec_yaml = """
name: "my_catalog.my_db.orders"
version: "1.0"
columns:
  - name: "order_id"
    type: "integer"
    constraints:
        not_null: true
        primary_key: true
  - name: "customer_id"
    type: "uuid"
    constraints:
      foreign_key:
        name: "orders_customers_fk"
        references:
          table: "my_catalog.my_db.customers"
"""
    sql = """
CREATE TABLE my_catalog.my_db.orders (
    order_id INT NOT NULL PRIMARY KEY,
    customer_id UUID CONSTRAINT orders_customers_fk REFERENCES my_catalog.my_db.customers
)"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_column_foreign_key_with_ref_column():
    """
    Tests creating a table with a column FOREIGN KEY constraint with a referenced column.
    """
    spec_yaml = """
name: "my_catalog.my_db.orders"
version: "1.0"
columns:
  - name: "order_id"
    type: "integer"
    constraints:
        not_null: true
        primary_key: true
  - name: "customer_id"
    type: "uuid"
    constraints:
      foreign_key:
        name: "orders_customers_fk"
        references:
          table: "my_catalog.my_db.customers"
          columns: ["id"]
"""
    sql = """
CREATE TABLE my_catalog.my_db.orders (
    order_id INT NOT NULL PRIMARY KEY,
    customer_id UUID CONSTRAINT orders_customers_fk REFERENCES my_catalog.my_db.customers (id)
)"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_empty_storage_properties():
    """
    Tests creating a table with an empty storage properties block.
    """
    spec_yaml = """
name: "my_catalog.my_db.my_table"
version: "1.0"
storage: {}
columns:
  - name: "order_id"
    type: "uuid"
"""
    sql = "CREATE TABLE my_catalog.my_db.my_table (order_id UUID)"
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_table_foreign_key_unqualified_ref():
    """
    Tests creating a table with a FOREIGN KEY constraint with an unqualified referenced table.
    """
    spec_yaml = """
name: "my_catalog.my_db.pets"
version: "1.0"
columns:
  - name: "name"
    type: "string"
  - name: "owner_first_name"
    type: "string"
  - name: "owner_last_name"
    type: "string"
table_constraints:
  - type: "foreign_key"
    name: "pets_persons_fk"
    columns: ["owner_first_name", "owner_last_name"]
    references:
      table: "persons"
      columns: ["first_name", "last_name"]
"""
    sql = """
CREATE TABLE my_catalog.my_db.pets (
  name TEXT,
  owner_first_name TEXT,
  owner_last_name TEXT,
  CONSTRAINT pets_persons_fk FOREIGN KEY (owner_first_name, owner_last_name) REFERENCES persons (first_name, last_name)
)
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_table_foreign_key_partially_qualified_ref():
    """
    Tests creating a table with a FOREIGN KEY constraint with a partially qualified referenced table.
    """
    spec_yaml = """
name: "my_catalog.my_db.pets"
version: "1.0"
columns:
  - name: "name"
    type: "string"
  - name: "owner_first_name"
    type: "string"
  - name: "owner_last_name"
    type: "string"
table_constraints:
  - type: "foreign_key"
    name: "pets_persons_fk"
    columns: ["owner_first_name", "owner_last_name"]
    references:
      table: "my_db.persons"
      columns: ["first_name", "last_name"]
"""
    sql = """
CREATE TABLE my_catalog.my_db.pets (
  name TEXT,
  owner_first_name TEXT,
  owner_last_name TEXT,
  CONSTRAINT pets_persons_fk FOREIGN KEY (owner_first_name, owner_last_name) REFERENCES my_db.persons (first_name, last_name)
)
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_column_foreign_key_unqualified_ref():
    """
    Tests creating a table with a column FOREIGN KEY constraint with an unqualified referenced table.
    """
    spec_yaml = """
name: "my_catalog.my_db.orders"
version: "1.0"
columns:
  - name: "order_id"
    type: "integer"
    constraints:
        not_null: true
        primary_key: true
  - name: "customer_id"
    type: "uuid"
    constraints:
      foreign_key:
        name: "orders_customers_fk"
        references:
          table: "customers"
          columns: ["id"]
"""
    sql = """
CREATE TABLE my_catalog.my_db.orders (
    order_id INT NOT NULL PRIMARY KEY,
    customer_id UUID CONSTRAINT orders_customers_fk REFERENCES customers (id)
)"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_column_foreign_key_partially_qualified_ref():
    """
    Tests creating a table with a column FOREIGN KEY constraint with a partially qualified referenced table.
    """
    spec_yaml = """
name: "my_catalog.my_db.orders"
version: "1.0"
columns:
  - name: "order_id"
    type: "integer"
    constraints:
        not_null: true
        primary_key: true
  - name: "customer_id"
    type: "uuid"
    constraints:
      foreign_key:
        name: "orders_customers_fk"
        references:
          table: "my_db.customers"
          columns: ["id"]
"""
    sql = """
CREATE TABLE my_catalog.my_db.orders (
    order_id INT NOT NULL PRIMARY KEY,
    customer_id UUID CONSTRAINT orders_customers_fk REFERENCES my_db.customers (id)
)"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_partition_by_bucket():
    """
    Tests creating a table with a PARTITIONED BY (bucket(...)) clause.
    """
    spec_yaml = """
name: "orders"
version: "1.0"
storage:
  format: "iceberg"
partitioned_by:
  - column: "customer_id"
    transform: "bucket"
    transform_args: [10]
  - column: "order_date"
    transform: "bucket"
    transform_args: [5]
columns:
  - name: "order_id"
    type: "integer"
  - name: "customer_id"
    type: "integer"
  - name: "order_date"
    type: "date"
  - name: "total_amount"
    type: "float"
"""
    sql = """
CREATE TABLE orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  total_amount DOUBLE
)
USING iceberg
PARTITIONED BY (bucket(customer_id, 10), bucket(order_date, 5))
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_partition_by_truncate():
    """
    Tests creating a table with a PARTITIONED BY (truncate(...)) clause.
    """
    spec_yaml = """
name: "user_data"
version: "1.0"
storage:
  format: "iceberg"
partitioned_by:
  - column: "email"
    transform: "truncate"
    transform_args: [3]
columns:
  - name: "user_id"
    type: "integer"
  - name: "email"
    type: "string"
  - name: "name"
    type: "string"
  - name: "created_at"
    type: "timestamp"
"""
    sql = """
CREATE TABLE user_data (
  user_id INT,
  email TEXT,
  name TEXT,
  created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (TRUNCATE(email, 3))
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_partition_by_date_parts():
    """
    Tests creating a table with a PARTITIONED BY clause with year, month, and day transforms.
    """
    spec_yaml = """
name: "events"
version: "1.0"
partitioned_by:
  - column: "eventType"
  - column: "eventTime"
    transform: "year"
  - column: "eventTime"
    transform: "month"
  - column: "eventTime"
    transform: "day"
columns:
  - name: "eventId"
    type: "integer"
  - name: "data"
    type: "string"
  - name: "eventType"
    type: "string"
  - name: "eventTime"
    type: "timestamp"
"""
    sql = """
CREATE TABLE events(
    eventId INT,
    data TEXT,
    eventType TEXT,
    eventTime TIMESTAMP
)
PARTITIONED BY (eventType, YEAR(eventTime), MONTH(eventTime), DAY(eventTime))
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_generated_columns_date_cast():
    """
    Tests creating a table with a generated column that casts a timestamp to a date.
    """
    spec_yaml = """
name: "default.people10m"
version: "1.0"
columns:
  - name: "id"
    type: "integer"
  - name: "birthDate"
    type: "timestamp"
  - name: "dateOfBirth"
    type: "date"
    generated_as:
      column: "birthDate"
      transform: "cast"
      transform_args: ["DATE"]
"""
    sql = """
CREATE TABLE default.people10m (
  id INT,
  birthDate TIMESTAMP,
  dateOfBirth DATE GENERATED ALWAYS AS (CAST(birthDate AS DATE))
)
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_generated_columns_date_parts():
    """
    Tests creating a table with generated columns for year, month, and day from a timestamp.
    """
    spec_yaml = """
name: "events"
version: "1.0"
columns:
  - name: "eventId"
    type: "integer"
  - name: "eventTime"
    type: "timestamp"
  - name: "eventYear"
    type: "integer"
    generated_as:
      column: "eventTime"
      transform: "year"
  - name: "eventMonth"
    type: "integer"
    generated_as:
      column: "eventTime"
      transform: "month"
  - name: "eventDay"
    type: "integer"
    generated_as:
      column: "eventTime"
      transform: "day"
"""
    sql = """
CREATE TABLE events (
  eventId INT,
  eventTime TIMESTAMP,
  eventYear INT GENERATED ALWAYS AS (YEAR(eventTime)),
  eventMonth INT GENERATED ALWAYS AS (MONTH(eventTime)),
  eventDay INT GENERATED ALWAYS AS (DAY(eventTime))
)
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_generated_columns_and_partitioning():
    """
    Tests creating a table with generated columns that are also used for partitioning.
    """
    spec_yaml = """
name: "events"
version: "1.0"
partitioned_by:
  - column: "eventType"
  - column: "eventYear"
  - column: "eventMonth"
  - column: "eventDay"
columns:
  - name: "eventId"
    type: "integer"
  - name: "eventType"
    type: "string"
  - name: "eventTime"
    type: "timestamp"
  - name: "eventYear"
    type: "integer"
    generated_as:
      column: "eventTime"
      transform: "year"
  - name: "eventMonth"
    type: "integer"
    generated_as:
      column: "eventTime"
      transform: "month"
  - name: "eventDay"
    type: "integer"
    generated_as:
      column: "eventTime"
      transform: "day"
"""
    sql = """
CREATE TABLE events(
    eventId INT,
    eventType TEXT,
    eventTime TIMESTAMP,
    eventYear INT GENERATED ALWAYS AS (YEAR(eventTime)),
    eventMonth INT GENERATED ALWAYS AS (MONTH(eventTime)),
    eventDay INT GENERATED ALWAYS AS (DAY(eventTime))
)
PARTITIONED BY (eventType, eventYear, eventMonth, eventDay)
"""
    assert_ast_equal(spec_yaml, sql)


def test_create_table_with_identity_columns():
    """
    Tests creating a table with various identity column configurations.
    """
    spec_yaml = """
name: "my_table"
version: "1.0"
columns:
  - name: "id_col1"
    type: "integer"
    constraints:
      identity:
        always: true
  - name: "id_col2"
    type: "integer"
    constraints:
      identity:
        always: true
        start: -1
        increment: 1
  - name: "id_col3"
    type: "integer"
    constraints:
      identity:
        always: false
  - name: "id_col4"
    type: "integer"
    constraints:
      identity:
        always: false
        start: -1
        increment: 1
"""
    sql = """
CREATE TABLE my_table (
  id_col1 INT GENERATED ALWAYS AS IDENTITY,
  id_col2 INT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
  id_col3 INT GENERATED BY DEFAULT AS IDENTITY,
  id_col4 INT GENERATED BY DEFAULT AS IDENTITY (START WITH -1 INCREMENT BY 1)
)
"""
    assert_ast_equal(spec_yaml, sql)
