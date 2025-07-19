# yads

`yads`: _~~Yet Another Data Spec~~_ **YAML-Augmented Data Specification** is a Python library for managing data specs using YAML. It helps you define and manage your data warehouse tables, schemas, and documentation in a structured, version-controlled way. With `yads`, you can define your data assets once in YAML and then generate DDL statements for different SQL dialects and schema objects for PySpark, PyArrow, and more.

## Why yads?

~~The modern data stack is complex, with data assets defined across a multitude of platforms and tools. This often leads to fragmented and inconsistent documentation, making data discovery and governance a challenge. `yads` was created to address this by providing a centralized, version-controllable, and extensible way to manage metadata for modern data platforms.~~

~~The main goal of `yads` is to provide a single source of truth for your data assets using simple YAML files. These files can capture everything from table schemas and column descriptions to governance policies and usage notes. From these specifications, `yads` can transpile the information into various formats, such as DDL statements for different SQL dialects, Avro or PyArrow schemas, and generate documentation that is ready for both humans and Large Language Models (LLMs).~~

## Getting Started

## Installation

```bash
pip install yads
```

## Usage

### Defining a Specification

Create a YAML file to define your table schema and properties. For example, `customer_orders.yaml`:

```yaml
name: "warehouse.orders.customer_orders"
version: "2.1.0"
description: "Represents a single customer order, including line items and shipping details."

options:
  if_not_exists: false
  or_replace: true

properties:
  partitioned_by:
    - column: "order_date"
    - column: "created_at"
      transform: "month"
  location: "/warehouse/orders/customer_orders"
  table_type: "iceberg"
  format: "parquet"
  write_compression: "snappy"

metadata:
  owner: "sales-engineering"
  retention_days: 90
  granularity: "One order per customer"

columns:
  - name: "order_id"
    type: "uuid"
    description: "The unique identifier for the order (PK)."
    constraints:
      not_null: true
      primary_key: true

  - name: "customer_id"
    type: "integer"
    description: "The ID of the customer who placed the order."
    constraints:
      not_null: true

  - name: "order_date"
    type: "date"
    description: "The date the order was placed."
    constraints:
      not_null: true

  - name: "order_total"
    type: "decimal"
    params:
      precision: 10
      scale: 2
    description: "Total monetary value of the order."
    constraints:
      not_null: true
      default: 0.5

  - name: "shipping_address"
    type: "string"
    params:
      length: 255
    description: "The full shipping address."

  - name: "line_items"
    type: "array"
    element:
      type: "struct"
      fields:
        - name: "product_id"
          type: "integer"
          constraints:
            not_null: true
        - name: "quantity"
          type: "integer"
          constraints:
            not_null: true
        - name: "price"
          type: "decimal"
          params:
            precision: 8
            scale: 2
          constraints:
            not_null: true
    description: "A list of products included in the order."

  - name: "tags"
    type: "array"
    element:
      type: "string"
    description: "A list of tags for the order."

  - name: "metadata_tags"
    type: "map"
    key:
      type: "string"
      params:
        length: 50
    value:
      type: "string"
      params:
        length: 255
    description: "A map of metadata tags for the order."

  - name: "created_at"
    type: "timestamp_tz"
    description: "The timestamp when the order record was created in the system."
    constraints:
      not_null: true
    metadata:
      source: "system_generated"
```

### Generating Spark DDL

You can generate a Spark DDL `CREATE TABLE` statement from the specification:

```python
import yads
from yads.converters.sql import SqlglotConverter

converter = SqlglotConverter()
spec = yads.from_yaml("examples/specs/yads_spec.yaml")

# Generate the DDL
ddl = converter.convert(spec).sql(dialect="spark", pretty=True)

print(ddl)
```

```sql
CREATE OR REPLACE TABLE warehouse.orders.customer_orders (
  order_id UUID NOT NULL PRIMARY KEY,
  customer_id INT NOT NULL,
  order_date DATE NOT NULL,
  order_total DECIMAL(10, 2) NOT NULL DEFAULT 0.5,
  shipping_address VARCHAR(255),
  line_items ARRAY<STRUCT<product_id INT NOT NULL, quantity INT NOT NULL, price DECIMAL(8, 2) NOT NULL>>,
  tags ARRAY<TEXT>,
  metadata_tags MAP<VARCHAR(50), VARCHAR(255)>,
  created_at TIMESTAMPTZ NOT NULL
)
PARTITIONED BY (order_date, MONTH(created_at))
LOCATION '/warehouse/orders/customer_orders'
TBLPROPERTIES (
  'table_type' = 'iceberg',
  'format' = 'parquet',
  'write_compression' = 'snappy'
)
```

### Available Converters

Currently, `yads` only supports generating SQL DDL statements via the `SqlglotConverter`. Support for other output formats, such as PySpark or PyArrow schemas, is planned for future releases.

## Contributing

We welcome contributions to `yads`!

### How to Create a New Converter

To create a converter from a `SchemaSpec` to another format (e.g., PyArrow schema, Spark schema), you should follow these steps. This guide focuses on creating converters that primarily deal with the `columns` section of a `yads` spec. The `SqlglotConverter` is a reference for handling SQL DDL-specific sections like `properties` and `options`.

#### 1. Create a Converter Class

Define a custom converter that inherits from `BaseConverter`.

```python
# src/yads/converters/my_format_converter.py
from typing import Any

from yads.converters.base import BaseConverter
from yads.spec import Field, SchemaSpec
from yads.types import (
    Array,
    Map,
    Struct,
    Type,
    # ... import other yads types as needed
)


class MyFormatConverter(BaseConverter):
    """
    A converter from a YADS SchemaSpec to MyFormat.
    """

    def convert(self, spec: SchemaSpec) -> Any:
        """
        Converts a SchemaSpec object into MyFormat.
        """
        # Your implementation here
        pass
```

#### 2. Implement the `convert` Method

The `convert` method is the entry point. It should transform the list of `yads.spec.Field` objects from `spec.columns` into your target schema representation.

```python
# Example implementation
def convert(self, spec: SchemaSpec) -> "MyTargetSchema":
    target_fields = [self._convert_field(field) for field in spec.columns]
    return MyTargetSchema(fields=target_fields)
```

#### 3. Handle Fields and Types with a Dispatcher

To keep an extensible design, use a dispatcher pattern to handle different `yads` types. Create a dictionary in your converter's `__init__` method that maps `yads` type classes to specific handler methods.

```python
from yads import types

class MyFormatConverter(BaseConverter):
    def __init__(self):
        self._type_handlers = {
            types.String: self._handle_string,
            types.Integer: self._handle_integer,
            types.Decimal: self._handle_decimal,
            types.Array: self._handle_array,
            types.Struct: self._handle_struct,
            types.Map: self._handle_map,
            # ... add handlers for all supported yads types
        }

    def convert(self, spec: SchemaSpec) -> "MyTargetSchema":
        # ...
        pass

    def _convert_field(self, field: Field) -> "MyTargetField":
        """Converts a YADS Field to a target field format."""
        target_type = self._convert_type(field.type)
        return MyTargetField(name=field.name, type=target_type)

    def _convert_type(self, yads_type: Type) -> "MyTargetType":
        """Dispatches to the appropriate type handler."""
        handler = self._type_handlers.get(type(yads_type))
        if not handler:
            raise NotImplementedError(f"No handler for type: {type(yads_type)}")
        return handler(yads_type)

    # --- Type Handlers ---

    def _handle_string(self, yads_type: types.String) -> "MyTargetType":
        # Logic to convert YADS String to MyTargetType
        # e.g., return MyStringType(length=yads_type.length)
        pass

    def _handle_integer(self, yads_type: types.Integer) -> "MyTargetType":
        # Logic to convert YADS Integer
        pass

    def _handle_decimal(self, yads_type: types.Decimal) -> "MyTargetType":
        # Logic for decimal with precision and scale
        pass

    def _handle_array(self, yads_type: types.Array) -> "MyTargetType":
        # Recursively convert the element type
        element_type = self._convert_type(yads_type.element)
        return MyTargetArray(element_type)

    def _handle_struct(self, yads_type: types.Struct) -> "MyTargetType":
        # Recursively convert fields of the struct
        struct_fields = [self._convert_field(f) for f in yads_type.fields]
        return MyTargetStruct(struct_fields)

    def _handle_map(self, yads_type: types.Map) -> "MyTargetType":
        # Recursively convert key and value types
        key_type = self._convert_type(yads_type.key)
        value_type = self._convert_type(yads_type.value)
        return MyTargetMap(key_type, value_type)
```

This approach ensures that new types can be supported by simply adding a new handler method and updating the `_type_handlers` dictionary, without modifying the existing logic.
