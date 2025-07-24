# yads

`yads`: _~~Yet Another Data Spec~~_ **YAML-Augmented Data Specification** is a Python library for managing data specs using YAML. It helps you define your data warehouse tables, schemas, and documentation in a structured, version-controlled way. With `yads`, you can specify your data assets once in YAML and then generate DDL statements for different SQL dialects and schema objects for PySpark, PyArrow, and more.

## Why yads?

In modern data platforms, data assets are often defined across a multitude of tools, leading to fragmentation and inconsistency. `yads` addresses this by providing a centralized, version-controllable, and extensible way to manage your data assets using simple YAML files.

## Getting Started

### Installation

```bash
pip install yads
```

### Usage

#### Defining a Specification

Create a YAML file to define your table schema and properties. See the latest version of the [`yads` Specification](https://github.com/erich-hs/yads/blob/refactor/examples/specs/yads_spec.yaml) for a comprehensive example.

#### Generating SQL DDL

You can generate a `CREATE TABLE` statement for a specific SQL dialect from your YAML specification:

```python
import yads
from yads.converters.sql import SqlConverter

# Load the specification from a YAML file
spec = yads.from_yaml("examples/specs/yads_spec.yaml")

# Initialize a converter for the "spark" dialect
spark_sql_converter = SqlConverter(dialect="spark")

# Generate the DDL statement
ddl = spark_sql_converter.convert(spec, pretty=True)

print(ddl)
```

This will produce the following SQL DDL:

```sql
CREATE OR REPLACE TABLE warehouse.orders.customer_orders (
  order_id UUID NOT NULL PRIMARY KEY,
  customer_id INT NOT NULL,
  order_date DATE NOT NULL,
  order_total DECIMAL(10, 2) NOT NULL DEFAULT 0.5,
  shipping_address VARCHAR(255),
  line_items ARRAY<STRUCT<product_id INT NOT NULL, quantity INT NOT NULL, price DECIMAL(8, 2) NOT NULL>>,
  tags ARRAY<STRING>,
  metadata_tags MAP<VARCHAR(50), VARCHAR(255)>,
  created_at TIMESTAMPTZ NOT NULL
)
PARTITIONED BY (
  order_date,
  MONTH(created_at)
)
LOCATION '/warehouse/orders/customer_orders'
TBLPROPERTIES (
  'table_type' = 'iceberg',
  'format' = 'parquet',
  'write_compression' = 'snappy'
)
```

### Available Converters

Currently, `yads` supports generating SQL DDL statements via the `SqlConverter`. Support for other output formats, such as PySpark or PyArrow schemas, is planned for future releases.

#### Officially Supported SQL Dialects

While the generic `SqlConverter` can generate DDL for any dialect supported by `sqlglot`, we also provide officially supported converters for specific dialects. These specialized converters (e.g., `SparkSQLConverter`) perform additional validation to ensure that the generated DDL is fully compatible with the target dialect, providing a higher level of reliability.

To use an officially supported converter, simply import and instantiate it directly:

```python
from yads.converters import SparkSQLConverter

# The SparkSQLConverter will automatically handle the "spark" dialect
# and perform Spark-specific validation on your spec.
spark_converter = SparkSQLConverter()
ddl = spark_converter.convert(spec, pretty=True)
print(ddl)
```

## Contributing

We welcome contributions to `yads`!

### How to Add a New Officially Supported SQL Converter

If you want to add official support for a new SQL dialect (e.g., "Snowflake"), you can do so by creating a new converter that inherits from `SqlConverter`. This approach ensures that we can provide dialect-specific validation.

`SqlConverter` uses the **Template Method** design pattern. Its `convert` method acts as the template, defining the high-level steps for conversion, including a `_validate` hook that does nothing by default. To add a new supported dialect, you only need to subclass `SqlConverter` and override the `_validate` hook with your dialect-specific logic.

1.  **Create a New Converter Class in `sql.py`:** Add your new class to `src/yads/converters/sql.py`. It should inherit from `SqlConverter`. In its `__init__`, call `super()` and pass the correct dialect name.
2.  **Implement the `_validate` Method:** Override the `_validate` method. This is where you will add your validation logic to inspect the `SchemaSpec` and raise a `ValueError` for any unsupported features.
3.  **Expose the New Converter:** Add your new converter to the `__all__` list in `src/yads/converters/__init__.py`.

Here is an example for a hypothetical `SnowflakeConverter`:

```python
# In src/yads/converters/sql.py

class SnowflakeConverter(SqlConverter):
    """Converter for generating Snowflake SQL DDL."""

    def __init__(self, **convert_options: Any):
        super().__init__(dialect="snowflake", **convert_options)

    def _validate(self, spec: SchemaSpec) -> None:
        """Validates the SchemaSpec against Snowflake SQL compatibility."""
        # Add Snowflake-specific validation logic here.
        # For example, check for unsupported data types.
        for column in spec.columns:
            if isinstance(column.type, SomeUnsupportedType):
                raise ValueError("Snowflake does not support SomeUnsupportedType.")
```

### How to Create a New Core Converter

To create a new converter from a `SchemaSpec` to another format (e.g., a PyArrow or PySpark schema), you can follow these steps.

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
    A converter from a yads SchemaSpec to MyFormat.
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

To maintain an extensible design, use a dispatcher pattern to handle different `yads` types. In your converter's `__init__`, create a dictionary that maps `yads` type classes to specific handler methods.

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
        target_fields = [self._convert_field(field) for field in spec.columns]
        return MyTargetSchema(fields=target_fields)

    def _convert_field(self, field: Field) -> "MyTargetField":
        """Converts a yads Field to a target field format."""
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
