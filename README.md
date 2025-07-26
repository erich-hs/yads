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
from yads.converters.sql import SparkSQLConverter

# Load the specification from a YAML file
spec = yads.from_yaml("examples/specs/yads_spec.yaml")

# The SparkSQLConverter will automatically handle the "spark" dialect
# and perform Spark-specific validation on your spec.
spark_converter = SparkSQLConverter()
ddl = spark_converter.convert(spec, pretty=True)

print(ddl)
```

This will produce the following SQL DDL:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS prod_db.fact_sales.customer_orders_pro (
  order_id STRING NOT NULL,
  customer_id INT NOT NULL,
  order_ts TIMESTAMP NOT NULL,
  total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0.0,
  order_status STRING DEFAULT 'pending',
  product_category STRING,
  shipping_details STRUCT<address: STRING NOT NULL, city: STRING, postal_code: STRING>,
  tags MAP<STRING, STRING>,
  CONSTRAINT pk_customer_orders_pro PRIMARY KEY (order_id)
)
USING ICEBERG
LOCATION '/warehouse/sales/customer_orders_pro'
PARTITIONED BY (
  MONTH(order_ts),
  order_status
)
TBLPROPERTIES (
  'write.target-file-size-bytes' = '536870912',
  'read.split.target-size' = '268435456'
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

## Extending yads

`yads` is designed to be easily extensible, allowing you to add support for new SQL dialects or other formats. This guide provides a brief overview of how to extend `yads` with custom converters and validation rules.

### Creating a Core Converter

Core converters are responsible for the primary transformation of a `yads` `SchemaSpec` into a different object representation, such as a PySpark `StructType` or a PyArrow `Schema`. These converters form the foundation of `yads`'s extensibility.

To create a new core converter, the following steps are required:

1.  **Subclass `BaseConverter`**: Create a new class that inherits from `yads.converters.base.BaseConverter`.
2.  **Implement the `convert` Method**: This method takes a `SchemaSpec` as input and should return the target object, such as a PySpark `StructType`.
3.  **Implement Type and Field Handlers**: Within the converter, create methods to handle the conversion of `yads` types (e.g., `String`, `Decimal`, `Struct`) into their counterparts in the target framework. This typically involves a dispatch mechanism that maps `yads` types to specific handling functions.

Here is a simplified example of a `PySparkConverter`:

```python
# in yads/converters/spark.py
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)

from yads.converters.base import BaseConverter
from yads.spec import SchemaSpec, Field
from yads.types import String, Decimal, Type


class PySparkConverter(BaseConverter):
    def __init__(self):
        self._type_handlers = {
            String: self._handle_string_type,
            Decimal: self._handle_decimal_type,
        }

    def convert(self, spec: SchemaSpec) -> StructType:
        return StructType([self._convert_field(field) for field in spec.columns])

    def _convert_field(self, field: Field) -> StructField:
        spark_type = self._convert_type(field.type)
        return StructField(
            name=field.name,
            dataType=spark_type,
            nullable=not field.is_not_nullable,
            metadata={"description": field.description},
        )

    def _convert_type(self, yads_type: Type):
        if handler := self._type_handlers.get(type(yads_type)):
            return handler(yads_type)
        # Fallback or error for unsupported types
        return StringType()

    def _handle_string_type(self, yads_type: String) -> StringType:
        return StringType()

    def _handle_decimal_type(self, yads_type: Decimal) -> DecimalType:
        return DecimalType(precision=yads_type.precision, scale=yads_type.scale)
```

### Creating a SQL Converter

To add support for a new SQL dialect, you'll typically need to follow a structured conversion and validation flow.

#### The Conversion and Validation Flow

The process of converting a `yads` specification into a target format (like a SQL DDL) follows these general steps:

1.  **Parsing**: The YAML/JSON spec is parsed into a `SchemaSpec` object.
2.  **Core Conversion**: A core converter (e.g., `SqlglotConverter`) transforms the `SchemaSpec` into a generic intermediate representation, like a `sqlglot` Abstract Syntax Tree (AST).
3.  **Validation and Adjustment (Optional)**: For formats with specific constraints (like SQL dialects), an `AstValidator` can be used to process the intermediate representation. It applies a set of rules to check for unsupported features and adjusts the AST accordingly.
4.  **Serialization**: The final representation is serialized into the target output format (e.g., a SQL string).

> [!NOTE]
> The SQL DDL conversion pipeline follows a **"wide-to-narrow"** approach, designed to keep data definitions flexible, yet with minimal loss of expressiveness:
> -   **`yads` Spec (Most Expressive)**: The spec is the single source of truth. It's designed to capture the data model in detail, independent of any specific dialect or format's limitations. It is always treated as immutable.
> -   **`sqlglot` AST (Generic & Expressive)**: From the spec, a generic `sqlglot` AST is built. This tree is also highly expressive and serves as a common, dialect-agnostic representation of the table schema.
> -   **SQL Converter (Specific & Strict)**: The final converter "narrows" the generic AST into valid DDL for a specific SQL dialect. The `AstValidator` ensures that any feature from the spec is gracefully handled—either by raising an error (in `strict` mode) or by adjusting the AST to fit the dialect's constraints.
> This flow ensures that the `yads` spec remains the ultimate source of truth, while still allowing for the practical generation of correct, dialect-specific SQL.

#### Steps to Create a SQL Converter

To add support for a new SQL dialect, you'll typically need to:

1.  **Create a Converter Class**: Subclass `SqlConverter` (e.g., `MyNewSqlConverter`).
2.  **Implement Validation Rules**: If the dialect has features that are not universally supported, create new validation rules by subclassing `yads.validator.Rule`. Each rule needs to implement:
    *   `validate()`: To check the AST for a specific issue.
    *   `adjust()`: To modify the AST to resolve the issue.
3.  **Wire it Up**: In your new converter's `__init__`, instantiate `AstValidator` with your list of rules and pass it to the `SqlConverter`'s constructor.

Here's a simplified example of a custom converter for a fictional "AwesomeDB":

```python
# in yads/converters/sql.py
from yads.converters.sql import SqlConverter
from yads.validator import AstValidator, Rule

class DisallowLongVarcharsRule(Rule):
    # ... implementation for the rule ...

class AwesomeDBConverter(SqlConverter):
    def __init__(self, **convert_options: Any):
        rules = [DisallowLongVarcharsRule()]
        validator = AstValidator(rules=rules)
        super().__init__(dialect="awesomedb", ast_validator=validator, **convert_options)
```

By following this pattern, you can keep dialect-specific logic cleanly separated, making the codebase easier to maintain and extend.


## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License.
