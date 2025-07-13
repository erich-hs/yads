# yads

`yads`: _~~Yet Another Data Spec~~_ **YAML-Augmented Data Specification** is a Python library for managing data specs using YAML. It helps you define and manage your data warehouse tables, schemas, and documentation in a structured, version-controlled way. With `yads`, you can define your data assets once in YAML and then generate various outputs like DDL statements for different databases, data schemas for tools like Avro or PyArrow, and human-readable, LLM-ready documentation.

## Why yads?

The modern data stack is complex, with data assets defined across a multitude of platforms and tools. This often leads to fragmented and inconsistent documentation, making data discovery and governance a challenge. `yads` was created to address this by providing a centralized, version-controllable, and extensible way to manage metadata for modern data platforms.

The main goal of `yads` is to provide a single source of truth for your data assets using simple YAML files. These files can capture everything from table schemas and column descriptions to governance policies and usage notes. From these specifications, `yads` can transpile the information into various formats, such as DDL statements for different SQL dialects, Avro or PyArrow schemas, and generate documentation that is ready for both humans and Large Language Models (LLMs).

## Getting Started

## Installation

```bash
pip install yads
```

To include support for PySpark DataFrame schema generation, install the `pyspark` additional dependency with:

```bash
pip install 'yads[pyspark]'
```

## Usage

### Defining a Specification

Create a YAML file to define your table schema and properties. For example, `users.yaml`:

```yaml
# specs/dim_user.yaml

table_name: "dim_user"
database: "dm_product_performance"
database_schema: "curated"
description: "Dimension table for users."
dimensional_table_type: "dimension"
owner: "data_engineering"
version: "1.0.0"
scd_type: 2

location: "s3://lakehouse/dm_product_performance/curated/dim_user"
partitioning:
  - column: "created_date"
    strategy: "month"

properties:
  table_type: "ICEBERG"
  format: "parquet"
  write_compression: "snappy"

table_schema:
  - name: "id"
    type: "integer"
    description: "Unique identifier for the user"
    constraints:
      - not_null: true
  - name: "username"
    type: "string"
    description: "Username for the user"
    constraints:
      - not_null: true
  - name: "email"
    type: "string"
    description: "Email address for the user"
    constraints:
      - not_null: true
  - name: "preferences"
    type: "map"
    key_type: "string"
    value_type: "string"
  - name: "created_at"
    type: "timestamp"
    description: "Timestamp of user creation"
    constraints:
      - not_null: true
```

### Generating Spark DDL

You can generate a Spark DDL `CREATE TABLE` statement from the specification:

```python
from yads import TableSpecification

# Load the specification
spec = TableSpecification("specs/dim_user.yaml")

# Generate the DDL
ddl = spec.to_ddl(dialect="spark")

print(ddl)
```


```stdout
CREATE OR REPLACE TABLE dm_product_performance.curated.dim_user (
  `id` INTEGER NOT NULL,
  `username` STRING NOT NULL,
  `email` STRING NOT NULL,
  `preferences` MAP<STRING, STRING>,
  `created_at` TIMESTAMP NOT NULL
)
USING ICEBERG
PARTITIONED BY (month(`created_date`))
LOCATION 's3://lakehouse/dm_product_performance/curated/dim_user'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write_compression' = 'snappy'
);
>>>
```

### Generating a PySpark DataFrame Schema

You can generate a `pyspark.sql.types.StructType` schema for a PySpark DataFrame:

```python
from yads import TableSpecification
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load the specification
spec = TableSpecification("specs/dim_user.yaml")

# Generate the PySpark schema
spark_schema = spec.to_spark_schema()

df = spark.createDataFrame([], schema=spark_schema)
df.printSchema()
```

```stdout
root
 |-- id: integer (nullable = false)
 |-- username: string (nullable = false)
 |-- email: string (nullable = false)
 |-- preferences: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- created_at: timestamp (nullable = false)
>>>
```

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
