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
table_name: users
database: raw
description: User information table
layer: raw
dimensional_table_type: raw
owner: data_engineering
version: 1.0.0
retention_days: null

schema:
  - name: id
    type: integer
    description: Unique identifier for the user
    constraints:
      - unique: true
      - not_null: true
  - name: username
    type: string
    description: Username for the user
    constraints:
      - unique: true
      - not_null: true
  - name: email
    type: string
    description: Email address for the user
    constraints:
      - unique: true
      - not_null: true
  - name: created_at
    type: timestamp
    description: Timestamp of user creation
    constraints:
      - not_null: true
  - name: updated_at
    type: timestamp
    description: Timestamp of last user update
    constraints:
      - not_null: true
```

### Generating Spark DDL

You can generate a Spark DDL `CREATE TABLE` statement from the specification:

```python
from yads import TableSpecification

# Load the specification
spec = TableSpecification.from_yaml("users.yaml")

# Generate the DDL
ddl = spec.to_ddl()

print(ddl)
```

### Generating a PySpark DataFrame Schema

You can generate a `pyspark.sql.types.StructType` schema for a PySpark DataFrame:

```python
from yads import TableSpecification
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load the specification
spec = TableSpecification.from_yaml("users.yaml")

# Generate the PySpark schema
spark_schema = spec.to_spark_schema()

df = spark.createDataFrame([], schema=spark_schema)
df.printSchema()
```

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
