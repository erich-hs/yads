![yads](./docs/assets/yads_banner_120x120.png)

<p align="center">
  <a href="https://texel.sh">
    <img src="https://img.shields.io/badge/made_by-texel-2f6bff.svg?style=flat&amp;logo=data%3Aimage%2Fsvg%2Bxml%3Bbase64%2CPHN2ZyBpZD0iZXBDVWFUUWdOY3cxIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB2aWV3Qm94PSIwIDAgMjQgMjQiIHNoYXBlLXJlbmRlcmluZz0iZ2VvbWV0cmljUHJlY2lzaW9uIiB0ZXh0LXJlbmRlcmluZz0iZ2VvbWV0cmljUHJlY2lzaW9uIiBwcm9qZWN0LWlkPSJhZDllMzEyOGRmZTY0M2FiYjMzZGM2NDBjYjk3NmVhOSIgZXhwb3J0LWlkPSJiNmViYTNiM2RhN2U0NWFmODdiY2U3Yzg4OGYwM2ZiMCIgY2FjaGVkPSJmYWxzZSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjI0Ij48cGF0aCBkPSJNMjIxLjgyNTg4LDUzLjc2NTA5bC0uMjM5NDgsMTQ5Ljc2Nzc1TDc4LjE3NDEzLDI0Ni42OTk0NUw3OC40MTM2MSw5Ni45MzE3TDIyMS44MjU4OCw1My43NjUwOVpNMTM1LjkyMjIsMTM5LjU3MjE1bC0zMy41NjY2NCw3NC45NzE1N2w2Mi4yMDEyLTUzLjY1MTM0TDE5OC4xMjM0LDg1LjkyMDgxbC02Mi4yMDEyLDUzLjY1MTM0WiIgdHJhbnNmb3JtPSJtYXRyaXgoMC4xMTY5MyAwIDAgMC4xMTY5MyAtNS41Mzk1MDEgLTUuNTY2NjU5KSIgZmlsbD0iIzJmNmJmZiIvPjwvc3ZnPg0K" /></a>
  <a href="https://docs.texel.sh/yads/intro/quick_start/">
    <img src="https://img.shields.io/badge/docs-quickstart-f6f7fb" /></a>
  <a href="https://codecov.io/github/texel-sh/yads"> 
    <img src="https://codecov.io/github/texel-sh/yads/graph/badge.svg?token=GWO1S0YAZ3" /></a>
  <img src="https://github.com/texel-sh/yads/actions/workflows/ci.yml/badge.svg" alt="CI">
  <img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python 3.10+">
</p>


`yads` is an expressive, canonical [data specification](https://docs.texel.sh/yads/intro/specification/) to solve schema management throughout your data stack. Proudly [open source](https://github.com/texel-sh/yads/blob/main/LICENSE) and built in the open with and for the data community.

Check our [documentation](https://docs.texel.sh/yads/) to know more, and the [quick start guide](https://docs.texel.sh/yads/intro/quick_start/) to get started.

## Installation

```bash
# With pip
pip install yads
```

```bash
# With uv
uv add yads
```

`yads` is a lightweight dependency designed to run in your existing Python workflows. Each loader and converter is designed to support a wide range of versions for your source or target format.

You can install `yads` Python API alongside the required optional dependency for your use case.
```bash
uv add yads[pyarrow]
```

Or simply add `yads` to your project that is already using the optional dependency within the supported version range. See the supported versions [here](pyproject.toml).

## Overview

As the universal format for columnar data representation, `Arrow` is central to `yads`, but the specification is expressive enough to be derivable from the most common data formats used by data teams.

| Format | Loader | Converter | Installation |
| --------- | ---------- | ------------- | ------------- |
| PyArrow | `yads.from_pyarrow` | `yads.to_pyarrow` | `pip install yads[pyarrow]` |
| PySpark | `yads.from_pyspark` | `yads.to_pyspark` | `pip install yads[pyspark]` |
| Polars | `yads.from_polars` | `yads.to_polars` | `pip install yads[polars]` |
| Pydantic | _Not implemented_ | `yads.to_pydantic` | `pip install yads[pydantic]` |
| SQL | _Not implemented_ | `yads.to_sql` | `pip install yads[sql]` |
| YAML | `yads.from_yaml` | _Not implemented_ | `pip install yads` |

See the [loaders](./src/yads/loaders/) and [converters](./src/yads/converters/) API for advanced usage. A list of supported SQL dialects is available [here](./src/yads/converters/sql/sql_converter.py).

### `yads` specification

Typical workflows start with an expressive yads specification that can then be used throughout the data lifecycle.

The latest `yads` specification JSON schema is available [here](./spec/yads_spec_latest.json).

<!-- BEGIN:example minimal-yaml-to-others spec-yaml -->
```yaml
# docs/src/specs/customers.yaml
name: "catalog.crm.customers"
version: 1
yads_spec_version: "0.0.2"

columns:
  - name: "id"
    type: "bigint"
    constraints:
      not_null: true

  - name: "email"
    type: "string"

  - name: "created_at"
    type: "timestamptz"

  - name: "spend"
    type: "decimal"
    params:
      precision: 10
      scale: 2

  - name: "tags"
    type: "array"
    element:
      type: "string"
```
<!-- END:example minimal-yaml-to-others spec-yaml -->

Load a yads spec and generate a Pydantic `BaseModel`
<!-- BEGIN:example minimal-yaml-to-others load-spec-code -->
```python
import yads

spec = yads.from_yaml("docs/src/specs/customers.yaml")

# Generate a Pydantic BaseModel
Customers = yads.to_pydantic(spec, model_name="Customers")

print(Customers)
print(list(Customers.model_fields.keys()))
```
<!-- END:example minimal-yaml-to-others load-spec-code -->
<!-- BEGIN:example minimal-yaml-to-others load-spec-output -->
```text
<class 'yads.converters.pydantic_converter.Customers'>
['id', 'email', 'created_at', 'spend', 'tags']
```
<!-- END:example minimal-yaml-to-others load-spec-output -->

To validate and serialize data
<!-- BEGIN:example minimal-yaml-to-others pydantic-model-code -->
```python
from datetime import datetime, timezone

record = Customers(
    id=123,
    email="alice@example.com",
    created_at=datetime(2024, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
    spend="42.50",
    tags=["vip", "beta"],
)

print(record.model_dump())
```
<!-- END:example minimal-yaml-to-others pydantic-model-code -->
<!-- BEGIN:example minimal-yaml-to-others pydantic-model-output -->
```text
{'id': 123, 'email': 'alice@example.com', 'created_at': datetime.datetime(2024, 5, 1, 12, 0, tzinfo=datetime.timezone.utc), 'spend': Decimal('42.50'), 'tags': ['vip', 'beta']}
```
<!-- END:example minimal-yaml-to-others pydantic-model-output -->

Emit DDL for multiple SQL dialects from the same spec
<!-- BEGIN:example minimal-yaml-to-others spark-sql-code -->
```python
spark_ddl = yads.to_sql(spec, dialect="spark", pretty=True)
print(spark_ddl)
```
<!-- END:example minimal-yaml-to-others spark-sql-code -->
<!-- BEGIN:example minimal-yaml-to-others spark-sql-output -->
```sql
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  email STRING,
  created_at TIMESTAMP,
  spend DECIMAL(10, 2),
  tags ARRAY<STRING>
)
```
<!-- END:example minimal-yaml-to-others spark-sql-output -->
<!-- BEGIN:example minimal-yaml-to-others duckdb-sql-code -->
```python
duckdb_ddl = yads.to_sql(spec, dialect="duckdb", pretty=True)
print(duckdb_ddl)
```
<!-- END:example minimal-yaml-to-others duckdb-sql-code -->
<!-- BEGIN:example minimal-yaml-to-others duckdb-sql-output -->
```sql
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  email TEXT,
  created_at TIMESTAMPTZ,
  spend DECIMAL(10, 2),
  tags TEXT[]
)
```
<!-- END:example minimal-yaml-to-others duckdb-sql-output -->

Create a Polars DataFrame schema
<!-- BEGIN:example minimal-yaml-to-others polars-code -->
```python
import yads

pl_schema = yads.to_polars(spec)
print(pl_schema)
```
<!-- END:example minimal-yaml-to-others polars-code -->
<!-- BEGIN:example minimal-yaml-to-others polars-output -->
```text
Schema({'id': Int64, 'email': String, 'created_at': Datetime(time_unit='ns', time_zone='UTC'), 'spend': Decimal(precision=10, scale=2), 'tags': List(String)})
```
<!-- END:example minimal-yaml-to-others polars-output -->
Create a PyArrow schema with constraint preservation
<!-- BEGIN:example minimal-yaml-to-others pyarrow-code -->
```python
import yads

pa_schema = yads.to_pyarrow(spec)
print(pa_schema)
```
<!-- END:example minimal-yaml-to-others pyarrow-code -->
<!-- BEGIN:example minimal-yaml-to-others pyarrow-output -->
```text
id: int64 not null
email: string
created_at: timestamp[ns, tz=UTC]
spend: decimal128(10, 2)
tags: list<item: string>
  child 0, item: string
```
<!-- END:example minimal-yaml-to-others pyarrow-output -->

### Configurable conversions

The canonical yads spec is immutable, but conversions can be customized with configuration options.
<!-- BEGIN:example minimal-configurable-conversion spark-config-code -->
```python
import yads

spec = yads.from_yaml("docs/src/specs/customers.yaml")
ddl_min = yads.to_sql(
    spec,
    dialect="spark",
    include_columns={"id", "email"},
    pretty=True,
)

print(ddl_min)
```
<!-- END:example minimal-configurable-conversion spark-config-code -->
<!-- BEGIN:example minimal-configurable-conversion spark-config-output -->
```sql
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  email STRING
)
```
<!-- END:example minimal-configurable-conversion spark-config-output -->

Column overrides can be used to apply custom validation to specific columns, or to supersede default conversions.
<!-- BEGIN:example minimal-configurable-conversion column-override-code -->
```python
from pydantic import Field

def email_override(field, conv):
    # Enforce example.com domain with a regex pattern
    return str, Field(pattern=r"^.+@example\.com$")

Model = yads.to_pydantic(spec, column_overrides={"email": email_override})

try:
    Model(
        id=1,
        email="user@other.com",
        created_at="2024-01-01T00:00:00+00:00",
        spend="42.50",
        tags=["beta"],
    )
except Exception as e:
    print(type(e).__name__ + ":\n" + str(e))
```
<!-- END:example minimal-configurable-conversion column-override-code -->
<!-- BEGIN:example minimal-configurable-conversion column-override-output -->
```text
ValidationError:
1 validation error for catalog_crm_customers
email
  String should match pattern '^.+@example\.com$' [type=string_pattern_mismatch, input_value='user@other.com', input_type=str]
    For further information visit https://errors.pydantic.dev/2.12/v/string_pattern_mismatch
```
<!-- END:example minimal-configurable-conversion column-override-output -->

### Round-trip conversions

`yads` attempts to preserve the complete representation of data schemas across conversions. The following example demonstrates a round-trip from a PyArrow schema to a yads spec, then to a DuckDB DDL and PySpark schema, while preserving metadata and column constraints.

<!-- BEGIN:example minimal-roundtrip-conversion pyarrow-code -->
```python
import yads
import pyarrow as pa

schema = pa.schema(
    [
        pa.field(
            "id",
            pa.int64(),
            nullable=False,
            metadata={"description": "Customer ID"},
        ),
        pa.field(
            "name",
            pa.string(),
            metadata={"description": "Customer preferred name"},
        ),
        pa.field(
            "email",
            pa.string(),
            metadata={"description": "Customer email address"},
        ),
        pa.field(
            "created_at",
            pa.timestamp("ns", tz="UTC"),
            metadata={"description": "Customer creation timestamp"},
        ),
    ]
)

spec = yads.from_pyarrow(schema, name="catalog.crm.customers", version=1)
print(spec)
```
<!-- END:example minimal-roundtrip-conversion pyarrow-code -->
<!-- BEGIN:example minimal-roundtrip-conversion pyarrow-output -->
```text
spec catalog.crm.customers(version=1)(
  columns=[
    id: integer(bits=64)(
      description='Customer ID',
      constraints=[NotNullConstraint()]
    )
    name: string(
      description='Customer preferred name'
    )
    email: string(
      description='Customer email address'
    )
    created_at: timestamptz(unit=ns, tz=UTC)(
      description='Customer creation timestamp'
    )
  ]
)
```
<!-- END:example minimal-roundtrip-conversion pyarrow-output -->

Nullability and metadata are preserved as long as the target format supports them.

<!-- BEGIN:example minimal-roundtrip-conversion duckdb-code -->
```python
duckdb_ddl = yads.to_sql(spec, dialect="duckdb", pretty=True)
print(duckdb_ddl)
```
<!-- END:example minimal-roundtrip-conversion duckdb-code -->
<!-- BEGIN:example minimal-roundtrip-conversion duckdb-output -->
```sql
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  name TEXT,
  email TEXT,
  created_at TIMESTAMPTZ
)
```
<!-- END:example minimal-roundtrip-conversion duckdb-output -->
<!-- BEGIN:example minimal-roundtrip-conversion pyspark-code -->
```python
pyspark_schema = yads.to_pyspark(spec)
for field in pyspark_schema.fields:
    print(f"{field.name}, {field.dataType}, {field.nullable=}")
    print(f"{field.metadata=}\n")
```
<!-- END:example minimal-roundtrip-conversion pyspark-code -->
<!-- BEGIN:example minimal-roundtrip-conversion pyspark-output -->
```text
id, LongType(), field.nullable=False
field.metadata={'description': 'Customer ID'}

name, StringType(), field.nullable=True
field.metadata={'description': 'Customer preferred name'}

email, StringType(), field.nullable=True
field.metadata={'description': 'Customer email address'}

created_at, TimestampType(), field.nullable=True
field.metadata={'description': 'Customer creation timestamp'}
```
<!-- END:example minimal-roundtrip-conversion pyspark-output -->

## Design Philosophy

`yads` is spec-first, deterministic, and safe-by-default: given the same spec and backend, converters and loaders produce the same schema and the same validation diagnostics.

Conversions proceed silently only when they are lossless and fully semantics-preserving. When a backend cannot represent type parameters but preserves semantics (constraint loss, e.g. `String(length=10)` → `String()`), `yads` converts and emits structured warnings per affected field.

Backend type gaps are handled with value-preserving substitutes only; otherwise conversion requires an explicit `fallback_type`. Potentially lossy or reinterpreting changes (range narrowing, precision downgrades, sign changes, or unit changes) are never applied implicitly. Types with no value-preserving representation fail fast with clear errors and extension guidance.

Single rule: preserve semantics or notify; never lose or reinterpret data without explicit opt-in.
