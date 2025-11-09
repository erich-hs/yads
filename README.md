# yads

`yads` is a canonical, typed data specification for the modern multi-disciplinary data team. Define a schema once; load and convert it deterministically across formats with minimal loss of semantics.

## Installation
```bash
# With pip
pip install yads
```

```bash
# With uv
uv add yads
```

## Overview

As the universal format for columnar data representation, `Arrow` is central to `yads`, but the specification is expressive enough to be derivable from the most common data formats used by data teams.

| Format | Loader | Converter |
| --------- | ---------- | ------------- |
| PyArrow | `yads.from_pyarrow` | `yads.to_pyarrow` |
| PySpark | `yads.from_pyspark` | `yads.to_pyspark` |
| Polars | `yads.from_polars` | `yads.to_polars` |
| Pydantic | _Not implemented_ | `yads.to_pydantic` |
| SQL | _Not implemented_ | `yads.to_sql` |
| YAML | `yads.from_yaml` | _Not implemented_ |

See the [loaders](./src/yads/loaders/) and [converters](./src/yads/converters/) API for advanced usage. A list of supported SQL dialects is available [here](./src/yads/converters/sql/sql_converter.py).

### `yads` specification

Typical workflows start with an expressive yads specification that can then be used throughout the data lifecycle.

The latest `yads` specification JSON schema is available [here](./spec/yads_spec_latest.json).

```yaml
# registry/specs/customers.yaml
name: catalog.crm.customers
version: 1.0.0
columns:
  - name: id
    type: bigint
    constraints:
      not_null: true
  - name: email
    type: string
  - name: created_at
    type: timestamptz
  - name: spend
    type: decimal
    params:
      precision: 10
      scale: 2
  - name: tags
    type: array
    element:
      type: string
```

Load a yads spec (from a YAML string, file-like object, or path)
```python
import yads

spec = yads.from_yaml("registry/specs/customers.yaml")

# Generate a Pydantic BaseModel
Customers = yads.to_pydantic(spec, model_name="Customers")

print("MODEL:", Customers)
print("FIELDS:", list(Customers.model_fields.keys()))
```
```text
MODEL: <class 'yads.converters.pydantic_converter.Customers'>
FIELDS: ['id', 'email', 'created_at', 'spend', 'tags']
```

Validate an incoming record with Pydantic
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
```stdout
{'id': 123, 'email': 'alice@example.com', 'created_at': datetime.datetime(2024, 5, 1, 12, 0, tzinfo=datetime.timezone.utc), 'spend': Decimal('42.50'), 'tags': ['vip', 'beta']}
```

Emit DDL for multiple SQL dialects from the same spec
```python
spark_sql = yads.to_sql(spec, dialect="spark", pretty=True)
duckdb_sql = yads.to_sql(spec, dialect="duckdb", pretty=True)

print("-- Spark DDL --\\n" + spark_sql)
print("\\n-- DuckDB DDL --\\n" + duckdb_sql)
```
```sql
-- Spark DDL --
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  email STRING,
  created_at TIMESTAMP,
  spend DECIMAL(10, 2),
  tags ARRAY<STRING>
)

-- DuckDB DDL --
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  email TEXT,
  created_at TIMESTAMPTZ,
  spend DECIMAL(10, 2),
  tags TEXT[]
)
```

Create a Polars schema for typed DataFrame IO
```python
import yads
pl_schema = yads.to_polars(spec)
print(pl_schema)
```
```text
Schema({'id': Int64, 'email': String, 'created_at': Datetime(time_unit='ns', time_zone='UTC'), 'spend': Decimal(precision=10, scale=2), 'tags': List(String)})
```
Create a PyArrow schema with constraint preservation
```python
import yads
pa_schema = yads.to_pyarrow(spec)
print(pa_schema)
```
```text
id: int64 not null
email: string
created_at: timestamp[ns, tz=UTC]
spend: decimal128(10, 2)
tags: list<item: string>
  child 0, item: string
```

### Configurable conversions

The canonical yads spec is immutable, but conversions can be customized with configuration options.
```python
import yads

spec = yads.from_yaml("registry/specs/customers.yaml")
ddl_min = yads.to_sql(spec, dialect="spark", include_columns={"id", "email"}, pretty=True)

print(ddl_min)
```
```sql
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  email STRING
)
```

Column overrides can be used to apply custom validation to specific columns, or to supersede default conversions.
```python
from pydantic import Field

def email_override(field, conv):
    # Enforce example.com domain with a regex pattern
    return str, Field(pattern=r"^.+@example\\.com$")

Model = yads.to_pydantic(spec, column_overrides={"email": email_override})

try:
    Model(id=1, email="user@other.com")
except Exception as e:
    print(type(e).__name__ + ":\n" + str(e))
```
```text
ValidationError:
1 validation error for catalog_crm_customers
email
  String should match pattern '^.+@example\\.com$' [type=string_pattern_mismatch, input_value='user@other.com', input_type=str]
```

### Round-trip conversions

`yads` attempts to preserve the complete representation of data schemas across conversions. The following example demonstrates a round-trip from a PyArrow schema to a yads spec, then to a DuckDB DDL and PySpark schema, while preserving metadata and column constraints.

```python
import yads
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False, metadata={"description": "Customer ID"}),
    pa.field("name", pa.string(), metadata={"description": "Customer preferred name"}),
    pa.field("email", pa.string(), metadata={"description": "Customer email address"}),
    pa.field("created_at", pa.timestamp('ns', tz='UTC'), metadata={"description": "Record creation timestamp"}),
])

spec = yads.from_pyarrow(schema, name="catalog.crm.customers", version="1.0.0")
print(spec)
```
```text
spec catalog.crm.customers(version='1.0.0')(
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

Nullability and metadata are preserved as long as the target format supports them.

```python
print(yads.to_sql(spec, dialect="duckdb", pretty=True))
```
```text
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  name TEXT,
  email TEXT,
  created_at TIMESTAMPTZ
)
```
```python
pyspark_schema = yads.to_pyspark(spec)
for field in pyspark_schema.fields:
    print(f"{field.name}, {field.dataType}, {field.nullable=}")
    print(f"{field.metadata=}\n")
```
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

## Design Philosophy

`yads` is spec-first, deterministic, and safe-by-default: given the same spec and backend, converters and loaders produce the same schema and the same validation diagnostics.

Conversions proceed silently only when they are lossless and fully semantics-preserving. When a backend cannot represent type parameters but preserves semantics (constraint loss, e.g. `String(length=10)` → `String()`), `yads` converts and emits structured warnings per affected field.

Backend type gaps are handled with value-preserving substitutes only; otherwise conversion requires an explicit `fallback_type`. Potentially lossy or reinterpreting changes (range narrowing, precision downgrades, sign changes, or unit changes) are never applied implicitly. Types with no value-preserving representation fail fast with clear errors and extension guidance.

Single rule: preserve semantics or notify; never lose or reinterpret data without explicit opt-in.
