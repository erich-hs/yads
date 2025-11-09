# `yads`

A canonical, typed data specification for data teams. Define once; load/convert
deterministically across formats and backends with explicit,
semantics-preserving rules.

## Installation
```bash
# With pip
pip install yads
```

```bash
# With uv
uv add yads
```

## Features

| Format | Loader | Converter |
| --------- | ---------- | ------------- |
| PyArrow | `yads.from_pyarrow` | `yads.to_pyarrow` |
| PySpark | `yads.from_pyspark` | `yads.to_pyspark` |
| Polars | `yads.from_polars` | `yads.to_polars` |
| Pydantic | _Not implemented_ | `yads.to_pydantic` |
| SQL | _Not implemented_ | `yads.to_sql` |
| YAML | `yads.from_yaml` | _Not implemented_ |

See the [loaders](./src/yads/loaders/) and [converters](./src/yads/converters/) API for advanced usage. A list of supported SQL dialects is available [here](./src/yads/converters/sql/sql_converter.py).

### End-to-end: canonical spec → model and schemas

```python
# Load a spec from YAML (string shown; file-like objects and paths also work)
from io import StringIO
import yads

yaml_spec = """
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
"""

spec = yads.from_yaml(StringIO(yaml_spec))
```

```python
# Generate a Pydantic BaseModel and validate an incoming record
from datetime import datetime, timezone
import yads

Customers = yads.to_pydantic(spec, model_name="Customers")
print("MODEL:", Customers)
print("FIELDS:", list(Customers.model_fields.keys()))
record = Customers(
    id=123,
    email="alice@example.com",
    created_at=datetime(2024, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
    spend="42.50",
    tags=["vip", "beta"],
)
print("DUMP:", record.model_dump())
```

Output:

```python
MODEL: <class 'yads.converters.pydantic_converter.Customers'>
FIELDS: ['id', 'email', 'created_at', 'spend', 'tags']
DUMP: {'id': 123, 'email': 'alice@example.com', 'created_at': datetime.datetime(2024, 5, 1, 12, 0, tzinfo=datetime.timezone.utc), 'spend': Decimal('42.50'), 'tags': ['vip', 'beta']}
```

```python
# Emit DDL for multiple engines from the same spec
import yads

spark_sql = yads.to_sql(spec, dialect="spark", pretty=True)
duckdb_sql = yads.to_sql(spec, dialect="duckdb", pretty=True)
print("-- Spark DDL --\\n" + spark_sql)
print("\\n-- DuckDB DDL --\\n" + duckdb_sql)
```

Output:

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

```python
# Create a Polars schema for typed DataFrame IO
import yads
pl_schema = yads.to_polars(spec)
print(pl_schema)
```

Output:

```text
Schema({'id': Int64, 'email': String, 'created_at': Datetime(time_unit='ns', time_zone='UTC'), 'spend': Decimal(precision=10, scale=2), 'tags': List(String)})
```

```python
# Create a PyArrow schema for Parquet/Arrow IO
import yads
pa_schema = yads.to_pyarrow(spec)
print(pa_schema)
```

Output:

```text
id: int64 not null
email: string
created_at: timestamp[ns, tz=UTC]
spend: decimal128(10, 2)
tags: list<item: string>
  child 0, item: string
```

### Filter columns for a derived artifact

```python
import yads
ddl_min = yads.to_sql(spec, dialect="spark", include_columns={"id", "email"}, pretty=True)
print(ddl_min)
```

Output:

```sql
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  email STRING
)
```

### Add per-column validation (Pydantic overrides)

```python
from pydantic import Field
import yads

def email_override(field, conv):
    # Enforce example.com domain with a regex pattern
    return str, Field(pattern=r"^.+@example\\.com$")

Model = yads.to_pydantic(spec, column_overrides={"email": email_override})
try:
    Model(id=1, email="user@other.com")
except Exception as e:
    print(type(e).__name__ + ":\n" + str(e))
```

Output:

```text
ValidationError:
1 validation error for catalog_crm_customers
email
  String should match pattern '^.+@example\\.com$' [type=string_pattern_mismatch, input_value='user@other.com', input_type=str]
```

### Round-trip: PyArrow schema → Spec → Spark/DuckDB DDL/Pydantic

```python
import yads
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False, metadata={"description": "Customer ID"}),
    pa.field("name", pa.string(), metadata={"description": "Customer preferred name"}),
    pa.field("email", pa.string(), metadata={"description": "Customer email address"}),
    pa.field("created_at", pa.timestamp('ns', tz='UTC'), metadata={"description": "Customer creation timestamp"}),
])

spec = yads.from_pyarrow(schema, name="catalog.crm.customers", version="1.0.0")
print("-- yads Spec --")
print(spec)

print("-- DuckDB DDL --")
print(yads.to_sql(spec, dialect="duckdb", pretty=True))

print("-- PySpark Schema --")
pyspark_schema = yads.to_pyspark(spec)
for field in pyspark_schema.fields:
    print(f"{field.name}, {field.dataType}, {field.nullable=}")
    print(f"{field.metadata=}\n")
```

Output:

```text
SPEC: spec catalog.crm.customers(version='1.0.0')(
  columns=[
    id: integer(bits=64)(
      constraints=[NotNullConstraint()]
    )
    email: string
    created_at: timestamptz(unit=ns, tz=UTC)
    spend: decimal(precision=10, scale=2, bits=128)
    tags: array<string>
  ]
)
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
MODEL: <class 'yads.converters.pydantic_converter.CustomersFromArrow'>
FIELDS: ['id', 'email', 'created_at', 'spend', 'tags']
```

### Constraints and metadata preservation across conversions

```python
from io import StringIO
import yads

yaml_spec = """
name: analytics.app.events
version: 1.2.3
metadata:
  owner: analytics
  domain: engagement
columns:
  - name: event_id
    type: uuid
    constraints:
      not_null: true
  - name: user_id
    type: bigint
    constraints:
      not_null: true
  - name: event_type
    type: string
    constraints:
      default: click
    description: Logical type of event
    metadata:
      pii: false
  - name: ts
    type: timestamptz
    constraints:
      not_null: true
  - name: props
    type: json
    metadata:
      comment: Unstructured event payload

table_constraints:
  - type: primary_key
    name: pk_events
    columns: [event_id]
  - type: foreign_key
    name: fk_users
    columns: [user_id]
    references:
      table: core.users
      columns: [id]
"""

spec = yads.from_yaml(StringIO(yaml_spec))
print("SPEC:")
print(spec)

print("-- Spark DDL --")
print(yads.to_sql(spec, dialect="spark", pretty=True))

print("-- DuckDB DDL --")
print(yads.to_sql(spec, dialect="duckdb", pretty=True))

Model = yads.to_pydantic(spec, model_name="EventRecord")
print("MODEL:", Model)
schema = Model.model_json_schema()
print("REQUIRED:", schema.get("required"))
print("FIELD:event_type:", schema.get("properties", {}).get("event_type"))
print("FIELD:user_id:", schema.get("properties", {}).get("user_id"))
```

Output:

```text
SPEC:
spec analytics.app.events(version='1.2.3')(
  metadata={
    owner='analytics',
    domain='engagement'
  }
  table_constraints=[
    PrimaryKeyTableConstraint(
      name='pk_events',
      columns=[
        'event_id'
      ]
    )
    ForeignKeyTableConstraint(
      name='fk_users',
      columns=[
        'user_id'
      ],
      references=core.users(id)
    )
  ]
  columns=[
    event_id: uuid(
      constraints=[NotNullConstraint()]
    )
    user_id: integer(bits=64)(
      constraints=[NotNullConstraint()]
    )
    event_type: string(
      description='Logical type of event',
      constraints=[DefaultConstraint(value='click')],
      metadata={pii=False}
    )
    ts: timestamptz(unit=ns, tz=UTC)(
      constraints=[NotNullConstraint()]
    )
    props: json(
      metadata={comment='Unstructured event payload'}
    )
  ]
)
-- Spark DDL --
CREATE TABLE analytics.app.events (
  event_id STRING NOT NULL,
  user_id BIGINT NOT NULL,
  event_type STRING DEFAULT 'click',
  ts TIMESTAMP NOT NULL,
  props STRING,
  CONSTRAINT pk_events PRIMARY KEY (event_id),
  CONSTRAINT fk_users FOREIGN KEY (user_id) REFERENCES core.users (
    id
  )
)
-- DuckDB DDL --
CREATE TABLE analytics.app.events (
  event_id UUID NOT NULL,
  user_id BIGINT NOT NULL,
  event_type TEXT DEFAULT 'click',
  ts TIMESTAMPTZ NOT NULL,
  props JSON,
  CONSTRAINT pk_events PRIMARY KEY (event_id),
  CONSTRAINT fk_users FOREIGN KEY (user_id) REFERENCES core.users (
    id
  )
)
MODEL: <class 'yads.converters.pydantic_converter.EventRecord'>
REQUIRED: ['event_id', 'user_id', 'ts', 'props']
FIELD:event_type: {'anyOf': [{'type': 'string'}, {'type': 'null'}], 'default': 'click', 'description': 'Logical type of event', 'title': 'Event Type', 'yads': {'metadata': {'pii': False}}}
FIELD:user_id: {'maximum': 9223372036854775807, 'minimum': -9223372036854775808, 'title': 'User Id', 'type': 'integer'}
```

### Round-trip: Polars schema → Spec → Spark/DuckDB DDL

```python
import polars as pl
import yads

pl_schema = pl.Schema({
    "id": pl.Int64,
    "email": pl.String,
    "created_at": pl.Datetime(time_unit="ns", time_zone="UTC"),
    "spend": pl.Decimal(precision=10, scale=2),
    "tags": pl.List(pl.String),
})

spec = yads.from_polars(pl_schema, name="catalog.crm.customers", version="1.0.0")
print("SPEC:")
print(spec)

print("-- DuckDB DDL --")
print(yads.to_sql(spec, dialect="duckdb", pretty=True))

print("-- Spark DDL --")
print(yads.to_sql(spec, dialect="spark", pretty=True))
```

Output:

```text
SPEC:
spec catalog.crm.customers(version='1.0.0')(
  columns=[
    id: integer(bits=64)
    email: string
    created_at: timestamptz(unit=ns, tz=UTC)
    spend: decimal(precision=10, scale=2)
    tags: array<string>
  ]
)
-- DuckDB DDL --
CREATE TABLE catalog.crm.customers (
  id BIGINT,
  email TEXT,
  created_at TIMESTAMPTZ,
  spend DECIMAL(10, 2),
  tags TEXT[]
)
-- Spark DDL --
CREATE TABLE catalog.crm.customers (
  id BIGINT,
  email STRING,
  created_at TIMESTAMP,
  spend DECIMAL(10, 2),
  tags ARRAY<STRING>
)
```

## Design Philosophy

`yads` is spec-first, deterministic, and safe-by-default: given the same spec and backend, converters and loaders produce the same schema and the same validation diagnostics.

Conversions proceed silently only when they are lossless and fully semantics-preserving. When a backend cannot represent type parameters but preserves semantics (constraint loss, e.g. `String(length=10)` → `String()`), `yads` converts and emits structured warnings per affected field.

Backend type gaps are handled with value-preserving substitutes only; otherwise conversion requires an explicit `fallback_type`. Potentially lossy or reinterpreting changes (range narrowing, precision downgrades, sign changes, or unit changes) are never applied implicitly. Types with no value-preserving representation fail fast with clear errors and extension guidance.

Single rule: preserve semantics or notify; never lose or reinterpret data without explicit opt-in.