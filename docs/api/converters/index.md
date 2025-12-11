---
icon: "lucide/arrow-big-right-dash"
---
# Converters

Convert a canonical `YadsSpec` to runtime schema objects for multiple formats.

<!-- BEGIN:example pyarrow-converter-basic convert-example-code -->
```python
import yads

spec = yads.from_yaml("docs/src/specs/submissions.yaml")
schema = yads.to_pyarrow(spec)
print(schema)
```
<!-- END:example pyarrow-converter-basic convert-example-code -->
<!-- BEGIN:example pyarrow-converter-basic convert-example-output -->
```text
submission_id: int64 not null
completion_percent: decimal128(5, 2)
time_taken_seconds: int32
submitted_at: timestamp[ns, tz=UTC]
```
<!-- END:example pyarrow-converter-basic convert-example-output -->

The following sections outline the high level-entry points functions available in `yads`. You can refer to the dedicated converter documentation for their complete API reference with more customization options.

=== "uv"
    | Target | Helper | Converter | Install |
    | --- | --- | --- | --- |
    | PyArrow | `yads.to_pyarrow` | [`yads.converters.PyArrowConverter`](pyarrow.md#yads.converters.pyarrow_converter.PyArrowConverter) | `uv add 'yads[pyarrow]'` |
    | Pydantic | `yads.to_pydantic` | [`yads.converters.PydanticConverter`](pydantic.md#yads.converters.pydantic_converter.PydanticConverter) | `uv add 'yads[pydantic]'` |
    | Polars | `yads.to_polars` | [`yads.converters.PolarsConverter`](polars.md#yads.converters.polars_converter.PolarsConverter) | `uv add 'yads[polars]'` |
    | PySpark | `yads.to_pyspark` | [`yads.converters.PySparkConverter`](pyspark.md#yads.converters.pyspark_converter.PySparkConverter) | `uv add 'yads[pyspark]'` |
    | Spark SQL | `yads.to_sql(dialect="spark")` | [`yads.converters.sql.SparkSQLConverter`](sql/sql.md#yads.converters.sql.sql_converter.SparkSQLConverter) | `uv add 'yads[sql]'` |
    | DuckDB SQL | `yads.to_sql(dialect="duckdb")` | [`yads.converters.sql.DuckdbSQLConverter`](sql/sql.md#yads.converters.sql.sql_converter.DuckdbSQLConverter) | `uv add 'yads[sql]'` |

=== "pip"
    | Target | Helper | Converter | Install |
    | --- | --- | --- | --- |
    | PyArrow | `yads.to_pyarrow` | [`yads.converters.PyArrowConverter`](pyarrow.md#yads.converters.pyarrow_converter.PyArrowConverter) | `pip install 'yads[pyarrow]'` |
    | Pydantic | `yads.to_pydantic` | [`yads.converters.PydanticConverter`](pydantic.md#yads.converters.pydantic_converter.PydanticConverter) | `pip install 'yads[pydantic]'` |
    | Polars | `yads.to_polars` | [`yads.converters.PolarsConverter`](polars.md#yads.converters.polars_converter.PolarsConverter) | `pip install 'yads[polars]'` |
    | PySpark | `yads.to_pyspark` | [`yads.converters.PySparkConverter`](pyspark.md#yads.converters.pyspark_converter.PySparkConverter) | `pip install 'yads[pyspark]'` |
    | Spark SQL | `yads.to_sql(dialect="spark")` | [`yads.converters.sql.SparkSQLConverter`](sql/sql.md#yads.converters.sql.sql_converter.SparkSQLConverter) | `pip install 'yads[sql]'` |
    | DuckDB SQL | `yads.to_sql(dialect="duckdb")` | [`yads.converters.sql.DuckdbSQLConverter`](sql/sql.md#yads.converters.sql.sql_converter.DuckdbSQLConverter) | `pip install 'yads[sql]'` |

## Shared customization options

All converters expose the following arguments for shaping output:

- [`include_columns` / `ignore_columns`](#columns-scope) scope the target schema.
- [`mode`](#conversion-mode) toggles the convert mode to one of `raise` or `coerce`.
- [`fallback_type`](#fallback-type) can be used to specify a custom fallback type other than the default.
- [`column_overrides`](#column-overrides) allows for specific overrides that take precedence over the converter's built-in logic.

### Columns scope

Restrict conversion to only the columns you need for a given consumer.
<!-- BEGIN:example pyarrow-converter-basic spec-print-code -->
```python
import yads

spec = yads.from_yaml("docs/src/specs/submissions.yaml")
print(spec)
```
<!-- END:example pyarrow-converter-basic spec-print-code -->
<!-- BEGIN:example pyarrow-converter-basic spec-print-output -->
```text
spec prod.assessments.submissions(version=1)(
  columns=[
    submission_id: integer(bits=64)(
      constraints=[PrimaryKeyConstraint(), NotNullConstraint()]
    )
    completion_percent: decimal(precision=5, scale=2)(
      constraints=[DefaultConstraint(value=0.0)]
    )
    time_taken_seconds: integer(bits=32)
    submitted_at: timestamptz(unit=ns, tz=UTC)
  ]
)
```
<!-- END:example pyarrow-converter-basic spec-print-output -->
<!-- BEGIN:example pyarrow-converter-basic columns-scope-code -->
```python
schema = yads.to_pyarrow(
    spec,
    include_columns={"submission_id", "submitted_at"},
)
print(schema)
```
<!-- END:example pyarrow-converter-basic columns-scope-code -->
<!-- BEGIN:example pyarrow-converter-basic columns-scope-output -->
```text
submission_id: int64 not null
submitted_at: timestamp[ns, tz=UTC]
```
<!-- END:example pyarrow-converter-basic columns-scope-output -->

### Conversion mode

Switch between strict (`mode="raise"`) and permissive (`mode="coerce"`) runs per
call. Here, a Variant column fails in raise mode.

<!-- BEGIN:example pyarrow-converter-basic conversion-mode-raise-code -->
```python
import pyarrow as pa
import yads.types as ytypes
from yads.spec import Column
from dataclasses import replace

spec = yads.from_yaml("docs/src/specs/submissions.yaml")
spec_with_variant = replace(
    spec,
    columns=[*spec.columns, Column(name="payload", type=ytypes.Variant())],
)

try:
    yads.to_pyarrow(
        spec_with_variant,
        mode="raise",
        fallback_type=pa.string(),
    )
except Exception as exc:
    print(type(exc).__name__ + ": " + str(exc))
```
<!-- END:example pyarrow-converter-basic conversion-mode-raise-code -->
<!-- BEGIN:example pyarrow-converter-basic conversion-mode-raise-output -->
```text
UnsupportedFeatureError: PyArrowConverter does not support type: variant for 'payload'.
```
<!-- END:example pyarrow-converter-basic conversion-mode-raise-output -->

But coerces to the default `fallback_type` when in coerce mode.
<!-- BEGIN:example pyarrow-converter-basic conversion-mode-coerce-code -->
```python
schema = yads.to_pyarrow(
    spec_with_variant,
    mode="coerce",
    fallback_type=pa.string(),
)
print(schema.field("payload"))
```
<!-- END:example pyarrow-converter-basic conversion-mode-coerce-code -->
<!-- BEGIN:example pyarrow-converter-basic conversion-mode-coerce-output -->
```text
pyarrow.Field<payload: string>
```
<!-- END:example pyarrow-converter-basic conversion-mode-coerce-output -->

### Fallback type

Provide a fallback Arrow type to keep converting unsupported logical types when
running in `coerce` mode.

<!-- BEGIN:example pyarrow-converter-basic fallback-type-code -->
```python
schema = yads.to_pyarrow(
    spec_with_variant,
    mode="coerce",
    fallback_type=pa.large_binary(),
)
print(schema.field("payload"))
```
<!-- END:example pyarrow-converter-basic fallback-type-code -->
<!-- BEGIN:example pyarrow-converter-basic fallback-type-output -->
```text
pyarrow.Field<payload: large_binary>
```
<!-- END:example pyarrow-converter-basic fallback-type-output -->

### Column overrides

Customize individual fields for downstream constraints or metadata.

<!-- BEGIN:example pyarrow-converter-basic column-override-code -->
```python
def submitted_at_override(field, conv):
    return pa.field(
        field.name,
        pa.date32(),
        nullable=False,
        metadata={"description": "Replaced to pa.date32"},
    )

schema = yads.to_pyarrow(
    spec,
    column_overrides={"submitted_at": submitted_at_override},
)
print(schema)
```
<!-- END:example pyarrow-converter-basic column-override-code -->
<!-- BEGIN:example pyarrow-converter-basic column-override-output -->
```text
submission_id: int64 not null
completion_percent: decimal128(5, 2)
time_taken_seconds: int32
submitted_at: date32[day] not null
  -- field metadata --
  description: 'Replaced to pa.date32'
```
<!-- END:example pyarrow-converter-basic column-override-output -->

## Wrapper helpers

::: yads.converters.to_pyarrow
    options:
      heading_level: 3

::: yads.converters.to_pydantic
    options:
      heading_level: 3

::: yads.converters.to_polars
    options:
      heading_level: 3

::: yads.converters.to_pyspark
    options:
      heading_level: 3

::: yads.converters.to_sql
    options:
      heading_level: 3
