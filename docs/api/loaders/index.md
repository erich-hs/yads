---
icon: "lucide/arrow-big-left-dash"
---
# Loaders

Load external schemas or YAML specs into a canonical `YadsSpec`.

<!-- BEGIN:example pyarrow-loader-basic from-schema-code -->
```python
import pyarrow as pa
import yads

pyarrow_schema = pa.schema(
    [
        pa.field("submission_id", pa.int64(), nullable=False),
        pa.field("completion_percent", pa.decimal128(5, 2)),
        pa.field("time_taken_second", pa.int32()),
        pa.field("submitted_at", pa.timestamp("ns", tz="UTC")),
    ]
)

spec = yads.from_pyarrow(
    pyarrow_schema,
    name="prod.assessments.submissions",
    version=1,
)
print(spec)
```
<!-- END:example pyarrow-loader-basic from-schema-code -->
<!-- BEGIN:example pyarrow-loader-basic from-schema-output -->
```text
spec prod.assessments.submissions(version=1)(
  columns=[
    submission_id: integer(bits=64)(
      constraints=[NotNullConstraint()]
    )
    completion_percent: decimal(precision=5, scale=2, bits=128)
    time_taken_second: integer(bits=32)
    submitted_at: timestamptz(unit=ns, tz=UTC)
  ]
)
```
<!-- END:example pyarrow-loader-basic from-schema-output -->

The following sections outline the high-level entry point functions available in
`yads`. You can refer to the dedicated loader documentation for their complete
API reference with more customization options.

| Source | Helper | Loader |
| --- | --- | --- |
| YAML (path or stream) | `yads.from_yaml` | [`yads.loaders.YamlLoader`](yaml.md#yads.loaders.yaml_loader.YamlLoader) |
| PyArrow schema | `yads.from_pyarrow` | [`yads.loaders.PyArrowLoader`](pyarrow.md#yads.loaders.pyarrow_loader.PyArrowLoader) |
| PySpark StructType | `yads.from_pyspark` | [`yads.loaders.PySparkLoader`](pyspark.md#yads.loaders.pyspark_loader.PySparkLoader) |
| Polars schema | `yads.from_polars` | [`yads.loaders.PolarsLoader`](polars.md#yads.loaders.polars_loader.PolarsLoader) |

Other YAML helpers:
`yads.from_yaml_string`, `yads.from_yaml_path`, and `yads.from_yaml_stream`.

## Shared customization options

PyArrow, PySpark, and Polars loaders expose the following arguments for shaping
input into a valid spec:

- [`mode`](#loading-mode) toggles the load mode to one of `raise` or `coerce`.
- [`fallback_type`](#fallback-type) sets a `String` or `Binary` default when
  coercing unsupported source types.

### Loading mode

Switch between strict (`mode="raise"`) and permissive (`mode="coerce"`) runs per
call or via loader configuration. Here, a Dictionary column fails in raise mode.
<!-- BEGIN:example pyarrow-loader-basic conversion-mode-raise-code -->
```python
unsupported_pyarrow_schema = pa.schema(
    [
        pa.field("submission_id", pa.int64(), nullable=False),
        pa.field(
            "attributes",
            pa.dictionary(index_type=pa.int32(), value_type=pa.string()),
        ),
    ]
)

try:
    yads.from_pyarrow(
        unsupported_pyarrow_schema,
        name="prod.assessments.submissions",
        version=1,
        mode="raise",
    )
except Exception as exc:
    print(type(exc).__name__ + ": " + str(exc))
```
<!-- END:example pyarrow-loader-basic conversion-mode-raise-code -->
<!-- BEGIN:example pyarrow-loader-basic conversion-mode-raise-output -->
```text
UnsupportedFeatureError: PyArrowLoader does not support PyArrow type: 'dictionary<values=string, indices=int32, ordered=0>' (DictionaryType) for 'attributes'.
```
<!-- END:example pyarrow-loader-basic conversion-mode-raise-output -->

### Fallback type

Provide a fallback `YadsType` to keep loading unsupported logical types when
running in `coerce` mode.
<!-- BEGIN:example pyarrow-loader-basic conversion-mode-coerce-code -->
```python
spec = yads.from_pyarrow(
    unsupported_pyarrow_schema,
    name="prod.assessments.submissions",
    version=1,
    mode="coerce",
    fallback_type=ytypes.String(),
)
print(spec.columns[-1])
```
<!-- END:example pyarrow-loader-basic conversion-mode-coerce-code -->
<!-- BEGIN:example pyarrow-loader-basic conversion-mode-coerce-output -->
```text
attributes: string
```
<!-- END:example pyarrow-loader-basic conversion-mode-coerce-output -->

## Wrapper helpers

::: yads.loaders.from_yaml
    options:
      heading_level: 3

::: yads.loaders.from_yaml_string
    options:
      heading_level: 3

::: yads.loaders.from_yaml_path
    options:
      heading_level: 3

::: yads.loaders.from_yaml_stream
    options:
      heading_level: 3

::: yads.loaders.from_dict
    options:
      heading_level: 3

::: yads.loaders.from_pyarrow
    options:
      heading_level: 3

::: yads.loaders.from_pyspark
    options:
      heading_level: 3

::: yads.loaders.from_polars
    options:
      heading_level: 3
