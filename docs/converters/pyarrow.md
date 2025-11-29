# PyArrow Converter

`PyArrowConverter` turns a validated `YadsSpec` into a `pyarrow.Schema` and
respects the same include/exclude filters available on every converter. Use it
directly or through `yads.to_pyarrow` whenever you need a deterministic schema
object for downstream Arrow consumers. The snippet below prints the schema for
the canonical `customers` spec.

<!-- BEGIN:example pyarrow-converter-basic code -->
```python
import yads.types as ytypes
from yads.spec import Column, YadsSpec
from yads.constraints import NotNullConstraint
from yads.converters import PyArrowConverter

spec = YadsSpec(
    name="catalog.crm.customers",
    version=1,
    columns=[
        Column(
            name="id",
            type=ytypes.Integer(bits=64),
            constraints=[NotNullConstraint()],
        ),
        Column(name="email", type=ytypes.String()),
        Column(name="created_at", type=ytypes.TimestampTZ(tz="UTC")),
        Column(
            name="spend",
            type=ytypes.Decimal(precision=10, scale=2),
        ),
        Column(name="tags", type=ytypes.Array(element=ytypes.String())),
    ],
)

schema = PyArrowConverter().convert(spec)
print(schema)
```
<!-- END:example pyarrow-converter-basic code -->
<!-- BEGIN:example pyarrow-converter-basic output -->
```text
id: int64 not null
email: string
created_at: timestamp[ns, tz=UTC]
spend: decimal128(10, 2)
tags: list<item: string>
  child 0, item: string
```
<!-- END:example pyarrow-converter-basic output -->

!!! tip
    Install one of the supported versions of PyArrow to use this converter with `uv add yads[pyarrow]`

::: yads.converters.pyarrow_converter.PyArrowConverter

`PyArrowConverterConfig` offers fine grained control over string/list sizing,
column overrides, and fallback coercions for unsupported logical types.

::: yads.converters.pyarrow_converter.PyArrowConverterConfig
