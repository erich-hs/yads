# PyArrow Converter

Converters bridge a validated `YadsSpec` into runtime schemas. Each converter
focuses on one target so you can reason about limitations or dependency needs
without surprise interactions. The PyArrow converter ships in the core package,
and you simply add the `pyarrow` extra when you want a strongly typed schema for
downstream tooling.

`PyArrowConverter` turns a spec into a `pyarrow.Schema`. It supports field
filtering, column overrides, and multiple coercion strategies so you can decide
whether unsupported constructs should raise or degrade gracefully. The example
below prints the resulting schema for the canonical customers spec:

<!-- BEGIN:example pyarrow-converter-basic code -->
```python
from yads.constraints import NotNullConstraint
from yads.converters import PyArrowConverter
from yads.spec import Column, YadsSpec
import yads.types as ytypes

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
    Install PyArrow before running conversions: `uv sync --group pyarrow`. The
    converter raises `yads.exceptions.DependencyMissingError` when the optional
    dependency is not available.

::: yads.converters.pyarrow_converter.PyArrowConverter
    options:
      show_root_heading: false

## Configuration

`PyArrowConverterConfig` offers fine grained control over string/list sizing,
column overrides, and fallback coercions for unsupported logical types.

::: yads.converters.pyarrow_converter.PyArrowConverterConfig
    options:
      show_root_heading: false
