# PyArrow Converter

Converters bridge a validated `YadsSpec` into runtime schemas. Each converter
focuses on one target so you can reason about limitations or dependency needs
without surprise interactions. The PyArrow converter ships in the core package,
and you simply add the `pyarrow` extra when you want a strongly typed schema for
downstream tooling.

`PyArrowConverter` turns a spec into a `pyarrow.Schema`. It supports field
filtering, column overrides, and multiple coercion strategies so you can decide
whether unsupported constructs should raise or degrade gracefully.

```python
import pyarrow as pa
import yads.types as ytypes
from yads.converters import PyArrowConverter
from yads.spec import Column, YadsSpec

spec = YadsSpec(
    name="catalog.db.table",
    version=1,
    columns=[
        Column(name="id", type=ytypes.Integer(bits=64)),
        Column(name="name", type=ytypes.String()),
    ],
)

schema = PyArrowConverter().convert(spec)
assert schema == pa.schema([
    pa.field("id", pa.int64(), nullable=True),
    pa.field("name", pa.string(), nullable=True),
])
```

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
