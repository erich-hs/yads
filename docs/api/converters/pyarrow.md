# PyArrow Converter

`PyArrowConverter` turns a validated `YadsSpec` into a `pyarrow.Schema` and
respects the same include/exclude filters available on every converter. Use it
directly or through `yads.to_pyarrow` whenever you need a deterministic schema
object for downstream Arrow consumers. The snippet below loads the shared
`submissions` spec from YAML and prints the resulting schema.

<!-- BEGIN:example pyarrow-converter-basic convert-example-lowlevel-code -->
```python
import yads
from yads.converters import PyArrowConverter, PyArrowConverterConfig

spec = yads.from_yaml("docs/src/specs/submissions.yaml")

converter = PyArrowConverter(PyArrowConverterConfig(mode="coerce"))
schema = converter.convert(spec)
print(schema)
```
<!-- END:example pyarrow-converter-basic convert-example-lowlevel-code -->
<!-- BEGIN:example pyarrow-converter-basic convert-example-lowlevel-output -->
```text
submission_id: int64 not null
completion_percent: decimal128(5, 2)
time_taken_seconds: int32
submitted_at: timestamp[ns, tz=UTC]
```
<!-- END:example pyarrow-converter-basic convert-example-lowlevel-output -->

!!! info
    Install one of the supported versions of PyArrow to use this converter with
    `uv add 'yads[pyarrow]'`

::: yads.converters.pyarrow_converter.PyArrowConverter

`PyArrowConverterConfig` offers fine grained control over string/list sizing,
column overrides, and fallback coercions for unsupported logical types.

::: yads.converters.pyarrow_converter.PyArrowConverterConfig
