# PyArrow Loader

`PyArrowLoader` turns a `PyArrow` schema into a canonical `YadsSpec`.

<!-- BEGIN:example pyarrow-loader-basic loader-example-lowlevel-code -->
```python
import pyarrow as pa
from yads.loaders import PyArrowLoader, PyArrowLoaderConfig

pyarrow_schema = pa.schema(
    [
        pa.field("submission_id", pa.int64(), nullable=False),
        pa.field("completion_percent", pa.decimal128(5, 2)),
        pa.field("time_taken_second", pa.int32()),
        pa.field("submitted_at", pa.timestamp("ns", tz="UTC")),
    ]
)

loader = PyArrowLoader(PyArrowLoaderConfig(mode="coerce"))
spec = loader.load(
    pyarrow_schema,
    name="prod.assessments.submissions",
    version=1,
)
print(spec)
```
<!-- END:example pyarrow-loader-basic loader-example-lowlevel-code -->
<!-- BEGIN:example pyarrow-loader-basic loader-example-lowlevel-output -->
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
<!-- END:example pyarrow-loader-basic loader-example-lowlevel-output -->

::: yads.loaders.pyarrow_loader.PyArrowLoader

::: yads.loaders.pyarrow_loader.PyArrowLoaderConfig