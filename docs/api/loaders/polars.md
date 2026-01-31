# Polars Loader

`PolarsLoader` turns a `Polars.Schema` into a canonical `YadsSpec`.

<!-- BEGIN:example polars-loader-basic loader-example-lowlevel-code -->
```python
import polars as pl
from yads.loaders import PolarsLoader, PolarsLoaderConfig

polars_schema = pl.Schema(
    {
        "submission_id": pl.Int64,
        "completion_percent": pl.Decimal(5, 2),
        "time_taken_second": pl.Int32,
        "submitted_at": pl.Datetime(time_unit="ns", time_zone="UTC"),
    }
)

loader = PolarsLoader(PolarsLoaderConfig(mode="coerce"))
spec = loader.load(
    polars_schema,
    name="prod.assessments.submissions",
    version=1,
)
print(spec)
```
<!-- END:example polars-loader-basic loader-example-lowlevel-code -->
<!-- BEGIN:example polars-loader-basic loader-example-lowlevel-output -->
```text
spec prod.assessments.submissions(version=1)(
  columns=[
    submission_id: integer(bits=64)
    completion_percent: decimal(precision=5, scale=2)
    time_taken_second: integer(bits=32)
    submitted_at: timestamptz(unit=ns, tz=UTC)
  ]
)
```
<!-- END:example polars-loader-basic loader-example-lowlevel-output -->

::: yads.loaders.polars_loader.PolarsLoader

::: yads.loaders.polars_loader.PolarsLoaderConfig