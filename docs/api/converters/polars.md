# Polars Converter

`PolarsConverter` turns a canonical `YadsSpec` into a `pl.Schema`.

<!-- BEGIN:example polars-converter-basic code -->
```python
import yads
from yads.converters import PolarsConverter, PolarsConverterConfig
from pprint import pprint

spec = yads.from_yaml("docs/src/specs/submissions.yaml")

converter = PolarsConverter(PolarsConverterConfig(mode="coerce"))
schema = converter.convert(spec)
pprint(dict(schema))
```
<!-- END:example polars-converter-basic code -->
<!-- BEGIN:example polars-converter-basic output -->
```text
{'completion_percent': Decimal(precision=5, scale=2),
 'submission_id': Int64,
 'submitted_at': Datetime(time_unit='ns', time_zone='UTC'),
 'time_taken_seconds': Int32}
```
<!-- END:example polars-converter-basic output -->

!!! info
    Install one of the supported versions of Polars to use this converter with `uv add 'yads[polars]'`

::: yads.converters.polars_converter.PolarsConverter

::: yads.converters.polars_converter.PolarsConverterConfig
