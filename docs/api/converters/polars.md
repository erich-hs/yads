# Polars Converter

`PolarsConverter` turns a `YadsSpec` into a `pl.Schema` so lazy or eager
pipelines can validate columns before collecting data. It honors the same
include/exclude filters as other converters and exposes overrides for unsupported
logical types.

<!-- BEGIN:example polars-converter-basic code -->
```python
from pprint import pprint

import yads.types as ytypes
from yads.spec import Column, YadsSpec
from yads.constraints import NotNullConstraint
from yads.converters import PolarsConverter

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

schema = PolarsConverter().convert(spec)
pprint(dict(schema))
```
<!-- END:example polars-converter-basic code -->
<!-- BEGIN:example polars-converter-basic output -->
```text
{'created_at': Datetime(time_unit='ns', time_zone='UTC'),
 'email': String,
 'id': Int64,
 'spend': Decimal(precision=10, scale=2),
 'tags': List(String)}
```
<!-- END:example polars-converter-basic output -->

!!! tip
    Install one of the supported versions of Polars to use this converter with `uv add yads[polars]`

::: yads.converters.polars_converter.PolarsConverter

::: yads.converters.polars_converter.PolarsConverterConfig
