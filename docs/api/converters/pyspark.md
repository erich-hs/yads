# PySpark Converter

`PySparkConverter` produces `pyspark.sql.types.StructType` schemas from a
`YadsSpec`. Use it to keep DataFrame builders synchronized with the canonical
spec while still allowing overrides, column filters, and fallback types for
unsupported constructs.

<!-- BEGIN:example pyspark-converter-basic code -->
```python
from pprint import pprint

import yads.types as ytypes
from yads.spec import Column, YadsSpec
from yads.constraints import NotNullConstraint
from yads.converters import PySparkConverter

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

schema = PySparkConverter().convert(spec)
pprint(schema.jsonValue())
```
<!-- END:example pyspark-converter-basic code -->
<!-- BEGIN:example pyspark-converter-basic output -->
```text
{'fields': [{'metadata': {}, 'name': 'id', 'nullable': False, 'type': 'long'},
            {'metadata': {},
             'name': 'email',
             'nullable': True,
             'type': 'string'},
            {'metadata': {},
             'name': 'created_at',
             'nullable': True,
             'type': 'timestamp'},
            {'metadata': {},
             'name': 'spend',
             'nullable': True,
             'type': 'decimal(10,2)'},
            {'metadata': {},
             'name': 'tags',
             'nullable': True,
             'type': {'containsNull': True,
                      'elementType': 'string',
                      'type': 'array'}}],
 'type': 'struct'}
```
<!-- END:example pyspark-converter-basic output -->

!!! tip
    Install one of the supported versions of PySpark to use this converter with `uv add yads[pyspark]`

::: yads.converters.pyspark_converter.PySparkConverter

::: yads.converters.pyspark_converter.PySparkConverterConfig
