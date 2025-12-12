# PySpark Converter

`PySparkConverter` produces `pyspark.sql.types.StructType` schemas from a
`YadsSpec`. Use it to keep DataFrame builders synchronized with the canonical
spec while still allowing overrides, column filters, and fallback types for
unsupported constructs.

<!-- BEGIN:example pyspark-converter-basic code -->
```python
import yads
from yads.converters import PySparkConverter, PySparkConverterConfig
import json

spec = yads.from_yaml("docs/src/specs/submissions.yaml")

converter = PySparkConverter(PySparkConverterConfig(mode="coerce"))
schema = converter.convert(spec)
print(json.dumps(schema.jsonValue(), indent=2))
```
<!-- END:example pyspark-converter-basic code -->
<!-- BEGIN:example pyspark-converter-basic output -->
```text
{
  "type": "struct",
  "fields": [
    {
      "name": "submission_id",
      "type": "long",
      "nullable": false,
      "metadata": {}
    },
    {
      "name": "completion_percent",
      "type": "decimal(5,2)",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "time_taken_seconds",
      "type": "integer",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "submitted_at",
      "type": "timestamp",
      "nullable": true,
      "metadata": {}
    }
  ]
}
```
<!-- END:example pyspark-converter-basic output -->

!!! info
    Install one of the supported versions of PySpark to use this converter with `uv add 'yads[pyspark]'`

::: yads.converters.pyspark_converter.PySparkConverter

::: yads.converters.pyspark_converter.PySparkConverterConfig
