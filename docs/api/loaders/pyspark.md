# PySpark Loader

`PySparkLoader` turns a `PySpark.StructType` into a canonical `YadsSpec`.

<!-- BEGIN:example pyspark-loader-basic loader-example-lowlevel-code -->
```python
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DecimalType,
    TimestampType,
)
from yads.loaders import PySparkLoader, PySparkLoaderConfig

pyspark_schema = StructType(
    [
        StructField("submission_id", IntegerType(), nullable=False),
        StructField("completion_percent", DecimalType(5, 2)),
        StructField("time_taken_second", IntegerType()),
        StructField("submitted_at", TimestampType()),
    ]
)

loader = PySparkLoader(PySparkLoaderConfig(mode="coerce"))
spec = loader.load(
    pyspark_schema,
    name="prod.assessments.submissions",
    version=1,
)
print(spec)
```
<!-- END:example pyspark-loader-basic loader-example-lowlevel-code -->
<!-- BEGIN:example pyspark-loader-basic loader-example-lowlevel-output -->
```text
spec prod.assessments.submissions(version=1)(
  columns=[
    submission_id: integer(bits=32)(
      constraints=[NotNullConstraint()]
    )
    completion_percent: decimal(precision=5, scale=2)
    time_taken_second: integer(bits=32)
    submitted_at: timestampltz(unit=ns)
  ]
)
```
<!-- END:example pyspark-loader-basic loader-example-lowlevel-output -->

::: yads.loaders.pyspark_loader.PySparkLoader

::: yads.loaders.pyspark_loader.PySparkLoaderConfig