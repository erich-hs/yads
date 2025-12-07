# SQLConverter

`SQLConverter` combines the AST generator (`SQLGlotConverter`) with optional
validators to emit reproducible CREATE TABLE statements. Dialect shortcuts such
as `SparkSQLConverter` and `DuckdbSQLConverter` preconfigure validation and
sqlglot settings so you can call `convert()` without hand-tuning options.

<!-- BEGIN:example sql-converter-basic code -->
```python
import yads.types as ytypes
from yads.spec import Column, YadsSpec
from yads.constraints import NotNullConstraint
from yads.converters.sql import SparkSQLConverter

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

ddl = SparkSQLConverter().convert(spec, pretty=True)
print(ddl)
```
<!-- END:example sql-converter-basic code -->
<!-- BEGIN:example sql-converter-basic output -->
```text
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  email STRING,
  created_at TIMESTAMP,
  spend DECIMAL(10, 2),
  tags ARRAY<STRING>
)
```
<!-- END:example sql-converter-basic output -->

!!! tip
    Install SQLGlot before running conversions: `uv sync --group sql`. Optional
    dialect extras pull in their own dependencies when needed.

::: yads.converters.sql.sql_converter.SQLConverter

::: yads.converters.sql.sql_converter.SQLConverterConfig

::: yads.converters.sql.sql_converter.SparkSQLConverter

::: yads.converters.sql.sql_converter.DuckdbSQLConverter