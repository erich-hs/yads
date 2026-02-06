# PostgreSQL Loader

`PostgreSqlLoader` reads a PostgreSQL table definition into a canonical
`YadsSpec`.

<!-- BEGIN:example postgres-loader-basic loader-example-lowlevel-code -->
```python
import psycopg2
from yads.loaders.sql import PostgreSqlLoader, SqlLoaderConfig

conn = psycopg2.connect("postgresql://localhost/analytics")
loader = PostgreSqlLoader(conn, SqlLoaderConfig(mode="coerce"))
spec = loader.load(
    "submissions",
    schema="public",
    name="prod.assessments.submissions",
    version=1,
)
print(spec)
```
<!-- END:example postgres-loader-basic loader-example-lowlevel-code -->
<!-- BEGIN:example postgres-loader-basic loader-example-lowlevel-output -->
```text
spec prod.assessments.submissions(version=1)(
  columns=[
    submission_id: integer(bits=64)(
      constraints=[NotNullConstraint(), PrimaryKeyConstraint()]
    )
    completion_percent: decimal(precision=5, scale=2)(
      constraints=[DefaultConstraint(value=0.0)]
    )
    time_taken_seconds: integer(bits=32)
    submitted_at: timestamptz(unit=us, tz=UTC)
  ]
)
```
<!-- END:example postgres-loader-basic loader-example-lowlevel-output -->

::: yads.loaders.sql.PostgreSqlLoader

::: yads.loaders.sql.SqlLoaderConfig
