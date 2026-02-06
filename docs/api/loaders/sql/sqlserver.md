# SQL Server Loader

`SqlServerLoader` reads a SQL Server table definition into a canonical
`YadsSpec`.

<!-- BEGIN:example sqlserver-loader-basic loader-example-lowlevel-code -->
```python
import pyodbc
from yads.loaders.sql import SqlLoaderConfig, SqlServerLoader

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=analytics"
)
loader = SqlServerLoader(conn, SqlLoaderConfig(mode="coerce"))
spec = loader.load(
    "submissions",
    schema="dbo",
    name="prod.assessments.submissions",
    version=1,
)
print(spec)
```
<!-- END:example sqlserver-loader-basic loader-example-lowlevel-code -->
<!-- BEGIN:example sqlserver-loader-basic loader-example-lowlevel-output -->
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
<!-- END:example sqlserver-loader-basic loader-example-lowlevel-output -->

::: yads.loaders.sql.SqlServerLoader

::: yads.loaders.sql.SqlLoaderConfig
