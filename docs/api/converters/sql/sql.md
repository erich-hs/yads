# SQL Converters

Conversion from a `yads` spec to SQL starts with a neutral neutral [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree) representation that is handled by [`sqlglot`](https://sqlglot.com/sqlglot.html) via the [SQLGlotConverter](sqlglot.md).

A `SQLConverter` can then be used to convert this intermediary AST into a dialect-specific SQL DDL statement, while optionally handing the resulting tree to an `AstValidator` before rendering SQL to the target dialect.

The following flowchart illustrates the process:

```mermaid
flowchart TD
    SQLGlotConverter["SQLGlotConverter<br/>builds dialect-agnostic AST"]
    SQLGlotConverter --> SQLConverter["SQLConverter<br/>orchestrates AST -> SQL"]
    SQLConverter -->|optional| AstValidator["AstValidator<br/>rules adjust/guard AST"]
    AstValidator --> SQLConverter
    SQLConverter --> Spark["SparkSQLConverter<br/>dialect='spark'"]
    SQLConverter --> Duckdb["DuckdbSQLConverter<br/>dialect='duckdb'"]
    SQLConverter --> New["NewSQLConverter<br/>dialect='your-dialect'"]
    Spark --> SQLString["CREATE TABLE ..."]
    Duckdb --> SQLString
    New --> SQLString
```

Any `sqlglot` dialect string can be passed to `SQLConverter`, but for consistent guarantees, use one of the [Dedicated SQL Converters](#dedicated-sql-converters) that come with built-in dialect validations.

## SQL Converter

Use `SQLConverter` directly when you want to target arbitrary `sqlglot` dialects or supply your own AST validator stack. The example below loads a spec from disk and renders PostgreSQL DDL, but the only required change for other platforms is the `dialect` name in `SQLConverterConfig`.

<!-- BEGIN:example sql-converter-basic sql-converter-code -->
```python
import yads
from yads.converters.sql import SQLConverter, SQLConverterConfig

spec = yads.from_yaml("docs/src/specs/submissions.yaml")

converter = SQLConverter(
    SQLConverterConfig(
        dialect="postgres",
    )
)
ddl = converter.convert(spec, pretty=True)
print(ddl)
```
<!-- END:example sql-converter-basic sql-converter-code -->
<!-- BEGIN:example sql-converter-basic sql-converter-output -->
```text
CREATE TABLE prod.assessments.submissions (
  submission_id BIGINT NOT NULL,
  completion_percent DECIMAL(5, 2) DEFAULT 0.00,
  time_taken_seconds INT,
  submitted_at TIMESTAMPTZ
)
```
<!-- END:example sql-converter-basic sql-converter-output -->
!!! info
    Install one of the supported versions of SQLGlot to use this converter with `uv add 'yads[sql]'`

::: yads.converters.sql.sql_converter.SQLConverter

::: yads.converters.sql.sql_converter.SQLConverterConfig

## Dedicated SQL Converters

Dedicated SQL Converters subclass `SQLConverter` with pre-loaded validator rules, AST settings, and default dialects. They are the fastest path when you want officially supported DDL for those runtimes without tuning configs yourself.

<!-- BEGIN:example sql-converter-basic spark-converter-code -->
```python
import yads
from yads.converters.sql import SparkSQLConverter

spec = yads.from_yaml("docs/src/specs/submissions.yaml")

ddl = SparkSQLConverter().convert(spec, pretty=True)
print(ddl)
```
<!-- END:example sql-converter-basic spark-converter-code -->
<!-- BEGIN:example sql-converter-basic spark-converter-output -->
```text
CREATE TABLE prod.assessments.submissions (
  submission_id BIGINT NOT NULL,
  completion_percent DECIMAL(5, 2) DEFAULT 0.00,
  time_taken_seconds INT,
  submitted_at TIMESTAMP
)
```
<!-- END:example sql-converter-basic spark-converter-output -->

::: yads.converters.sql.sql_converter.SparkSQLConverter

::: yads.converters.sql.sql_converter.DuckdbSQLConverter
