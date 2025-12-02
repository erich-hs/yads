# Converters

Converters turn a validated `YadsSpec` into runtime-specific schemas or models.
Each converter shares the same filtering, override, and warning controls so you can move between runtimes with predictable
behavior. The table below lists the built-in targets plus the wrapper that
orchestrates each conversion.

| Dialect / Target | Helper | Converter | Install |
| --- | --- | --- | --- |
| PyArrow | `yads.to_pyarrow` | `yads.converters.PyArrowConverter` | `uv sync --group pyarrow` |
| Pydantic | `yads.to_pydantic` | `yads.converters.PydanticConverter` | `uv sync --group pydantic` |
| Polars | `yads.to_polars` | `yads.converters.PolarsConverter` | `uv sync --group polars` |
| PySpark | `yads.to_pyspark` | `yads.converters.PySparkConverter` | `uv sync --group pyspark` |
| Spark SQL | `yads.to_sql(dialect="spark")` | `yads.converters.sql.SparkSQLConverter` | `uv sync --group sql` |
| DuckDB SQL | `yads.to_sql(dialect="duckdb")` | `yads.converters.sql.DuckdbSQLConverter` | `uv sync --group sql` |

## Wrapper helpers

These helpers live in `yads.converters` and wire the public API to each
converter plus its config. They accept the same keyword arguments documented
below.

::: yads.converters.to_pyarrow
    options:
      heading_level: 3

::: yads.converters.to_pydantic
    options:
      heading_level: 3

::: yads.converters.to_polars
    options:
      heading_level: 3

::: yads.converters.to_pyspark
    options:
      heading_level: 3

::: yads.converters.to_sql
    options:
      heading_level: 3
