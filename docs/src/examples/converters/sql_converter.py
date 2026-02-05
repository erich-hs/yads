"""Executable examples for generating SQL DDL from a yads spec."""

from __future__ import annotations

from ..base import ExampleBlockRequest, ExampleDefinition


def _sql_converter_example() -> None:
    import yads
    from yads.converters.sql import SqlConverter, SqlConverterConfig

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")

    converter = SqlConverter(
        SqlConverterConfig(
            dialect="postgres",
        )
    )
    ddl = converter.convert(spec, pretty=True)
    print(ddl)


def _spark_sql_converter_example() -> None:
    import yads
    from yads.converters.sql import SparkSqlConverter

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")

    ddl = SparkSqlConverter().convert(spec, pretty=True)
    print(ddl)


EXAMPLE = ExampleDefinition(
    example_id="sql-converter-basic",
    blocks=(
        ExampleBlockRequest(
            slug="sql-converter-code",
            language="python",
            source="callable",
            callable=_sql_converter_example,
        ),
        ExampleBlockRequest(
            slug="sql-converter-output",
            language="text",
            source="stdout",
            callable=_sql_converter_example,
        ),
        ExampleBlockRequest(
            slug="spark-converter-code",
            language="python",
            source="callable",
            callable=_spark_sql_converter_example,
        ),
        ExampleBlockRequest(
            slug="spark-converter-output",
            language="text",
            source="stdout",
            callable=_spark_sql_converter_example,
        ),
    ),
)


if __name__ == "__main__":
    _sql_converter_example()
    _spark_sql_converter_example()
