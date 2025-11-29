"""Executable example for generating SQL DDL from a yads spec."""

from __future__ import annotations

from ..base import ExampleBlockRequest, ExampleDefinition


def _sql_converter_example() -> None:
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


EXAMPLE = ExampleDefinition(
    example_id="sql-converter-basic",
    blocks=(
        ExampleBlockRequest(
            slug="code",
            language="python",
            source="callable",
            callable=_sql_converter_example,
        ),
        ExampleBlockRequest(
            slug="output",
            language="text",
            source="stdout",
            callable=_sql_converter_example,
        ),
    ),
)


if __name__ == "__main__":
    _sql_converter_example()
