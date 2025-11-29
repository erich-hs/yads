"""Executable example for converting a yads spec into a PySpark schema."""

from __future__ import annotations

from ..base import ExampleBlockRequest, ExampleDefinition


def _pyspark_schema_example() -> None:
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


EXAMPLE = ExampleDefinition(
    example_id="pyspark-converter-basic",
    blocks=(
        ExampleBlockRequest(
            slug="code",
            language="python",
            source="callable",
            callable=_pyspark_schema_example,
        ),
        ExampleBlockRequest(
            slug="output",
            language="text",
            source="stdout",
            callable=_pyspark_schema_example,
        ),
    ),
)


if __name__ == "__main__":
    _pyspark_schema_example()
