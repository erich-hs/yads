"""Executable example for converting a yads spec into a PyArrow schema."""

from __future__ import annotations

from .base import ExampleBlockRequest, ExampleDefinition


def _pyarrow_schema_example() -> None:
    from yads.constraints import NotNullConstraint
    from yads.converters import PyArrowConverter
    from yads.spec import Column, YadsSpec
    import yads.types as ytypes

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

    schema = PyArrowConverter().convert(spec)
    print(schema)


EXAMPLE = ExampleDefinition(
    example_id="pyarrow-converter-basic",
    callable=_pyarrow_schema_example,
    blocks=(
        ExampleBlockRequest(slug="code", language="python", source="callable"),
        ExampleBlockRequest(slug="output", language="text", source="stdout"),
    ),
)


if __name__ == "__main__":
    _pyarrow_schema_example()
