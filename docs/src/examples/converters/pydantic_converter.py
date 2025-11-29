"""Executable example for converting a yads spec into a Pydantic model."""

from __future__ import annotations

from ..base import ExampleBlockRequest, ExampleDefinition


def _pydantic_model_example() -> None:
    from decimal import Decimal
    from pprint import pprint

    import yads.types as ytypes
    from yads.spec import Column, YadsSpec
    from yads.constraints import NotNullConstraint
    from yads.converters import PydanticConverter

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
            Column(name="created_at", type=ytypes.Timestamp()),
            Column(
                name="spend",
                type=ytypes.Decimal(precision=10, scale=2),
            ),
            Column(name="tags", type=ytypes.Array(element=ytypes.String())),
        ],
    )

    Customer = PydanticConverter().convert(spec)
    alice = Customer(
        id=1,
        email="alice@example.com",
        created_at="2024-01-02T15:04:05Z",
        spend=Decimal("12.34"),
        tags=["vip", "beta"],
    )
    pprint(alice.model_dump())


EXAMPLE = ExampleDefinition(
    example_id="pydantic-converter-basic",
    blocks=(
        ExampleBlockRequest(
            slug="code",
            language="python",
            source="callable",
            callable=_pydantic_model_example,
        ),
        ExampleBlockRequest(
            slug="output",
            language="text",
            source="stdout",
            callable=_pydantic_model_example,
        ),
    ),
)


if __name__ == "__main__":
    _pydantic_model_example()
