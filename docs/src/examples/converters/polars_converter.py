"""Executable example for converting a yads spec into a Polars schema."""

from __future__ import annotations

from ..base import ExampleBlockRequest, ExampleDefinition


def _polars_schema_example() -> None:
    import yads
    from yads.converters import PolarsConverter, PolarsConverterConfig
    from pprint import pprint

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")

    converter = PolarsConverter(PolarsConverterConfig(mode="coerce"))
    schema = converter.convert(spec)
    pprint(dict(schema))


EXAMPLE = ExampleDefinition(
    example_id="polars-converter-basic",
    blocks=(
        ExampleBlockRequest(
            slug="code",
            language="python",
            source="callable",
            callable=_polars_schema_example,
        ),
        ExampleBlockRequest(
            slug="output",
            language="text",
            source="stdout",
            callable=_polars_schema_example,
        ),
    ),
)


if __name__ == "__main__":
    _polars_schema_example()
