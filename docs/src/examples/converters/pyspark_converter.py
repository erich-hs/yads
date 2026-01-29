"""Executable example for converting a yads spec into a PySpark schema."""

from __future__ import annotations

import warnings

from ..base import ExampleBlockRequest, ExampleDefinition

warnings.simplefilter("ignore")


def _pyspark_schema_example() -> None:
    import yads
    from yads.converters import PySparkConverter, PySparkConverterConfig
    import json

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")

    converter = PySparkConverter(PySparkConverterConfig(mode="coerce"))
    schema = converter.convert(spec)
    print(json.dumps(schema.jsonValue(), indent=2))


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
