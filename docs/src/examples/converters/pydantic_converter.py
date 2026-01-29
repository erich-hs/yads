"""Executable example for converting a yads spec into a Pydantic model."""

from __future__ import annotations

from ..base import ExampleBlockRequest, ExampleDefinition

import warnings

warnings.simplefilter("ignore")


def _pydantic_model_example() -> None:
    import yads
    from yads.converters import PydanticConverter, PydanticConverterConfig
    import json

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")

    converter = PydanticConverter(PydanticConverterConfig(mode="coerce"))
    Submission = converter.convert(spec)
    print(json.dumps(Submission.model_json_schema(), indent=2))


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
