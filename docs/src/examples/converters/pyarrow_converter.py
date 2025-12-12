"""Executable example for converting a yads spec into a PyArrow schema."""

from __future__ import annotations

import warnings
from dataclasses import replace

import pyarrow as pa
import yads
import yads.types as ytypes
from yads.spec import Column

from ..base import ExampleBlockRequest, ExampleDefinition

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none, reportUnknownParameterType=none
# pyright: reportMissingParameterType=none

warnings.filterwarnings("ignore")

spec = yads.from_yaml("docs/src/specs/submissions.yaml")
spec_with_variant = replace(
    spec,
    columns=[*spec.columns, Column(name="payload", type=ytypes.Variant())],
)


def _pyarrow_converter_lowlevel_example() -> None:
    import yads
    from yads.converters import PyArrowConverter, PyArrowConverterConfig

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")

    converter = PyArrowConverter(PyArrowConverterConfig(mode="coerce"))
    schema = converter.convert(spec)
    print(schema)


def _pyarrow_converter_example() -> None:
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    schema = yads.to_pyarrow(spec)
    print(schema)


def _print_spec_example() -> None:
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    print(spec)


def _columns_scope_example() -> None:
    schema = yads.to_pyarrow(
        spec,
        include_columns={"submission_id", "submitted_at"},
    )
    print(schema)


def _conversion_mode_raise_example() -> None:
    import pyarrow as pa
    import yads.types as ytypes
    from yads.spec import Column
    from dataclasses import replace

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    spec_with_variant = replace(
        spec,
        columns=[*spec.columns, Column(name="payload", type=ytypes.Variant())],
    )

    try:
        yads.to_pyarrow(
            spec_with_variant,
            mode="raise",
            fallback_type=pa.string(),
        )
    except Exception as exc:
        print(type(exc).__name__ + ": " + str(exc))


def _conversion_mode_coerce_example() -> None:
    schema = yads.to_pyarrow(
        spec_with_variant,
        mode="coerce",
        fallback_type=pa.string(),
    )
    print(schema.field("payload"))


def _fallback_type_example() -> None:
    schema = yads.to_pyarrow(
        spec_with_variant,
        mode="coerce",
        fallback_type=pa.large_binary(),
    )
    print(schema.field("payload"))


def _column_override_example() -> None:
    def submitted_at_override(field, conv):
        return pa.field(
            field.name,
            pa.date32(),
            nullable=False,
            metadata={"description": "Replaced to pa.date32"},
        )

    schema = yads.to_pyarrow(
        spec,
        column_overrides={"submitted_at": submitted_at_override},
    )
    print(schema)


EXAMPLE = ExampleDefinition(
    example_id="pyarrow-converter-basic",
    blocks=(
        ExampleBlockRequest(
            slug="convert-example-code",
            language="python",
            source="callable",
            callable=_pyarrow_converter_example,
        ),
        ExampleBlockRequest(
            slug="convert-example-output",
            language="text",
            source="stdout",
            callable=_pyarrow_converter_example,
        ),
        ExampleBlockRequest(
            slug="convert-example-lowlevel-code",
            language="python",
            source="callable",
            callable=_pyarrow_converter_lowlevel_example,
        ),
        ExampleBlockRequest(
            slug="convert-example-lowlevel-output",
            language="text",
            source="stdout",
            callable=_pyarrow_converter_lowlevel_example,
        ),
        ExampleBlockRequest(
            slug="spec-print-code",
            language="python",
            source="callable",
            callable=_print_spec_example,
        ),
        ExampleBlockRequest(
            slug="spec-print-output",
            language="text",
            source="stdout",
            callable=_print_spec_example,
        ),
        ExampleBlockRequest(
            slug="columns-scope-code",
            language="python",
            source="callable",
            callable=_columns_scope_example,
        ),
        ExampleBlockRequest(
            slug="columns-scope-output",
            language="text",
            source="stdout",
            callable=_columns_scope_example,
        ),
        ExampleBlockRequest(
            slug="conversion-mode-raise-code",
            language="python",
            source="callable",
            callable=_conversion_mode_raise_example,
        ),
        ExampleBlockRequest(
            slug="conversion-mode-raise-output",
            language="text",
            source="stdout",
            callable=_conversion_mode_raise_example,
        ),
        ExampleBlockRequest(
            slug="conversion-mode-coerce-code",
            language="python",
            source="callable",
            callable=_conversion_mode_coerce_example,
        ),
        ExampleBlockRequest(
            slug="conversion-mode-coerce-output",
            language="text",
            source="stdout",
            callable=_conversion_mode_coerce_example,
        ),
        ExampleBlockRequest(
            slug="fallback-type-code",
            language="python",
            source="callable",
            callable=_fallback_type_example,
        ),
        ExampleBlockRequest(
            slug="fallback-type-output",
            language="text",
            source="stdout",
            callable=_fallback_type_example,
        ),
        ExampleBlockRequest(
            slug="column-override-code",
            language="python",
            source="callable",
            callable=_column_override_example,
        ),
        ExampleBlockRequest(
            slug="column-override-output",
            language="text",
            source="stdout",
            callable=_column_override_example,
        ),
    ),
)
