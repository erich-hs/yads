"""Executable examples for loading specs from PyArrow schemas."""

from __future__ import annotations

import pyarrow as pa
import yads
import yads.types as ytypes

from ..base import ExampleBlockRequest, ExampleDefinition

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none, reportUnknownParameterType=none

pyarrow_schema = pa.schema(
    [
        pa.field("submission_id", pa.int64(), nullable=False),
        pa.field("completion_percent", pa.decimal128(5, 2)),
        pa.field("time_taken_second", pa.int32()),
        pa.field("submitted_at", pa.timestamp("ns", tz="UTC")),
    ]
)

unsupported_pyarrow_schema = pa.schema(
    [
        pa.field("submission_id", pa.int64(), nullable=False),
        pa.field(
            "attributes",
            pa.dictionary(index_type=pa.int32(), value_type=pa.string()),
        ),
    ]
)


def _from_pyarrow_example() -> None:
    import pyarrow as pa
    import yads

    pyarrow_schema = pa.schema(
        [
            pa.field("submission_id", pa.int64(), nullable=False),
            pa.field("completion_percent", pa.decimal128(5, 2)),
            pa.field("time_taken_second", pa.int32()),
            pa.field("submitted_at", pa.timestamp("ns", tz="UTC")),
        ]
    )

    spec = yads.from_pyarrow(
        pyarrow_schema,
        name="prod.assessments.submissions",
        version=1,
    )
    print(spec)


def _conversion_mode_raise_example() -> None:
    unsupported_pyarrow_schema = pa.schema(
        [
            pa.field("submission_id", pa.int64(), nullable=False),
            pa.field(
                "attributes",
                pa.dictionary(index_type=pa.int32(), value_type=pa.string()),
            ),
        ]
    )

    try:
        yads.from_pyarrow(
            unsupported_pyarrow_schema,
            name="prod.assessments.submissions",
            version=1,
            mode="raise",
        )
    except Exception as exc:
        print(type(exc).__name__ + ": " + str(exc))


def _conversion_mode_coerce_example() -> None:
    spec = yads.from_pyarrow(
        unsupported_pyarrow_schema,
        name="prod.assessments.submissions",
        version=1,
        mode="coerce",
        fallback_type=ytypes.String(),
    )
    print(spec.columns[-1])


EXAMPLE = ExampleDefinition(
    example_id="pyarrow-loader-basic",
    blocks=(
        ExampleBlockRequest(
            slug="from-schema-code",
            language="python",
            source="callable",
            callable=_from_pyarrow_example,
        ),
        ExampleBlockRequest(
            slug="from-schema-output",
            language="text",
            source="stdout",
            callable=_from_pyarrow_example,
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
    ),
)
