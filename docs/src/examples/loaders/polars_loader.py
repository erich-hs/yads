"""Executable examples for loading specs from Polars schemas."""

from __future__ import annotations

from ..base import ExampleBlockRequest, ExampleDefinition


def _polars_loader_lowlevel_example() -> None:
    import polars as pl
    from yads.loaders import PolarsLoader, PolarsLoaderConfig

    polars_schema = pl.Schema(
        {
            "submission_id": pl.Int64,
            "completion_percent": pl.Decimal(5, 2),
            "time_taken_second": pl.Int32,
            "submitted_at": pl.Datetime(time_unit="ns", time_zone="UTC"),
        }
    )

    loader = PolarsLoader(PolarsLoaderConfig(mode="coerce"))
    spec = loader.load(
        polars_schema,
        name="prod.assessments.submissions",
        version=1,
    )
    print(spec)


EXAMPLE = ExampleDefinition(
    example_id="polars-loader-basic",
    blocks=(
        ExampleBlockRequest(
            slug="loader-example-lowlevel-code",
            language="python",
            source="callable",
            callable=_polars_loader_lowlevel_example,
        ),
        ExampleBlockRequest(
            slug="loader-example-lowlevel-output",
            language="text",
            source="stdout",
            callable=_polars_loader_lowlevel_example,
        ),
    ),
)
