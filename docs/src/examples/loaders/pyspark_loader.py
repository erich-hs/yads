"""Executable examples for loading specs from PySpark schemas."""

from __future__ import annotations

from ..base import ExampleBlockRequest, ExampleDefinition


def _pyspark_loader_lowlevel_example() -> None:
    from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        DecimalType,
        TimestampType,
    )
    from yads.loaders import PySparkLoader, PySparkLoaderConfig

    pyspark_schema = StructType(
        [
            StructField("submission_id", IntegerType(), nullable=False),
            StructField("completion_percent", DecimalType(5, 2)),
            StructField("time_taken_second", IntegerType()),
            StructField("submitted_at", TimestampType()),
        ]
    )

    loader = PySparkLoader(PySparkLoaderConfig(mode="coerce"))
    spec = loader.load(
        pyspark_schema,
        name="prod.assessments.submissions",
        version=1,
    )
    print(spec)


EXAMPLE = ExampleDefinition(
    example_id="pyspark-loader-basic",
    blocks=(
        ExampleBlockRequest(
            slug="loader-example-lowlevel-code",
            language="python",
            source="callable",
            callable=_pyspark_loader_lowlevel_example,
        ),
        ExampleBlockRequest(
            slug="loader-example-lowlevel-output",
            language="text",
            source="stdout",
            callable=_pyspark_loader_lowlevel_example,
        ),
    ),
)
