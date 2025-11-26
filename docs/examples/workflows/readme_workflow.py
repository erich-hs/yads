"""Executable README workflow snippets."""

from __future__ import annotations

from pathlib import Path

import yads

from ..base import ExampleBlockRequest, ExampleDefinition

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SPEC_REFERENCE = "registry/specs/customers.yaml"
SPEC_FILE_PATH = PROJECT_ROOT / SPEC_REFERENCE

spec = yads.from_yaml(SPEC_FILE_PATH)
Customers = yads.to_pydantic(spec, model_name="Customers")

spec_with_file_path = f"# {SPEC_REFERENCE}\n{SPEC_FILE_PATH.read_text().strip()}"


def _load_spec_step() -> None:
    import yads

    spec = yads.from_yaml("registry/specs/customers.yaml")

    # Generate a Pydantic BaseModel
    Customers = yads.to_pydantic(spec, model_name="Customers")

    print(Customers)
    print(list(Customers.model_fields.keys()))


def _validate_record_step() -> None:
    from datetime import datetime, timezone

    record = Customers(
        id=123,
        email="alice@example.com",
        created_at=datetime(2024, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
        spend="42.50",
        tags=["vip", "beta"],
    )

    print(record.model_dump())


def _spark_sql_step() -> None:
    spark_ddl = yads.to_sql(spec, dialect="spark", pretty=True)
    print(spark_ddl)


def _duckdb_sql_step() -> None:
    duckdb_ddl = yads.to_sql(spec, dialect="duckdb", pretty=True)
    print(duckdb_ddl)


def _polars_schema_step() -> None:
    import yads

    pl_schema = yads.to_polars(spec)
    print(pl_schema)


def _pyarrow_schema_step() -> None:
    import yads

    pa_schema = yads.to_pyarrow(spec)
    print(pa_schema)


EXAMPLE = ExampleDefinition(
    example_id="readme-workflow",
    callables={
        "load-spec": _load_spec_step,
        "validate-record": _validate_record_step,
        "spark-sql": _spark_sql_step,
        "duckdb-sql": _duckdb_sql_step,
        "polars-schema": _polars_schema_step,
        "pyarrow-schema": _pyarrow_schema_step,
    },
    blocks=(
        ExampleBlockRequest(
            slug="spec-yaml",
            language="yaml",
            source="literal",
            text=spec_with_file_path.strip(),
        ),
        ExampleBlockRequest(
            slug="load-spec-code",
            language="python",
            source="callable",
            step="load-spec",
        ),
        ExampleBlockRequest(
            slug="load-spec-output",
            language="text",
            source="stdout",
            step="load-spec",
        ),
        ExampleBlockRequest(
            slug="validate-record-code",
            language="python",
            source="callable",
            step="validate-record",
        ),
        ExampleBlockRequest(
            slug="validate-record-output",
            language="text",
            source="stdout",
            step="validate-record",
        ),
        ExampleBlockRequest(
            slug="spark-sql-code",
            language="python",
            source="callable",
            step="spark-sql",
        ),
        ExampleBlockRequest(
            slug="spark-sql-output",
            language="sql",
            source="stdout",
            step="spark-sql",
        ),
        ExampleBlockRequest(
            slug="duckdb-sql-code",
            language="python",
            source="callable",
            step="duckdb-sql",
        ),
        ExampleBlockRequest(
            slug="duckdb-sql-output",
            language="sql",
            source="stdout",
            step="duckdb-sql",
        ),
        ExampleBlockRequest(
            slug="polars-code",
            language="python",
            source="callable",
            step="polars-schema",
        ),
        ExampleBlockRequest(
            slug="polars-output",
            language="text",
            source="stdout",
            step="polars-schema",
        ),
        ExampleBlockRequest(
            slug="pyarrow-code",
            language="python",
            source="callable",
            step="pyarrow-schema",
        ),
        ExampleBlockRequest(
            slug="pyarrow-output",
            language="text",
            source="stdout",
            step="pyarrow-schema",
        ),
    ),
)


if __name__ == "__main__":
    _pyarrow_schema_step()
