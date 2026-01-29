from __future__ import annotations

from pathlib import Path

import yads
import warnings

from ..base import ExampleBlockRequest, ExampleDefinition

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parents[4]
SPEC_REFERENCE = "docs/src/specs/customers.yaml"
SPEC_FILE_PATH = REPO_ROOT / SPEC_REFERENCE

spec = yads.from_yaml(SPEC_FILE_PATH)
Customers = yads.to_pydantic(spec, model_name="Customers")

spec_with_file_path = f"# {SPEC_REFERENCE}\n{SPEC_FILE_PATH.read_text().strip()}"

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none


def _load_spec_step() -> None:
    import yads

    spec = yads.from_yaml("docs/src/specs/customers.yaml")

    # Generate a Pydantic BaseModel
    Customers = yads.to_pydantic(spec, model_name="Customers")

    print(Customers)
    print(list(Customers.model_fields.keys()))


def _pydantic_model_step() -> None:
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
    example_id="minimal-yaml-to-others",
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
            callable=_load_spec_step,
        ),
        ExampleBlockRequest(
            slug="load-spec-output",
            language="text",
            source="stdout",
            callable=_load_spec_step,
        ),
        ExampleBlockRequest(
            slug="pydantic-model-code",
            language="python",
            source="callable",
            callable=_pydantic_model_step,
        ),
        ExampleBlockRequest(
            slug="pydantic-model-output",
            language="text",
            source="stdout",
            callable=_pydantic_model_step,
        ),
        ExampleBlockRequest(
            slug="spark-sql-code",
            language="python",
            source="callable",
            callable=_spark_sql_step,
        ),
        ExampleBlockRequest(
            slug="spark-sql-output",
            language="sql",
            source="stdout",
            callable=_spark_sql_step,
        ),
        ExampleBlockRequest(
            slug="duckdb-sql-code",
            language="python",
            source="callable",
            callable=_duckdb_sql_step,
        ),
        ExampleBlockRequest(
            slug="duckdb-sql-output",
            language="sql",
            source="stdout",
            callable=_duckdb_sql_step,
        ),
        ExampleBlockRequest(
            slug="polars-code",
            language="python",
            source="callable",
            callable=_polars_schema_step,
        ),
        ExampleBlockRequest(
            slug="polars-output",
            language="text",
            source="stdout",
            callable=_polars_schema_step,
        ),
        ExampleBlockRequest(
            slug="pyarrow-code",
            language="python",
            source="callable",
            callable=_pyarrow_schema_step,
        ),
        ExampleBlockRequest(
            slug="pyarrow-output",
            language="text",
            source="stdout",
            callable=_pyarrow_schema_step,
        ),
    ),
)


if __name__ == "__main__":
    _pyarrow_schema_step()
