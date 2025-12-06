from __future__ import annotations

import pyarrow as pa
import yads

from ..base import ExampleBlockRequest, ExampleDefinition

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none, reportMissingImports=none

ROUNDTRIP_SCHEMA = pa.schema(
    [
        pa.field(
            "id",
            pa.int64(),
            nullable=False,
            metadata={"description": "Customer ID"},
        ),
        pa.field(
            "name",
            pa.string(),
            metadata={"description": "Customer preferred name"},
        ),
        pa.field(
            "email",
            pa.string(),
            metadata={"description": "Customer email address"},
        ),
        pa.field(
            "created_at",
            pa.timestamp("ns", tz="UTC"),
            metadata={"description": "Customer creation timestamp"},
        ),
    ],
)
spec = yads.from_pyarrow(ROUNDTRIP_SCHEMA, name="catalog.crm.customers", version=1)


def _pyarrow_roundtrip_code() -> None:
    import yads
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field(
                "id",
                pa.int64(),
                nullable=False,
                metadata={"description": "Customer ID"},
            ),
            pa.field(
                "name",
                pa.string(),
                metadata={"description": "Customer preferred name"},
            ),
            pa.field(
                "email",
                pa.string(),
                metadata={"description": "Customer email address"},
            ),
            pa.field(
                "created_at",
                pa.timestamp("ns", tz="UTC"),
                metadata={"description": "Customer creation timestamp"},
            ),
        ]
    )

    spec = yads.from_pyarrow(schema, name="catalog.crm.customers", version=1)
    print(spec)


def _duckdb_step() -> None:
    duckdb_ddl = yads.to_sql(spec, dialect="duckdb", pretty=True)
    print(duckdb_ddl)


def _pyspark_step() -> None:
    pyspark_schema = yads.to_pyspark(spec)
    for field in pyspark_schema.fields:
        print(f"{field.name}, {field.dataType}, {field.nullable=}")
        print(f"{field.metadata=}\n")


EXAMPLE = ExampleDefinition(
    example_id="minimal-roundtrip-conversion",
    blocks=(
        ExampleBlockRequest(
            slug="pyarrow-code",
            language="python",
            source="callable",
            callable=_pyarrow_roundtrip_code,
        ),
        ExampleBlockRequest(
            slug="pyarrow-output",
            language="text",
            source="stdout",
            callable=_pyarrow_roundtrip_code,
        ),
        ExampleBlockRequest(
            slug="duckdb-code",
            language="python",
            source="callable",
            callable=_duckdb_step,
        ),
        ExampleBlockRequest(
            slug="duckdb-output",
            language="sql",
            source="stdout",
            callable=_duckdb_step,
        ),
        ExampleBlockRequest(
            slug="pyspark-code",
            language="python",
            source="callable",
            callable=_pyspark_step,
        ),
        ExampleBlockRequest(
            slug="pyspark-output",
            language="text",
            source="stdout",
            callable=_pyspark_step,
        ),
    ),
)
