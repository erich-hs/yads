"""Executable examples for loading specs from PostgreSQL tables."""

from __future__ import annotations

from typing import Any, Callable
import sys
import types

from ..base import ExampleBlockRequest, ExampleDefinition

RowsResponse = tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]
Responder = Callable[[str, tuple[Any, ...] | None], RowsResponse]


def _rows_from_dicts(rows: list[dict[str, Any]]) -> RowsResponse:
    if not rows:
        return [], []
    columns = list(rows[0].keys())
    description = [(name, None, None, None, None, None, None) for name in columns]
    data = [tuple(row.get(name) for name in columns) for row in rows]
    return data, description


_POSTGRES_COLUMNS: list[dict[str, Any]] = [
    {
        "column_name": "submission_id",
        "ordinal_position": 1,
        "data_type": "bigint",
        "udt_name": "int8",
        "character_maximum_length": None,
        "numeric_precision": 64,
        "numeric_scale": 0,
        "datetime_precision": None,
        "interval_type": None,
        "is_nullable": "NO",
        "column_default": None,
        "is_identity": "NO",
        "identity_generation": None,
        "identity_start": None,
        "identity_increment": None,
        "is_generated": "NEVER",
        "generation_expression": None,
    },
    {
        "column_name": "completion_percent",
        "ordinal_position": 2,
        "data_type": "numeric",
        "udt_name": "numeric",
        "character_maximum_length": None,
        "numeric_precision": 5,
        "numeric_scale": 2,
        "datetime_precision": None,
        "interval_type": None,
        "is_nullable": "YES",
        "column_default": "0.0",
        "is_identity": "NO",
        "identity_generation": None,
        "identity_start": None,
        "identity_increment": None,
        "is_generated": "NEVER",
        "generation_expression": None,
    },
    {
        "column_name": "time_taken_seconds",
        "ordinal_position": 3,
        "data_type": "integer",
        "udt_name": "int4",
        "character_maximum_length": None,
        "numeric_precision": 32,
        "numeric_scale": 0,
        "datetime_precision": None,
        "interval_type": None,
        "is_nullable": "YES",
        "column_default": None,
        "is_identity": "NO",
        "identity_generation": None,
        "identity_start": None,
        "identity_increment": None,
        "is_generated": "NEVER",
        "generation_expression": None,
    },
    {
        "column_name": "submitted_at",
        "ordinal_position": 4,
        "data_type": "timestamp with time zone",
        "udt_name": "timestamptz",
        "character_maximum_length": None,
        "numeric_precision": None,
        "numeric_scale": None,
        "datetime_precision": 6,
        "interval_type": None,
        "is_nullable": "YES",
        "column_default": None,
        "is_identity": "NO",
        "identity_generation": None,
        "identity_start": None,
        "identity_increment": None,
        "is_generated": "NEVER",
        "generation_expression": None,
    },
]

_POSTGRES_CONSTRAINTS: list[dict[str, Any]] = [
    {
        "constraint_name": "submissions_pkey",
        "constraint_type": "PRIMARY KEY",
        "column_name": "submission_id",
        "ordinal_position": 1,
        "ref_schema": None,
        "ref_table": None,
        "ref_column": None,
    }
]


def _postgres_responder(query: str, _params: tuple[Any, ...] | None) -> RowsResponse:
    normalized = " ".join(query.lower().split())

    if "select current_database()" in normalized:
        return _rows_from_dicts([{"current_database": "analytics"}])

    if "from information_schema.columns" in normalized:
        return _rows_from_dicts(_POSTGRES_COLUMNS)

    if "from information_schema.table_constraints" in normalized:
        return _rows_from_dicts(_POSTGRES_CONSTRAINTS)

    if "from pg_catalog.pg_attribute" in normalized and "typcategory = 'a'" in normalized:
        return _rows_from_dicts([])

    if "from pg_catalog.pg_class c" in normalized and "pg_sequence" in normalized:
        return _rows_from_dicts([])

    if "from pg_catalog.pg_type ct" in normalized:
        return _rows_from_dicts([])

    if "from pg_catalog.pg_type d" in normalized and "typtype = 'd'" in normalized:
        return _rows_from_dicts([])

    return _rows_from_dicts([])


class _FakeCursor:
    def __init__(self, responder: Responder) -> None:
        self._responder = responder
        self.description: list[tuple[Any, ...]] | None = None
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        rows, description = self._responder(query, params)
        self._rows = rows
        self.description = description

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        return None


class _FakePostgresConnection:
    def cursor(self) -> _FakeCursor:
        return _FakeCursor(_postgres_responder)


_fake_psycopg2 = types.ModuleType("psycopg2")


def _connect_stub(*_args: Any, **_kwargs: Any) -> _FakePostgresConnection:
    return _FakePostgresConnection()


_fake_psycopg2.connect = _connect_stub
sys.modules["psycopg2"] = _fake_psycopg2


def _postgres_loader_lowlevel_example() -> None:
    import psycopg2
    from yads.loaders.sql import PostgreSqlLoader, SqlLoaderConfig

    conn = psycopg2.connect("postgresql://localhost/analytics")
    loader = PostgreSqlLoader(conn, SqlLoaderConfig(mode="coerce"))
    spec = loader.load(
        "submissions",
        schema="public",
        name="prod.assessments.submissions",
        version=1,
    )
    print(spec)


EXAMPLE = ExampleDefinition(
    example_id="postgres-loader-basic",
    blocks=(
        ExampleBlockRequest(
            slug="loader-example-lowlevel-code",
            language="python",
            source="callable",
            callable=_postgres_loader_lowlevel_example,
        ),
        ExampleBlockRequest(
            slug="loader-example-lowlevel-output",
            language="text",
            source="stdout",
            callable=_postgres_loader_lowlevel_example,
        ),
    ),
)
