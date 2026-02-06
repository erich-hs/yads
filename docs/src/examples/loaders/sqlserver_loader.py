"""Executable examples for loading specs from SQL Server tables."""

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


_SQLSERVER_COLUMNS: list[dict[str, Any]] = [
    {
        "column_name": "submission_id",
        "ordinal_position": 1,
        "data_type": "bigint",
        "character_maximum_length": None,
        "numeric_precision": None,
        "numeric_scale": None,
        "datetime_precision": None,
        "is_nullable": "NO",
        "column_default": None,
    },
    {
        "column_name": "completion_percent",
        "ordinal_position": 2,
        "data_type": "decimal",
        "character_maximum_length": None,
        "numeric_precision": 5,
        "numeric_scale": 2,
        "datetime_precision": None,
        "is_nullable": "YES",
        "column_default": "((0.0))",
    },
    {
        "column_name": "time_taken_seconds",
        "ordinal_position": 3,
        "data_type": "int",
        "character_maximum_length": None,
        "numeric_precision": None,
        "numeric_scale": None,
        "datetime_precision": None,
        "is_nullable": "YES",
        "column_default": None,
    },
    {
        "column_name": "submitted_at",
        "ordinal_position": 4,
        "data_type": "datetimeoffset",
        "character_maximum_length": None,
        "numeric_precision": None,
        "numeric_scale": None,
        "datetime_precision": 7,
        "is_nullable": "YES",
        "column_default": None,
    },
]

_SQLSERVER_CONSTRAINTS: list[dict[str, Any]] = [
    {
        "constraint_name": "PK_submissions",
        "constraint_type": "PRIMARY KEY",
        "column_name": "submission_id",
        "ordinal_position": 1,
        "ref_schema": None,
        "ref_table": None,
        "ref_column": None,
    }
]


def _sqlserver_responder(query: str, _params: tuple[Any, ...] | None) -> RowsResponse:
    normalized = " ".join(query.lower().split())

    if "select db_name()" in normalized:
        return _rows_from_dicts([{"current_database": "analytics"}])

    if "from information_schema.columns" in normalized:
        return _rows_from_dicts(_SQLSERVER_COLUMNS)

    if "from sys.key_constraints" in normalized:
        return _rows_from_dicts(_SQLSERVER_CONSTRAINTS)

    if "from sys.identity_columns" in normalized:
        return _rows_from_dicts([])

    if "from sys.computed_columns" in normalized:
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


class _FakeSqlServerConnection:
    def cursor(self) -> _FakeCursor:
        return _FakeCursor(_sqlserver_responder)


_fake_pyodbc = types.ModuleType("pyodbc")


def _connect_stub(*_args: Any, **_kwargs: Any) -> _FakeSqlServerConnection:
    return _FakeSqlServerConnection()


_fake_pyodbc.connect = _connect_stub
sys.modules["pyodbc"] = _fake_pyodbc


def _sqlserver_loader_lowlevel_example() -> None:
    import pyodbc
    from yads.loaders.sql import SqlLoaderConfig, SqlServerLoader

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=analytics"
    )
    loader = SqlServerLoader(conn, SqlLoaderConfig(mode="coerce"))
    spec = loader.load(
        "submissions",
        schema="dbo",
        name="prod.assessments.submissions",
        version=1,
    )
    print(spec)


EXAMPLE = ExampleDefinition(
    example_id="sqlserver-loader-basic",
    blocks=(
        ExampleBlockRequest(
            slug="loader-example-lowlevel-code",
            language="python",
            source="callable",
            callable=_sqlserver_loader_lowlevel_example,
        ),
        ExampleBlockRequest(
            slug="loader-example-lowlevel-output",
            language="text",
            source="stdout",
            callable=_sqlserver_loader_lowlevel_example,
        ),
    ),
)
