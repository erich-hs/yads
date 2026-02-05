from __future__ import annotations

import warnings
from typing import Any

import pytest

from yads.exceptions import UnsupportedFeatureError, ValidationWarning
from yads.loaders.sql.base import SqlLoader, SqlLoaderConfig, safe_int
from yads.types import String


class DummyCursor:
    def __init__(self, rows: list[tuple[Any, ...]], description: list[tuple[str]] | None):
        self._rows = rows
        self.description = description
        self.calls: list[tuple[Any, ...]] = []
        self.closed = False
        self.fetchall_called = False

    def execute(self, *args: Any) -> None:
        self.calls.append(args)

    def fetchall(self) -> list[tuple[Any, ...]]:
        self.fetchall_called = True
        return self._rows

    def close(self) -> None:
        self.closed = True


class DummyConnection:
    def __init__(self, cursor: DummyCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> DummyCursor:
        return self._cursor


class DummySqlLoader(SqlLoader):
    def load(self, *args: Any, **kwargs: Any):
        raise NotImplementedError


def test_execute_query_without_params_returns_rows() -> None:
    cursor = DummyCursor(rows=[(1, "Ada")], description=[("id",), ("name",)])
    loader = DummySqlLoader(DummyConnection(cursor))

    result = loader._execute_query("select * from users")

    assert result == [{"id": 1, "name": "Ada"}]
    assert cursor.calls == [("select * from users",)]
    assert cursor.fetchall_called is True
    assert cursor.closed is True


def test_execute_query_with_params_handles_no_description() -> None:
    cursor = DummyCursor(rows=[], description=None)
    loader = DummySqlLoader(DummyConnection(cursor))

    result = loader._execute_query("select * from users where id = ?", params=(1,))

    assert result == []
    assert cursor.calls == [("select * from users where id = ?", (1,))]
    assert cursor.fetchall_called is False
    assert cursor.closed is True


def test_raise_or_coerce_in_coerce_mode_returns_fallback_and_warns() -> None:
    config = SqlLoaderConfig(mode="coerce", fallback_type=String())
    loader = DummySqlLoader(DummyConnection(DummyCursor([], None)), config)

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        with loader.load_context(field="amount"):
            result = loader.raise_or_coerce("money")

    assert result is config.fallback_type
    assert any(isinstance(w.message, ValidationWarning) for w in recorded)
    assert any("field 'amount'" in str(w.message) for w in recorded)


def test_raise_or_coerce_without_fallback_raises() -> None:
    config = SqlLoaderConfig(mode="coerce", fallback_type=None)
    loader = DummySqlLoader(DummyConnection(DummyCursor([], None)), config)

    with pytest.raises(UnsupportedFeatureError, match="fallback_type"):
        loader.raise_or_coerce("money")


def test_raise_or_coerce_in_raise_mode_raises() -> None:
    config = SqlLoaderConfig(mode="raise", fallback_type=String())
    loader = DummySqlLoader(DummyConnection(DummyCursor([], None)), config)

    with pytest.raises(
        UnsupportedFeatureError,
        match="Unsupported database type 'money'",
    ):
        loader.raise_or_coerce("money")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("5", 5),
        ("not-a-number", None),
    ],
)
def test_safe_int(value: Any, expected: int | None) -> None:
    assert safe_int(value) == expected
