from __future__ import annotations

import pytest

from yads.converters import (
    to_pyarrow,
    to_pydantic,
    to_pyspark,
    to_polars,
    to_sql,
)
from yads.spec import Column, YadsSpec
from yads.types import Integer


@pytest.fixture()
def simple_spec() -> YadsSpec:
    """Create a minimal spec for facade wrapper tests."""

    return YadsSpec(
        name="catalog.db.table",
        version=1,
        columns=[Column(name="id", type=Integer())],
    )


def test_to_pyarrow_passes_column_overrides(
    monkeypatch: pytest.MonkeyPatch, simple_spec: YadsSpec
) -> None:
    """`to_pyarrow` should accept column overrides without importing PyArrow eagerly."""

    state: dict[str, object] = {}

    class DummyPyArrowConverter:
        def __init__(self, config):
            state["config"] = config

        def convert(self, spec: YadsSpec):
            state["spec"] = spec
            return "pyarrow-schema"

    monkeypatch.setattr(
        "yads.converters.pyarrow_converter.PyArrowConverter",
        DummyPyArrowConverter,
    )

    def override(field, converter):  # pragma: no cover - trivial callable
        return field, converter

    result = to_pyarrow(
        simple_spec,
        column_overrides={"id": override},
        ignore_columns={"skip_me"},
        include_columns={"id"},
    )

    assert result == "pyarrow-schema"
    assert state["spec"] is simple_spec

    config = state["config"]
    assert isinstance(config.ignore_columns, frozenset)
    assert config.ignore_columns == frozenset({"skip_me"})
    assert isinstance(config.include_columns, frozenset)
    assert config.include_columns == frozenset({"id"})
    assert dict(config.column_overrides) == {"id": override}


def test_to_pydantic_passes_column_overrides(
    monkeypatch: pytest.MonkeyPatch, simple_spec: YadsSpec
) -> None:
    """`to_pydantic` should forward overrides into the converter config."""

    state: dict[str, object] = {}

    class DummyPydanticConverter:
        def __init__(self, config):
            state["config"] = config

        def convert(self, spec: YadsSpec):
            state["spec"] = spec
            return "PydanticModel"

    monkeypatch.setattr("yads.converters.PydanticConverter", DummyPydanticConverter)

    def override(field, converter):  # pragma: no cover - trivial callable
        return field, converter

    result = to_pydantic(simple_spec, column_overrides={"id": override})

    assert result == "PydanticModel"
    assert state["spec"] is simple_spec
    assert dict(state["config"].column_overrides) == {"id": override}


def test_to_pyspark_passes_column_overrides(
    monkeypatch: pytest.MonkeyPatch, simple_spec: YadsSpec
) -> None:
    """`to_pyspark` should forward overrides and return the converter result."""

    state: dict[str, object] = {}

    class DummyPySparkConverter:
        def __init__(self, config):
            state["config"] = config

        def convert(self, spec: YadsSpec):
            state["spec"] = spec
            return "StructType"

    monkeypatch.setattr(
        "yads.converters.pyspark_converter.PySparkConverter",
        DummyPySparkConverter,
    )

    def override(field, converter):  # pragma: no cover - trivial callable
        return field, converter

    result = to_pyspark(simple_spec, column_overrides={"id": override})

    assert result == "StructType"
    assert state["spec"] is simple_spec
    assert dict(state["config"].column_overrides) == {"id": override}


def test_to_polars_passes_column_overrides(
    monkeypatch: pytest.MonkeyPatch, simple_spec: YadsSpec
) -> None:
    """`to_polars` should forward overrides and return the converter result."""

    state: dict[str, object] = {}

    class DummyPolarsConverter:
        def __init__(self, config):
            state["config"] = config

        def convert(self, spec: YadsSpec):
            state["spec"] = spec
            return "polars-schema"

    monkeypatch.setattr(
        "yads.converters.polars_converter.PolarsConverter",
        DummyPolarsConverter,
    )

    def override(field, converter):  # pragma: no cover - trivial callable
        return field, converter

    result = to_polars(simple_spec, column_overrides={"id": override})

    assert result == "polars-schema"
    assert state["spec"] is simple_spec
    assert dict(state["config"].column_overrides) == {"id": override}


def test_to_sql_routes_and_passes_overrides(
    monkeypatch: pytest.MonkeyPatch, simple_spec: YadsSpec
) -> None:
    """`to_sql` should build the AST config once and route to the proper converter."""

    state: dict[str, object] = {}

    class DummySparkSQLConverter:
        def __init__(self, *, mode="coerce", ast_config):
            state["mode"] = mode
            state["ast_config"] = ast_config

        def convert(self, spec: YadsSpec, **sql_options):
            state["spec"] = spec
            state["sql_options"] = sql_options
            return "spark-sql"

    class DummyDuckdbSQLConverter:
        def __init__(self, *, mode="coerce", ast_config):
            self.mode = mode
            self.ast_config = ast_config

        def convert(self, spec: YadsSpec, **sql_options):
            return "duckdb-sql"

    monkeypatch.setattr(
        "yads.converters.sql.sql_converter.SparkSQLConverter",
        DummySparkSQLConverter,
    )
    monkeypatch.setattr(
        "yads.converters.sql.sql_converter.DuckdbSQLConverter",
        DummyDuckdbSQLConverter,
    )

    def override(field, converter):  # pragma: no cover - trivial callable
        return field, converter

    result = to_sql(
        simple_spec,
        dialect="spark",
        column_overrides={"id": override},
        if_not_exists=True,
        ignore_catalog=True,
        fallback_type=None,
        pretty=True,
    )

    assert result == "spark-sql"
    assert state["spec"] is simple_spec
    assert state["mode"] == "coerce"
    assert state["sql_options"] == {"pretty": True}

    ast_config = state["ast_config"]
    assert ast_config.if_not_exists is True
    assert ast_config.ignore_catalog is True
    assert dict(ast_config.column_overrides) == {"id": override}
