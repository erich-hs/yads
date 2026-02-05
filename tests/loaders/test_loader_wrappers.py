from __future__ import annotations

from typing import Any

from yads.loaders import (
    from_dict,
    from_polars,
    from_postgresql,
    from_pyarrow,
    from_pyspark,
    from_sqlserver,
)
from yads.types import String


def test_from_dict_uses_dict_loader(monkeypatch) -> None:
    state: dict[str, Any] = {}

    class DummyLoader:
        def load(self, data: dict[str, Any]) -> str:
            state["data"] = data
            return "spec"

    monkeypatch.setattr("yads.loaders.DictLoader", DummyLoader)

    payload = {"name": "test", "version": 1, "columns": []}
    result = from_dict(payload)

    assert result == "spec"
    assert state["data"] == payload


def test_from_pyarrow_builds_config_and_calls_loader(monkeypatch) -> None:
    import yads.loaders.pyarrow_loader as pyarrow_loader

    state: dict[str, Any] = {}

    class DummyConfig:
        def __init__(self, *, mode: str, fallback_type: Any | None) -> None:
            self.mode = mode
            self.fallback_type = fallback_type

    class DummyLoader:
        def __init__(self, config: DummyConfig) -> None:
            state["config"] = config

        def load(
            self,
            schema: Any,
            *,
            name: str,
            version: int,
            description: str | None = None,
        ) -> str:
            state["load"] = {
                "schema": schema,
                "name": name,
                "version": version,
                "description": description,
            }
            return "spec"

    monkeypatch.setattr(pyarrow_loader, "PyArrowLoaderConfig", DummyConfig)
    monkeypatch.setattr(pyarrow_loader, "PyArrowLoader", DummyLoader)

    schema = object()
    fallback = String()
    result = from_pyarrow(
        schema,
        mode="raise",
        fallback_type=fallback,
        name="spec.name",
        version=2,
        description="Example",
    )

    assert result == "spec"
    assert state["config"].mode == "raise"
    assert state["config"].fallback_type is fallback
    assert state["load"] == {
        "schema": schema,
        "name": "spec.name",
        "version": 2,
        "description": "Example",
    }


def test_from_pyspark_builds_config_and_calls_loader(monkeypatch) -> None:
    import yads.loaders.pyspark_loader as pyspark_loader

    state: dict[str, Any] = {}

    class DummyConfig:
        def __init__(self, *, mode: str, fallback_type: Any | None) -> None:
            self.mode = mode
            self.fallback_type = fallback_type

    class DummyLoader:
        def __init__(self, config: DummyConfig) -> None:
            state["config"] = config

        def load(
            self,
            schema: Any,
            *,
            name: str,
            version: int,
            description: str | None = None,
        ) -> str:
            state["load"] = {
                "schema": schema,
                "name": name,
                "version": version,
                "description": description,
            }
            return "spec"

    monkeypatch.setattr(pyspark_loader, "PySparkLoaderConfig", DummyConfig)
    monkeypatch.setattr(pyspark_loader, "PySparkLoader", DummyLoader)

    schema = object()
    fallback = String()
    result = from_pyspark(
        schema,
        mode="coerce",
        fallback_type=fallback,
        name="spec.name",
        version=3,
        description=None,
    )

    assert result == "spec"
    assert state["config"].mode == "coerce"
    assert state["config"].fallback_type is fallback
    assert state["load"] == {
        "schema": schema,
        "name": "spec.name",
        "version": 3,
        "description": None,
    }


def test_from_polars_builds_config_and_calls_loader(monkeypatch) -> None:
    import yads.loaders.polars_loader as polars_loader

    state: dict[str, Any] = {}

    class DummyConfig:
        def __init__(self, *, mode: str, fallback_type: Any | None) -> None:
            self.mode = mode
            self.fallback_type = fallback_type

    class DummyLoader:
        def __init__(self, config: DummyConfig) -> None:
            state["config"] = config

        def load(
            self,
            schema: Any,
            *,
            name: str,
            version: int,
            description: str | None = None,
        ) -> str:
            state["load"] = {
                "schema": schema,
                "name": name,
                "version": version,
                "description": description,
            }
            return "spec"

    monkeypatch.setattr(polars_loader, "PolarsLoaderConfig", DummyConfig)
    monkeypatch.setattr(polars_loader, "PolarsLoader", DummyLoader)

    schema = object()
    result = from_polars(
        schema,
        mode="coerce",
        fallback_type=None,
        name="spec.name",
        version=4,
        description="Polars",
    )

    assert result == "spec"
    assert state["config"].mode == "coerce"
    assert state["config"].fallback_type is None
    assert state["load"] == {
        "schema": schema,
        "name": "spec.name",
        "version": 4,
        "description": "Polars",
    }


def test_from_postgresql_builds_config_and_calls_loader(monkeypatch) -> None:
    import yads.loaders.sql.postgres_loader as postgres_loader

    state: dict[str, Any] = {}

    class DummyLoader:
        def __init__(self, connection: Any, config: Any) -> None:
            state["connection"] = connection
            state["config"] = config

        def load(
            self,
            table_name: str,
            *,
            schema: str,
            name: str | None,
            version: int,
            description: str | None,
        ) -> str:
            state["load"] = {
                "table_name": table_name,
                "schema": schema,
                "name": name,
                "version": version,
                "description": description,
            }
            return "spec"

    monkeypatch.setattr(postgres_loader, "PostgreSqlLoader", DummyLoader)

    connection = object()
    fallback = String()
    result = from_postgresql(
        connection,
        "users",
        schema="public",
        mode="coerce",
        fallback_type=fallback,
        name="public.users",
        version=2,
        description="Users table",
    )

    assert result == "spec"
    assert state["connection"] is connection
    assert state["config"].mode == "coerce"
    assert state["config"].fallback_type is fallback
    assert state["load"] == {
        "table_name": "users",
        "schema": "public",
        "name": "public.users",
        "version": 2,
        "description": "Users table",
    }


def test_from_sqlserver_builds_config_and_calls_loader(monkeypatch) -> None:
    import yads.loaders.sql.sqlserver_loader as sqlserver_loader

    state: dict[str, Any] = {}

    class DummyLoader:
        def __init__(self, connection: Any, config: Any) -> None:
            state["connection"] = connection
            state["config"] = config

        def load(
            self,
            table_name: str,
            *,
            schema: str,
            name: str | None,
            version: int,
            description: str | None,
        ) -> str:
            state["load"] = {
                "table_name": table_name,
                "schema": schema,
                "name": name,
                "version": version,
                "description": description,
            }
            return "spec"

    monkeypatch.setattr(sqlserver_loader, "SqlServerLoader", DummyLoader)

    connection = object()
    fallback = String()
    result = from_sqlserver(
        connection,
        "users",
        schema="dbo",
        mode="raise",
        fallback_type=fallback,
        name="dbo.users",
        version=5,
        description=None,
    )

    assert result == "spec"
    assert state["connection"] is connection
    assert state["config"].mode == "raise"
    assert state["config"].fallback_type is fallback
    assert state["load"] == {
        "table_name": "users",
        "schema": "dbo",
        "name": "dbo.users",
        "version": 5,
        "description": None,
    }
