from __future__ import annotations

from typing import Any, Callable, Literal, Type, TYPE_CHECKING

from .base import BaseConverter, BaseConverterConfig
from .sql.sql_converter import (
    SQLConverter,
    SQLConverterConfig,
    SparkSQLConverter,
    DuckdbSQLConverter,
)
from .sql.ast_converter import AstConverter, SQLGlotConverter, SQLGlotConverterConfig
from .pyarrow_converter import PyArrowConverter, PyArrowConverterConfig
from .pydantic_converter import PydanticConverter, PydanticConverterConfig

__all__ = [
    "to_pyarrow",
    "to_pydantic",
    "to_sql",
    "BaseConverter",
    "BaseConverterConfig",
    "AstConverter",
    "SQLConverter",
    "SQLConverterConfig",
    "SparkSQLConverter",
    "DuckdbSQLConverter",
    "SQLGlotConverter",
    "SQLGlotConverterConfig",
    "PyArrowConverter",
    "PyArrowConverterConfig",
    "PydanticConverter",
    "PydanticConverterConfig",
]

if TYPE_CHECKING:
    import pyarrow as pa  # type: ignore[import-untyped]
    from pydantic import BaseModel  # type: ignore[import-untyped]
    from sqlglot.expressions import DataType as SQLGlotDataType
    from ..spec import YadsSpec


def to_pyarrow(
    spec: YadsSpec,
    *,
    # BaseConverterConfig options
    mode: Literal["raise", "coerce"] = "coerce",
    ignore_columns: set[str] | None = None,
    include_columns: set[str] | None = None,
    column_overrides: dict[str, Callable[[Any, Any], Any]] | None = None,
    # PyArrowConverterConfig options
    use_large_string: bool = False,
    use_large_binary: bool = False,
    use_large_list: bool = False,
    fallback_type: Any | None = None,
) -> "pa.Schema":
    """Convert a `YadsSpec` to a `pyarrow.Schema`.

    Args:
        spec: The validated yads specification to convert.
        mode: Conversion mode. "raise" raises on unsupported features;
            "coerce" adjusts with warnings. Defaults to "coerce".
        ignore_columns: Columns to exclude from conversion.
        include_columns: If provided, only these columns are included.
        column_overrides: Per-column custom conversion callables.
        use_large_string: Use `pa.large_string()` for string columns.
        use_large_binary: Use `pa.large_binary()` when binary has no fixed length.
        use_large_list: Use `pa.large_list(element)` for variable-size arrays.
        fallback_type: Fallback Arrow type used in coerce mode for unsupported types.
            When set, overrides the default built-in `pa.string()`. Defaults to None.

    Returns:
        A `pyarrow.Schema` instance.
    """
    # Import lazily to avoid importing heavy deps at module import time
    import pyarrow as pa  # type: ignore[import-untyped]

    config = PyArrowConverterConfig(
        mode=mode,
        ignore_columns=frozenset(ignore_columns) if ignore_columns else frozenset[str](),
        include_columns=frozenset(include_columns) if include_columns else None,
        column_overrides=column_overrides or {},
        use_large_string=use_large_string,
        use_large_binary=use_large_binary,
        use_large_list=use_large_list,
        fallback_type=fallback_type or pa.string(),
    )
    return PyArrowConverter(config).convert(spec)


def to_pydantic(
    spec: YadsSpec,
    *,
    # BaseConverterConfig options
    mode: Literal["raise", "coerce"] = "coerce",
    ignore_columns: set[str] | None = None,
    include_columns: set[str] | None = None,
    column_overrides: dict[str, Callable[[Any, Any], Any]] | None = None,
    # PydanticConverterConfig options
    model_name: str | None = None,
    model_config: dict[str, Any] | None = None,
    fallback_type: type | None = None,
) -> "Type[BaseModel]":
    """Convert a `YadsSpec` to a Pydantic `BaseModel` subclass.

    Args:
        spec: The validated yads specification to convert.
        mode: Conversion mode. "raise" raises on unsupported features;
            "coerce" adjusts with warnings. Defaults to "coerce".
        ignore_columns: Columns to exclude from conversion.
        include_columns: If provided, only these columns are included.
        column_overrides: Per-column custom conversion callables.
        model_name: Custom name for the generated model. When not set, the spec name is
            used as `spec.name.replace(".", "_")`. Defaults to None.
        model_config: Optional Pydantic model configuration dict. See more at
            https://docs.pydantic.dev/2.0/usage/model_config/
        fallback_type: Fallback Python type used in coerce mode for unsupported types.
            When set, overrides the default built-in `str`. Defaults to None.

    Returns:
        A dynamically generated Pydantic model class.
    """
    config = PydanticConverterConfig(
        mode=mode,
        ignore_columns=frozenset(ignore_columns) if ignore_columns else frozenset[str](),
        include_columns=frozenset(include_columns) if include_columns else None,
        column_overrides=column_overrides or {},
        model_name=model_name,
        model_config=model_config,
        fallback_type=fallback_type or str,
    )
    return PydanticConverter(config).convert(spec)


def to_sql(
    spec: YadsSpec,
    *,
    # Dialect routing
    dialect: Literal["spark", "duckdb"] = "spark",
    # BaseConverterConfig options (applied to AST converter)
    mode: Literal["raise", "coerce"] = "coerce",
    ignore_columns: set[str] | None = None,
    include_columns: set[str] | None = None,
    column_overrides: dict[str, Callable[[Any, Any], Any]] | None = None,
    # SQLGlotConverterConfig options
    if_not_exists: bool = False,
    or_replace: bool = False,
    ignore_catalog: bool = False,
    ignore_database: bool = False,
    fallback_type: "SQLGlotDataType.Type" | None = None,
    # SQL serialization options to forward to sqlglot (e.g., pretty=True)
    **sql_options: Any,
) -> str:
    """Convert a `YadsSpec` to SQL DDL.

    This facade routes to the appropriate SQL converter based on `dialect` and
    forwards AST-level options to the underlying SQLGlot-based converter.

    Args:
        spec: The validated yads specification to convert.
        dialect: Target dialect. Supported: "spark", "duckdb".
        mode: Conversion mode. "raise" or "coerce". Defaults to "coerce".
        ignore_columns: Columns to exclude from conversion.
        include_columns: If provided, only these columns are included.
        column_overrides: Per-column custom AST conversion callables.
        if_not_exists: Emit CREATE TABLE IF NOT EXISTS.
        or_replace: Emit CREATE OR REPLACE TABLE.
        ignore_catalog: Omit catalog from fully qualified table names.
        ignore_database: Omit database from fully qualified table names.
        fallback_type: Fallback SQL data type used in coerce mode for unsupported types.
        **sql_options: Additional formatting options forwarded to sqlglot's `sql()`.

    Returns:
        SQL DDL string for a CREATE TABLE statement.

    Raises:
        ValueError: If an unsupported dialect is provided.
    """
    from sqlglot import expressions as exp  # type: ignore[import-untyped]

    glot_fallback = fallback_type if fallback_type is not None else exp.DataType.Type.TEXT
    ast_config = SQLGlotConverterConfig(
        mode=mode,
        ignore_columns=frozenset(ignore_columns) if ignore_columns else frozenset[str](),
        include_columns=frozenset(include_columns) if include_columns else None,
        column_overrides=column_overrides or {},
        if_not_exists=if_not_exists,
        or_replace=or_replace,
        ignore_catalog=ignore_catalog,
        ignore_database=ignore_database,
        fallback_type=glot_fallback,
    )

    converter: SQLConverter
    match dialect:
        case "spark":
            converter = SparkSQLConverter(mode=mode, ast_config=ast_config)
        case "duckdb":
            converter = DuckdbSQLConverter(mode=mode, ast_config=ast_config)
        case _:
            raise ValueError("Unsupported SQL dialect. Expected 'spark' or 'duckdb'.")

    return converter.convert(spec, **sql_options)
