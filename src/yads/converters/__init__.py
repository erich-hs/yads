from __future__ import annotations

from typing import Any, Callable, Literal, Mapping, Type, TYPE_CHECKING

from .._dependencies import requires_dependency
from .base import BaseConverter, BaseConverterConfig
from .pydantic_converter import PydanticConverter, PydanticConverterConfig


def __getattr__(name: str):
    if name in ("PyArrowConverter", "PyArrowConverterConfig"):
        from . import pyarrow_converter

        return getattr(pyarrow_converter, name)
    if name in ("PySparkConverter", "PySparkConverterConfig"):
        from . import pyspark_converter

        return getattr(pyspark_converter, name)
    if name in ("PolarsConverter", "PolarsConverterConfig"):
        from . import polars_converter

        return getattr(polars_converter, name)
    if name in (
        "AstConverter",
        "SQLGlotConverter",
        "SQLGlotConverterConfig",
        "SQLConverter",
        "SQLConverterConfig",
        "SparkSQLConverter",
        "DuckdbSQLConverter",
    ):
        from . import sql

        return getattr(sql, name)
    raise AttributeError(name)


__all__ = [
    "to_pyarrow",
    "to_pydantic",
    "to_pyspark",
    "to_polars",
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
    "PySparkConverter",
    "PySparkConverterConfig",
    "PolarsConverter",
    "PolarsConverterConfig",
]

if TYPE_CHECKING:
    import pyarrow as pa  # type: ignore[import-untyped]
    import polars as pl  # type: ignore[import-untyped]
    from pydantic import BaseModel  # type: ignore[import-untyped]
    from pydantic.fields import FieldInfo  # type: ignore[import-untyped]
    from pyspark.sql.types import (
        StructType,
        StructField,
        DataType,
    )  # type: ignore[import-untyped]
    from sqlglot import expressions as exp  # type: ignore[import-untyped]
    from .pyarrow_converter import PyArrowConverter
    from .pyspark_converter import PySparkConverter
    from .polars_converter import PolarsConverter
    from .sql.ast_converter import AstConverter, SQLGlotConverter, SQLGlotConverterConfig
    from .sql.sql_converter import (
        SQLConverter,
        SQLConverterConfig,
        SparkSQLConverter,
        DuckdbSQLConverter,
    )
    from ..spec import YadsSpec, Field as SpecField

    # Column override type aliases (for static typing only)
    PyArrowColumnOverride = Callable[[SpecField, PyArrowConverter], pa.Field]
    PydanticColumnOverride = Callable[
        [SpecField, PydanticConverter], tuple[Any, FieldInfo]
    ]
    PySparkColumnOverride = Callable[[SpecField, PySparkConverter], StructField]
    PolarsColumnOverride = Callable[[SpecField, PolarsConverter], pl.Field]
    SQLGlotColumnOverride = Callable[[SpecField, SQLGlotConverter], exp.ColumnDef]


@requires_dependency("pyarrow", min_version="15.0.0", import_name="pyarrow")
def to_pyarrow(
    spec: YadsSpec,
    *,
    # BaseConverterConfig options
    mode: Literal["raise", "coerce"] = "coerce",
    ignore_columns: set[str] | None = None,
    include_columns: set[str] | None = None,
    column_overrides: Mapping[str, PyArrowColumnOverride] | None = None,
    # PyArrowConverterConfig options
    use_large_string: bool = False,
    use_large_binary: bool = False,
    use_large_list: bool = False,
    fallback_type: pa.DataType | None = None,
) -> pa.Schema:
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
    from . import pyarrow_converter

    config = pyarrow_converter.PyArrowConverterConfig(
        mode=mode,
        ignore_columns=frozenset(ignore_columns) if ignore_columns else frozenset[str](),
        include_columns=frozenset(include_columns) if include_columns else None,
        column_overrides=column_overrides or {},
        use_large_string=use_large_string,
        use_large_binary=use_large_binary,
        use_large_list=use_large_list,
        fallback_type=fallback_type,
    )
    return pyarrow_converter.PyArrowConverter(config).convert(spec)


@requires_dependency("pydantic", min_version="2.0.0", import_name="pydantic")
def to_pydantic(
    spec: YadsSpec,
    *,
    # BaseConverterConfig options
    mode: Literal["raise", "coerce"] = "coerce",
    ignore_columns: set[str] | None = None,
    include_columns: set[str] | None = None,
    column_overrides: Mapping[str, PydanticColumnOverride] | None = None,
    # PydanticConverterConfig options
    model_name: str | None = None,
    model_config: dict[str, Any] | None = None,
    fallback_type: type[str] | type[dict] | type[bytes] | None = None,
) -> Type[BaseModel]:
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
        fallback_type=fallback_type,
    )
    return PydanticConverter(config).convert(spec)


@requires_dependency("pyspark", min_version="3.1.1", import_name="pyspark.sql.types")
def to_pyspark(
    spec: YadsSpec,
    *,
    # BaseConverterConfig options
    mode: Literal["raise", "coerce"] = "coerce",
    ignore_columns: set[str] | None = None,
    include_columns: set[str] | None = None,
    column_overrides: Mapping[str, PySparkColumnOverride] | None = None,
    # PySparkConverterConfig options
    fallback_type: DataType | None = None,
) -> StructType:
    """Convert a `YadsSpec` to a PySpark `StructType`.

    Args:
        spec: The validated yads specification to convert.
        mode: Conversion mode. "raise" raises on unsupported features;
            "coerce" adjusts with warnings. Defaults to "coerce".
        ignore_columns: Columns to exclude from conversion.
        include_columns: If provided, only these columns are included.
        column_overrides: Per-column custom conversion callables.
        fallback_type: Fallback PySpark data type used in coerce mode for unsupported types.
            When set, overrides the default built-in `StringType()`. Defaults to None.

    Returns:
        A PySpark `StructType` instance.
    """
    from . import pyspark_converter

    config = pyspark_converter.PySparkConverterConfig(
        mode=mode,
        ignore_columns=frozenset(ignore_columns) if ignore_columns else frozenset[str](),
        include_columns=frozenset(include_columns) if include_columns else None,
        column_overrides=column_overrides or {},
        fallback_type=fallback_type,
    )
    return pyspark_converter.PySparkConverter(config).convert(spec)


@requires_dependency("polars", min_version="1.0.0", import_name="polars")
def to_polars(
    spec: YadsSpec,
    *,
    # BaseConverterConfig options
    mode: Literal["raise", "coerce"] = "coerce",
    ignore_columns: set[str] | None = None,
    include_columns: set[str] | None = None,
    column_overrides: Mapping[str, PolarsColumnOverride] | None = None,
    # PolarsConverterConfig options
    fallback_type: pl.DataType | None = None,
) -> pl.Schema:
    """Convert a `YadsSpec` to a `polars.Schema`.

    Args:
        spec: The validated yads specification to convert.
        mode: Conversion mode. "raise" raises on unsupported features;
            "coerce" adjusts with warnings. Defaults to "coerce".
        ignore_columns: Columns to exclude from conversion.
        include_columns: If provided, only these columns are included.
        column_overrides: Per-column custom conversion callables.
        fallback_type: Fallback Polars data type used in coerce mode for unsupported types.
            When set, overrides the default built-in `pl.String`. Defaults to None.

    Returns:
        A `polars.Schema` instance.
    """
    from . import polars_converter

    config = polars_converter.PolarsConverterConfig(
        mode=mode,
        ignore_columns=frozenset(ignore_columns) if ignore_columns else frozenset[str](),
        include_columns=frozenset(include_columns) if include_columns else None,
        column_overrides=column_overrides or {},
        fallback_type=fallback_type,
    )
    return polars_converter.PolarsConverter(config).convert(spec)


@requires_dependency("sqlglot", min_version="27.0.0", import_name="sqlglot")
def to_sql(
    spec: YadsSpec,
    *,
    # Dialect routing
    dialect: Literal["spark", "duckdb"] = "spark",
    # BaseConverterConfig options (applied to AST converter)
    mode: Literal["raise", "coerce"] = "coerce",
    ignore_columns: set[str] | None = None,
    include_columns: set[str] | None = None,
    column_overrides: Mapping[str, SQLGlotColumnOverride] | None = None,
    # SQLGlotConverterConfig options
    if_not_exists: bool = False,
    or_replace: bool = False,
    ignore_catalog: bool = False,
    ignore_database: bool = False,
    fallback_type: Any = None,
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
            Defaults to None.
        **sql_options: Additional formatting options forwarded to sqlglot's `sql()`.

    Returns:
        SQL DDL string for a CREATE TABLE statement.

    Raises:
        ValueError: If an unsupported dialect is provided.
    """
    from .sql.ast_converter import SQLGlotConverterConfig
    from .sql.sql_converter import SparkSQLConverter, DuckdbSQLConverter

    ast_config = SQLGlotConverterConfig(
        mode=mode,
        ignore_columns=frozenset(ignore_columns) if ignore_columns else frozenset[str](),
        include_columns=frozenset(include_columns) if include_columns else None,
        column_overrides=column_overrides or {},
        if_not_exists=if_not_exists,
        or_replace=or_replace,
        ignore_catalog=ignore_catalog,
        ignore_database=ignore_database,
        fallback_type=fallback_type,
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
