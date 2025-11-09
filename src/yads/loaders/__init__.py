"""Entry points for loading `YadsSpec` from various sources.

This module provides simple functions for loading a `YadsSpec` from common
formats:

- `from_yaml_string`: Load from YAML content provided as a string.
- `from_yaml_path`: Load from a filesystem path to a YAML file.
- `from_yaml_stream`: Load from a file-like stream (text or binary).
- `from_yaml`: Convenience loader that accepts a path (`str` or
  `pathlib.Path`) or a file-like stream. It does not accept arbitrary
  content strings.

All functions return a validated immutable `YadsSpec` instance.
"""

from __future__ import annotations

from pathlib import Path
from typing import IO, Any, cast, Literal, TYPE_CHECKING

from .base import BaseLoader, BaseLoaderConfig, ConfigurableLoader, DictLoader
from .yaml_loader import YamlLoader

if TYPE_CHECKING:
    from ..spec import YadsSpec
    from ..types import YadsType


def __getattr__(name: str):
    if name in ("PyArrowLoader", "PyArrowLoaderConfig"):
        from . import pyarrow_loader

        return getattr(pyarrow_loader, name)
    if name in ("PySparkLoader", "PySparkLoaderConfig"):
        from . import pyspark_loader

        return getattr(pyspark_loader, name)
    if name in ("PolarsLoader", "PolarsLoaderConfig"):
        from . import polars_loader

        return getattr(polars_loader, name)
    raise AttributeError(name)


__all__ = [
    "from_dict",
    "from_yaml_string",
    "from_yaml_path",
    "from_yaml_stream",
    "from_yaml",
    "from_pyarrow",
    "from_pyspark",
    "from_polars",
    "BaseLoader",
    "BaseLoaderConfig",
    "ConfigurableLoader",
    "DictLoader",
    "YamlLoader",
    "PyArrowLoader",
    "PyArrowLoaderConfig",
    "PySparkLoader",
    "PySparkLoaderConfig",
    "PolarsLoader",
    "PolarsLoaderConfig",
]


def from_dict(data: dict[str, Any]) -> YadsSpec:
    """Load a `YadsSpec` from a dictionary.

    Args:
        data: The dictionary representation of the spec.

    Returns:
        A validated immutable `YadsSpec` instance.

    Example:
        >>> data = {
        ...     "name": "users",
        ...     "version": "1.0.0",
        ...     "columns": [
        ...         {
        ...             "name": "id",
        ...             "type": "integer",
        ...         },
        ...         {
        ...             "name": "email",
        ...             "type": "string",
        ...         }
        ...     ]
        ... }
        >>> spec = from_dict(data)
        >>> print(f"Loaded spec: {spec.name} v{spec.version}")
        Loaded spec: users v1.0.0
    """
    return DictLoader().load(data)


def from_yaml_string(content: str) -> YadsSpec:
    """Load a spec from YAML string content.

    Args:
        content: YAML content as a string.

    Returns:
        A validated immutable `YadsSpec` instance.

    Example:
        >>> content = \"""
        ... name: users
        ... version: 1.0.0
        ... columns:
        ...   - name: id
        ...     type: integer
        ...   - name: email
        ...     type: string
        ... \"""
        >>> spec = from_yaml_string(content)
        >>> print(f"Loaded spec: {spec.name} v{spec.version}")
        Loaded spec: users v1.0.0
    """
    return YamlLoader().load(content)


def from_yaml_path(path: str | Path, *, encoding: str = "utf-8") -> YadsSpec:
    """Load a spec from a YAML file path.

    Args:
        path: Filesystem path to a YAML file.
        encoding: Text encoding used to read the file.

    Returns:
        A validated immutable `YadsSpec` instance.

    Raises:
        FileNotFoundError: If the file does not exist.

    Example:
        >>> spec = from_yaml_path("specs/users.yaml")
        >>> print(f"Loaded spec: {spec.name} v{spec.version}")
        Loaded spec: users v1.0.0
    """
    text = Path(path).read_text(encoding=encoding)
    return YamlLoader().load(text)


def from_yaml_stream(stream: IO[str] | IO[bytes], *, encoding: str = "utf-8") -> YadsSpec:
    """Load a spec from a file-like stream.

    The stream is not closed by this function.

    Args:
        stream: File-like object opened in text or binary mode.
        encoding: Used only if `stream` is binary.

    Returns:
        A validated immutable `YadsSpec` instance.

    Example:
        >>> with open("specs/users.yaml", "r") as f:
        ...     spec = from_yaml_stream(f)
        >>> print(f"Loaded spec: {spec.name} v{spec.version}")
        Loaded spec: users v1.0.0
    """
    raw = stream.read()
    text = raw.decode(encoding) if isinstance(raw, (bytes, bytearray)) else raw
    return YamlLoader().load(text)


def from_yaml(
    source: str | Path | IO[str] | IO[bytes], *, encoding: str = "utf-8"
) -> YadsSpec:
    """Load a spec from a path or a file-like stream.

    This convenience loader avoids ambiguity by not accepting arbitrary content
    strings. Pass content strings to `from_yaml_string` instead.

    Args:
        source: A filesystem path (`str` or `pathlib.Path`) or a file-like
            object opened in text or binary mode.
        encoding: Text encoding used when reading files or decoding binary
            streams.

    Returns:
        A validated immutable `YadsSpec` instance.

    Example:
        >>> spec = from_yaml("specs/users.yaml")
        >>> print(f"Loaded spec: {spec.name} v{spec.version}")
        Loaded spec: users v1.0.0
    """
    if hasattr(source, "read"):
        return from_yaml_stream(cast(IO[str] | IO[bytes], source), encoding=encoding)
    return from_yaml_path(cast(str | Path, source), encoding=encoding)


def from_pyarrow(
    schema: Any,
    *,
    mode: Literal["raise", "coerce"] = "coerce",
    fallback_type: YadsType | None = None,
    name: str,
    version: str,
    description: str | None = None,
) -> YadsSpec:
    """Load a spec from a `pyarrow.Schema`.

    Args:
        schema: An instance of `pyarrow.Schema`.
        mode: Loading mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
        fallback_type: A yads type to use as fallback when an unsupported
            PyArrow type is encountered. Only used when mode is "coerce".
            Must be either String or Binary, or None. Defaults to None.
        name: Fully-qualified spec name to assign.
        version: Spec version string.
        description: Optional human-readable description.

    Returns:
        A validated immutable `YadsSpec` instance.

    Example:
        >>> import pyarrow as pa
        >>> schema = pa.schema([
        ...     pa.field("id", pa.int64()),
        ...     pa.field("name", pa.string()),
        ... ])
        >>> spec = from_pyarrow(schema, name="users", version="1.0.0")
    """
    from . import pyarrow_loader  # type: ignore

    config = pyarrow_loader.PyArrowLoaderConfig(mode=mode, fallback_type=fallback_type)
    return pyarrow_loader.PyArrowLoader(config).load(
        schema, name=name, version=version, description=description
    )


def from_pyspark(
    schema: Any,
    *,
    mode: Literal["raise", "coerce"] = "coerce",
    fallback_type: YadsType | None = None,
    name: str,
    version: str,
    description: str | None = None,
) -> YadsSpec:
    """Load a spec from a `pyspark.sql.types.StructType`.

    Args:
        schema: An instance of `pyspark.sql.types.StructType`.
        mode: Loading mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
        fallback_type: A yads type to use as fallback when an unsupported
            PySpark type is encountered. Only used when mode is "coerce".
            Must be either String or Binary, or None. Defaults to None.
        name: Fully-qualified spec name to assign.
        version: Spec version string.
        description: Optional human-readable description.

    Returns:
        A validated immutable `YadsSpec` instance.

    Example:
        >>> from pyspark.sql.types import StructType, StructField, LongType, StringType
        >>> schema = StructType([
        ...     StructField("id", LongType(), nullable=False),
        ...     StructField("name", StringType(), nullable=True),
        ... ])
        >>> spec = from_pyspark(schema, name="users", version="1.0.0")
    """
    from . import pyspark_loader  # type: ignore

    config = pyspark_loader.PySparkLoaderConfig(mode=mode, fallback_type=fallback_type)
    return pyspark_loader.PySparkLoader(config).load(
        schema, name=name, version=version, description=description
    )


def from_polars(
    schema: Any,
    *,
    mode: Literal["raise", "coerce"] = "coerce",
    fallback_type: YadsType | None = None,
    name: str,
    version: str,
    description: str | None = None,
) -> YadsSpec:
    """Load a spec from a `polars.Schema`.

    Args:
        schema: An instance of `polars.Schema`.
        mode: Loading mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
        fallback_type: A yads type to use as fallback when an unsupported
            Polars type is encountered. Only used when mode is "coerce".
            Must be either String or Binary, or None. Defaults to None.
        name: Fully-qualified spec name to assign.
        version: Spec version string.
        description: Optional human-readable description.

    Returns:
        A validated immutable `YadsSpec` instance.

    Example:
        >>> import polars as pl
        >>> schema = pl.Schema({"id": pl.Int64, "name": pl.Utf8})
        >>> spec = from_polars(schema, name="users", version="1.0.0")
    """
    from . import polars_loader  # type: ignore

    config = polars_loader.PolarsLoaderConfig(mode=mode, fallback_type=fallback_type)
    return polars_loader.PolarsLoader(config).load(
        schema, name=name, version=version, description=description
    )
