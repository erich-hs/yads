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
from typing import IO, Any, cast

from ..spec import YadsSpec
from .base import DictLoader
from .yaml_loader import YamlLoader
from .pyarrow_loader import PyArrowLoader

__all__ = [
    "from_dict",
    "from_yaml_string",
    "from_yaml_path",
    "from_yaml_stream",
    "from_yaml",
    "from_pyarrow",
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
    return DictLoader(data).load()


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
    return YamlLoader(content).load()


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
    return YamlLoader(text).load()


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
    return YamlLoader(text).load()


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
    schema: "Any", *, name: str, version: str, description: str | None = None
) -> YadsSpec:  # type: ignore[name-defined]
    """Load a spec from a `pyarrow.Schema`.

    Args:
        schema: An instance of `pyarrow.Schema`.
        name: Fully-qualified spec name to assign.
        version: Spec version string.
        description: Optional human-readable description.

    Returns:
        A validated immutable `YadsSpec` instance.
    """
    # Lazy import typing to avoid hard dependency at import time in this module
    return PyArrowLoader(
        schema, name=name, version=version, description=description
    ).load()
