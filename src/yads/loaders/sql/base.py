"""Base classes for SQL database loaders.

SQL loaders query database catalogs (information_schema, pg_catalog, etc.) to
extract table schema information and convert it to canonical `YadsSpec` instances.

Unlike other loaders that accept in-memory schema objects, SQL loaders require
a DBAPI-compatible database connection to query the catalog tables directly.
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ...exceptions import LoaderConfigError, UnsupportedFeatureError, validation_warning
from ...serializers import ConstraintSerializer, TypeSerializer
from .. import base as loader_base

if TYPE_CHECKING:
    from ...types import Binary, String, YadsType


@dataclass(frozen=True)
class SQLLoaderConfig(loader_base.BaseLoaderConfig):
    """Configuration for SQL database loaders.

    Args:
        mode: Loading mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
        fallback_type: A yads type to use as fallback when an unsupported
            database type is encountered. Only used when mode is "coerce".
            Must be either String or Binary, or None. Defaults to None.
    """

    fallback_type: String | Binary | None = None

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        super().__post_init__()
        if self.fallback_type is not None:
            from ...types import Binary, String

            if not isinstance(self.fallback_type, (String, Binary)):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise LoaderConfigError(
                    "fallback_type must be either String or Binary type, or None."
                )


class SQLLoader(loader_base.ConfigurableLoader, ABC):
    """Base class for SQL database schema loaders.

    SQL loaders query database catalogs to extract table schema information
    and convert it to canonical `YadsSpec` instances. They require a DBAPI-
    compatible database connection.

    The connection must implement the DBAPI 2.0 specification (PEP 249):
    - `cursor()` method returning a cursor object
    - Cursor must support `execute(query, params)`, `fetchall()`, and `close()`
    - Cursor must have a `description` attribute after execute

    Subclasses must implement the `load()` method for their specific database.

    Example:
        ```python
        import psycopg2
        from yads.loaders.sql import PostgreSQLLoader

        conn = psycopg2.connect("postgresql://localhost/mydb")
        loader = PostgreSQLLoader(conn)
        spec = loader.load("users", schema="public")
        ```
    """

    config: SQLLoaderConfig

    def __init__(
        self,
        connection: Any,
        config: SQLLoaderConfig | None = None,
    ) -> None:
        """Initialize the SQL loader.

        Args:
            connection: A DBAPI-compatible database connection. Must support
                `cursor()` method returning a cursor with `execute()`,
                `fetchall()`, `close()`, and `description` attribute.
            config: Configuration object. If None, uses default SQLLoaderConfig.
        """
        self._connection = connection
        self.config = config or SQLLoaderConfig()
        self._type_serializer = TypeSerializer()
        self._constraint_serializer = ConstraintSerializer()
        super().__init__(self.config)

    def _execute_query(
        self,
        query: str,
        params: tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries.

        Args:
            query: SQL query string with parameter placeholders.
            params: Optional tuple of parameter values.

        Returns:
            List of dictionaries where keys are column names from the query.
        """
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, params)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def raise_or_coerce(
        self,
        feature_name: str,
        *,
        coerce_type: YadsType | None = None,
        error_msg: str | None = None,
    ) -> YadsType:
        """Handle unsupported features based on the current mode.

        In "raise" mode, raises UnsupportedFeatureError.
        In "coerce" mode, emits a warning and returns the coerced type.

        Args:
            feature_name: Name of the unsupported feature (e.g., type name).
            coerce_type: Type to coerce to. If None, uses config.fallback_type.
            error_msg: Custom error message. If None, generates a default message.

        Returns:
            The coerced YadsType (only in "coerce" mode with valid fallback).

        Raises:
            UnsupportedFeatureError: In "raise" mode, or in "coerce" mode
                without a valid fallback type.
        """
        field_context = self._current_field_name or "<unknown>"
        msg = error_msg or (
            f"Unsupported database type '{feature_name}' for field '{field_context}'"
        )

        fallback = coerce_type or self.config.fallback_type

        if self.config.mode == "coerce":
            if fallback is None:
                raise UnsupportedFeatureError(
                    f"{msg}. Specify a fallback_type to enable coercion of unsupported types."
                )
            validation_warning(
                message=f"{msg}. The data type will be coerced to {fallback}.",
                filename=self.__class__.__module__,
                module=self.__class__.__module__,
            )
            return fallback

        raise UnsupportedFeatureError(f"{msg}.")
