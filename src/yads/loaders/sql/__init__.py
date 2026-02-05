"""SQL database loaders for `YadsSpec`.

This submodule provides loaders that extract table schema information from
SQL databases by querying their catalog tables (information_schema, pg_catalog,
etc.) and converting to canonical `YadsSpec` instances.

Available loaders:
- `PostgreSqlLoader`: Load specs from PostgreSQL tables.
"""

from .base import SqlLoader, SqlLoaderConfig
from .postgres_loader import PostgreSqlLoader

__all__ = [
    "SqlLoader",
    "SqlLoaderConfig",
    "PostgreSqlLoader",
]
