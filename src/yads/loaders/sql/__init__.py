"""SQL database loaders for `YadsSpec`.

This submodule provides loaders that extract table schema information from
SQL databases by querying their catalog tables (information_schema, pg_catalog,
etc.) and converting to canonical `YadsSpec` instances.

Available loaders:
- `PostgreSQLLoader`: Load specs from PostgreSQL tables.
"""

from .base import SQLLoader, SQLLoaderConfig
from .postgres_loader import PostgreSQLLoader

__all__ = [
    "SQLLoader",
    "SQLLoaderConfig",
    "PostgreSQLLoader",
]
