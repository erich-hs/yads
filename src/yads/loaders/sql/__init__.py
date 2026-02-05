"""SQL database loaders for `YadsSpec`.

This submodule provides loaders that extract table schema information from
SQL databases by querying their catalog tables (information_schema, pg_catalog,
sys.*, etc.) and converting to canonical `YadsSpec` instances.

Available loaders:
- `PostgreSqlLoader`: Load specs from PostgreSQL tables.
- `SqlServerLoader`: Load specs from SQL Server tables.
"""

from .base import SqlLoader, SqlLoaderConfig
from .postgres_loader import PostgreSqlLoader
from .sqlserver_loader import SqlServerLoader

__all__ = [
    "SqlLoader",
    "SqlLoaderConfig",
    "PostgreSqlLoader",
    "SqlServerLoader",
]
