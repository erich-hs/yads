"""Tests for SqlServerLoader."""

from __future__ import annotations

from typing import Any
import warnings

import pytest

from yads import types as ytypes
from yads.constraints import (
    DefaultConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
)
from yads.exceptions import LoaderError, UnsupportedFeatureError
from yads.loaders.sql import SqlServerLoader, SqlLoaderConfig


# ---- Fixtures ----------------------------------------------------------------


class MockCursor:
    """Mock DBAPI cursor for testing SQL Server queries."""

    # Column names for each query type
    COLUMNS_QUERY_COLUMNS = [
        "column_name",
        "ordinal_position",
        "data_type",
        "character_maximum_length",
        "numeric_precision",
        "numeric_scale",
        "datetime_precision",
        "is_nullable",
        "column_default",
    ]

    CONSTRAINTS_QUERY_COLUMNS = [
        "constraint_name",
        "constraint_type",
        "column_name",
        "ordinal_position",
        "ref_schema",
        "ref_table",
        "ref_column",
    ]

    IDENTITY_QUERY_COLUMNS = ["column_name", "seed_value", "increment_value"]

    COMPUTED_QUERY_COLUMNS = ["column_name", "definition", "is_persisted"]

    def __init__(self, query_results: dict[str, list[tuple[Any, ...]]]):
        self._query_results = query_results
        self._current_results: list[tuple[Any, ...]] = []
        self._description: list[tuple[str, ...]] | None = None

    def execute(self, query: str, _params: tuple[Any, ...] | None = None) -> None:
        # Determine query type based on content
        if "DB_NAME()" in query:
            results = self._query_results.get("current_database", [("test_db",)])
            columns = ["current_database"]
        elif "INFORMATION_SCHEMA.COLUMNS" in query:
            results = self._query_results.get("information_schema.columns", [])
            columns = self.COLUMNS_QUERY_COLUMNS
        elif "sys.key_constraints" in query or "sys.foreign_keys" in query:
            results = self._query_results.get("sys.constraints", [])
            columns = self.CONSTRAINTS_QUERY_COLUMNS
        elif "sys.identity_columns" in query:
            results = self._query_results.get("sys.identity_columns", [])
            columns = self.IDENTITY_QUERY_COLUMNS
        elif "sys.computed_columns" in query:
            results = self._query_results.get("sys.computed_columns", [])
            columns = self.COMPUTED_QUERY_COLUMNS
        else:
            results = []
            for key, val in self._query_results.items():
                if key in query:
                    results = val
                    break
            columns = [f"col{i}" for i in range(len(results[0]))] if results else []

        self._current_results = results
        if results:
            self._description = [(col,) for col in columns]
        else:
            self._description = None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._current_results

    def close(self) -> None:
        pass

    @property
    def description(self) -> list[tuple[str, ...]] | None:
        return self._description


class MockConnection:
    """Mock DBAPI connection for testing."""

    def __init__(self, query_results: dict[str, list[tuple[Any, ...]]]):
        self._query_results = query_results

    def cursor(self) -> MockCursor:
        return MockCursor(self._query_results)


def make_column_row(
    column_name: str,
    ordinal_position: int,
    data_type: str,
    character_maximum_length: int | None = None,
    numeric_precision: int | None = None,
    numeric_scale: int | None = None,
    datetime_precision: int | None = None,
    is_nullable: str = "YES",
    column_default: str | None = None,
) -> tuple[Any, ...]:
    """Create a column info row matching INFORMATION_SCHEMA.COLUMNS for SQL Server."""
    return (
        column_name,
        ordinal_position,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        datetime_precision,
        is_nullable,
        column_default,
    )


# ---- Basic Type Conversion Tests ---------------------------------------------


class TestSqlServerLoaderTypeConversion:
    """Test type conversion from SQL Server to YadsType."""

    @pytest.mark.parametrize(
        "data_type,expected_type",
        [
            # Integers
            ("tinyint", ytypes.Integer(bits=8, signed=False)),
            ("smallint", ytypes.Integer(bits=16, signed=True)),
            ("int", ytypes.Integer(bits=32, signed=True)),
            ("bigint", ytypes.Integer(bits=64, signed=True)),
            # Floats
            ("real", ytypes.Float(bits=32)),
            ("float", ytypes.Float(bits=64)),
            # Strings
            ("text", ytypes.String()),
            ("varchar", ytypes.String()),
            ("nvarchar", ytypes.String()),
            ("ntext", ytypes.String()),
            # Binary
            ("binary", ytypes.Binary()),
            ("varbinary", ytypes.Binary()),
            ("image", ytypes.Binary()),
            # Boolean
            ("bit", ytypes.Boolean()),
            # Temporal
            ("date", ytypes.Date(bits=32)),
            ("time", ytypes.Time(unit=ytypes.TimeUnit.US)),
            ("smalldatetime", ytypes.TimestampNTZ(unit=ytypes.TimeUnit.S)),
            ("datetime", ytypes.TimestampNTZ(unit=ytypes.TimeUnit.MS)),
            ("datetime2", ytypes.TimestampNTZ(unit=ytypes.TimeUnit.US)),
            ("datetimeoffset", ytypes.TimestampTZ(unit=ytypes.TimeUnit.US, tz="UTC")),
            # UUID
            ("uniqueidentifier", ytypes.UUID()),
            # Spatial
            ("geometry", ytypes.Geometry()),
            ("geography", ytypes.Geography()),
        ],
    )
    def test_basic_type_conversion(self, data_type: str, expected_type: ytypes.YadsType):
        """Test basic type conversions."""
        query_results = {
            "information_schema.columns": [make_column_row("test_col", 1, data_type)],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        assert len(spec.columns) == 1
        assert spec.columns[0].type == expected_type

    def test_varchar_with_length(self):
        """Test VARCHAR with length constraint."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "name",
                    1,
                    "varchar",
                    character_maximum_length=255,
                )
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.String(length=255)

    def test_nvarchar_with_length(self):
        """Test NVARCHAR with length constraint."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "name",
                    1,
                    "nvarchar",
                    character_maximum_length=100,
                )
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.String(length=100)

    def test_varchar_max(self):
        """Test VARCHAR(MAX) - length is -1."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "content",
                    1,
                    "varchar",
                    character_maximum_length=-1,
                )
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.String()

    def test_numeric_with_precision_scale(self):
        """Test NUMERIC with precision and scale."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "price",
                    1,
                    "decimal",
                    numeric_precision=10,
                    numeric_scale=2,
                )
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Decimal(precision=10, scale=2)


# ---- Constraint Tests --------------------------------------------------------


class TestSqlServerLoaderConstraints:
    """Test constraint loading from SQL Server."""

    def test_not_null_constraint(self):
        """Test NOT NULL constraint detection."""
        query_results = {
            "information_schema.columns": [
                make_column_row("id", 1, "int", is_nullable="NO")
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        assert not spec.columns[0].is_nullable
        assert any(isinstance(c, NotNullConstraint) for c in spec.columns[0].constraints)

    def test_identity_column(self):
        """Test IDENTITY column detection."""
        query_results = {
            "information_schema.columns": [
                make_column_row("id", 1, "int", is_nullable="NO")
            ],
            "sys.constraints": [],
            "sys.identity_columns": [("id", 1, 1)],  # seed=1, increment=1
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        identity_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, IdentityConstraint)
        ]
        assert len(identity_constraints) == 1
        assert (
            identity_constraints[0].always is False
        )  # SQL Server allows IDENTITY_INSERT
        assert identity_constraints[0].start == 1
        assert identity_constraints[0].increment == 1

    def test_default_literal_string_value(self):
        """Test literal string DEFAULT value."""
        query_results = {
            "information_schema.columns": [
                make_column_row("status", 1, "varchar", column_default="(('active'))")
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == "active"

    def test_default_numeric_value(self):
        """Test numeric DEFAULT value."""
        query_results = {
            "information_schema.columns": [
                make_column_row("count", 1, "int", column_default="((0))")
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == 0

    def test_default_function_emits_warning(self):
        """Test that function DEFAULT values emit warnings."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "created_at",
                    1,
                    "datetime2",
                    column_default="(getdate())",
                )
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            # Should emit a warning about function default
            assert any("getdate()" in str(warning.message) for warning in w)

        # No DefaultConstraint should be added
        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 0

    def test_primary_key_single_column(self):
        """Test single-column PRIMARY KEY."""
        query_results = {
            "information_schema.columns": [
                make_column_row("id", 1, "int", is_nullable="NO")
            ],
            "sys.constraints": [("PK_test", "PRIMARY KEY", "id", 1, None, None, None)],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        pk_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, PrimaryKeyConstraint)
        ]
        assert len(pk_constraints) == 1


# ---- Unsupported Type Tests --------------------------------------------------


class TestSqlServerLoaderUnsupportedTypes:
    """Test handling of unsupported SQL Server types."""

    def test_unsupported_type_raises_in_raise_mode(self):
        """Test that unsupported types raise in 'raise' mode."""
        query_results = {
            "information_schema.columns": [make_column_row("amount", 1, "money")],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        config = SqlLoaderConfig(mode="raise")
        loader = SqlServerLoader(conn, config)

        with pytest.raises(UnsupportedFeatureError):
            loader.load("test_table")

    def test_unsupported_type_coerces_with_fallback(self):
        """Test that unsupported types coerce to fallback type."""
        query_results = {
            "information_schema.columns": [make_column_row("amount", 1, "money")],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        config = SqlLoaderConfig(mode="coerce", fallback_type=ytypes.String())
        loader = SqlServerLoader(conn, config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("money" in str(warning.message).lower() for warning in w)

        assert spec.columns[0].type == ytypes.String()

    @pytest.mark.parametrize(
        "unsupported_type",
        [
            "xml",
            "money",
            "smallmoney",
            "sql_variant",
            "hierarchyid",
            "timestamp",
            "rowversion",
        ],
    )
    def test_unsupported_types_raise_without_fallback(self, unsupported_type: str):
        """Test that various unsupported types raise in coerce mode without fallback."""
        query_results = {
            "information_schema.columns": [make_column_row("col", 1, unsupported_type)],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        config = SqlLoaderConfig(mode="coerce", fallback_type=None)
        loader = SqlServerLoader(conn, config)

        with pytest.raises(UnsupportedFeatureError, match="fallback_type"):
            loader.load("test_table")


# ---- Error Handling Tests ----------------------------------------------------


class TestSqlServerLoaderErrors:
    """Test error handling."""

    def test_table_not_found_raises_error(self):
        """Test that missing table raises LoaderError."""
        query_results: dict[str, list[tuple[Any, ...]]] = {
            "information_schema.columns": [],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        with pytest.raises(LoaderError, match="not found"):
            loader.load("nonexistent_table")


# ---- Spec Metadata Tests -----------------------------------------------------


class TestSqlServerLoaderSpecMetadata:
    """Test spec metadata generation."""

    def test_default_spec_name(self):
        """Test default spec name from catalog.schema.table."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "int")],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
            "current_database": [("mydb",)],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("users", schema="dbo")

        assert spec.name == "mydb.dbo.users"

    def test_custom_spec_name(self):
        """Test custom spec name."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "int")],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("users", name="my_catalog.my_schema.users")

        assert spec.name == "my_catalog.my_schema.users"

    def test_spec_version(self):
        """Test spec version assignment."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "int")],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("users", version=5)

        assert spec.version == 5

    def test_spec_description(self):
        """Test spec description assignment."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "int")],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("users", description="User accounts table")

        assert spec.description == "User accounts table"


# ---- Foreign Key Tests -------------------------------------------------------


class TestSqlServerLoaderForeignKeys:
    """Test foreign key constraint handling."""

    def test_single_column_foreign_key(self):
        """Test single-column foreign key constraint."""
        from yads.constraints import ForeignKeyConstraint

        query_results = {
            "information_schema.columns": [make_column_row("user_id", 1, "int")],
            "sys.constraints": [
                ("FK_user", "FOREIGN KEY", "user_id", 1, "dbo", "users", "id")
            ],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        fk_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, ForeignKeyConstraint)
        ]
        assert len(fk_constraints) == 1
        assert fk_constraints[0].references.table == "users"
        assert fk_constraints[0].references.columns == ["id"]

    def test_foreign_key_with_non_dbo_schema(self):
        """Test foreign key referencing non-dbo schema table."""
        from yads.constraints import ForeignKeyConstraint

        query_results = {
            "information_schema.columns": [make_column_row("tenant_id", 1, "int")],
            "sys.constraints": [
                ("FK_tenant", "FOREIGN KEY", "tenant_id", 1, "core", "tenants", "id")
            ],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        fk_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, ForeignKeyConstraint)
        ]
        assert len(fk_constraints) == 1
        assert fk_constraints[0].references.table == "core.tenants"


# ---- Composite Table Constraints Tests ---------------------------------------


class TestSqlServerLoaderTableConstraints:
    """Test table-level constraints (composite PK/FK)."""

    def test_composite_primary_key(self):
        """Test composite primary key as table-level constraint."""
        from yads.constraints import PrimaryKeyTableConstraint

        query_results = {
            "information_schema.columns": [
                make_column_row("order_id", 1, "int", is_nullable="NO"),
                make_column_row("item_id", 2, "int", is_nullable="NO"),
            ],
            "sys.constraints": [
                ("PK_order_items", "PRIMARY KEY", "order_id", 1, None, None, None),
                ("PK_order_items", "PRIMARY KEY", "item_id", 2, None, None, None),
            ],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("order_items")

        # Should not have PK on columns (composite)
        for col in spec.columns:
            pk_constraints = [
                c for c in col.constraints if isinstance(c, PrimaryKeyConstraint)
            ]
            assert len(pk_constraints) == 0

        # Should have table-level PK
        pk_table_constraints = [
            c for c in spec.table_constraints if isinstance(c, PrimaryKeyTableConstraint)
        ]
        assert len(pk_table_constraints) == 1
        assert set(pk_table_constraints[0].columns) == {"order_id", "item_id"}


# ---- Computed Column Tests ---------------------------------------------------


class TestSqlServerLoaderComputedColumns:
    """Test computed column handling."""

    def test_computed_column_simple_expression(self):
        """Test computed column with simple expression."""
        query_results = {
            "information_schema.columns": [
                make_column_row("first_name", 1, "varchar"),
                make_column_row("upper_name", 2, "varchar"),
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [("upper_name", "(upper([first_name]))", True)],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        gen_col = spec.columns[1]
        assert gen_col.generated_as is not None
        assert gen_col.generated_as.column == "first_name"
        assert gen_col.generated_as.transform == "upper"

    def test_computed_column_binary_expression(self):
        """Test computed column with binary expression."""
        query_results = {
            "information_schema.columns": [
                make_column_row("quantity", 1, "int"),
                make_column_row("price", 2, "decimal"),
                make_column_row("total", 3, "decimal"),
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [("total", "([quantity]*[price])", True)],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        gen_col = spec.columns[2]
        assert gen_col.generated_as is not None
        assert gen_col.generated_as.column == "quantity"
        assert gen_col.generated_as.transform == "expression"
        assert "[quantity]*[price]" in gen_col.generated_as.transform_args[0]


# ---- Default Value Parsing Tests ---------------------------------------------


class TestSqlServerLoaderDefaultValues:
    """Test default value parsing."""

    def test_default_null(self):
        """Test NULL default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row("optional", 1, "varchar", column_default="(NULL)")
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value is None

    def test_default_unicode_string(self):
        """Test N'unicode' string default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row("name", 1, "nvarchar", column_default="(N'default')")
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == "default"

    def test_default_float_value(self):
        """Test floating point default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row("rate", 1, "float", column_default="((3.14))")
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == 3.14

    def test_default_negative_integer(self):
        """Test negative integer default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row("offset", 1, "int", column_default="((-10))")
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == -10

    def test_default_newid_warning(self):
        """Test newid() function emits warning."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "uuid_col", 1, "uniqueidentifier", column_default="(newid())"
                )
            ],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("newid()" in str(warning.message) for warning in w)

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 0


# ---- UNIQUE Constraint Warning Tests -----------------------------------------


class TestSqlServerLoaderUniqueConstraintWarning:
    """Test UNIQUE constraint warning handling."""

    def test_unique_constraint_emits_warning(self):
        """Test that UNIQUE constraints emit a warning."""
        query_results = {
            "information_schema.columns": [make_column_row("email", 1, "varchar")],
            "sys.constraints": [("UQ_email", "UNIQUE", "email", 1, None, None, None)],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("unique" in str(warning.message).lower() for warning in w)

        # Spec should still load successfully
        assert len(spec.columns) == 1


# ---- Config Tests ------------------------------------------------------------


class TestSqlServerLoaderConfig:
    """Test loader configuration."""

    def test_mode_override(self):
        """Test mode override in load() call."""
        query_results = {
            "information_schema.columns": [make_column_row("xml_col", 1, "xml")],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        # Default mode is coerce
        config = SqlLoaderConfig(fallback_type=ytypes.String())
        loader = SqlServerLoader(conn, config)

        # Override to raise mode
        with pytest.raises(UnsupportedFeatureError):
            loader.load("test_table", mode="raise")


# ---- Spatial Type Tests ------------------------------------------------------


class TestSqlServerLoaderSpatialTypes:
    """Test spatial type handling."""

    def test_geometry_type(self):
        """Test GEOMETRY type conversion."""
        query_results = {
            "information_schema.columns": [make_column_row("location", 1, "geometry")],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Geometry()

    def test_geography_type(self):
        """Test GEOGRAPHY type conversion."""
        query_results = {
            "information_schema.columns": [make_column_row("location", 1, "geography")],
            "sys.constraints": [],
            "sys.identity_columns": [],
            "sys.computed_columns": [],
        }
        conn = MockConnection(query_results)
        loader = SqlServerLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Geography()
