"""Tests for PostgreSqlLoader."""

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
from yads.loaders.sql import PostgreSqlLoader, SqlLoaderConfig


# ---- Fixtures ----------------------------------------------------------------


class MockCursor:
    """Mock DBAPI cursor for testing."""

    # Column names for each query type
    COLUMNS_QUERY_COLUMNS = [
        "column_name",
        "ordinal_position",
        "data_type",
        "udt_name",
        "character_maximum_length",
        "numeric_precision",
        "numeric_scale",
        "datetime_precision",
        "interval_type",
        "is_nullable",
        "column_default",
        "is_identity",
        "identity_generation",
        "identity_start",
        "identity_increment",
        "is_generated",
        "generation_expression",
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

    ARRAY_QUERY_COLUMNS = ["column_name", "element_type", "dimensions"]

    SERIAL_QUERY_COLUMNS = ["column_name", "start_value", "increment"]

    COMPOSITE_TYPE_COLUMNS = ["field_name", "field_position", "field_type", "not_null"]

    def __init__(self, query_results: dict[str, list[tuple[Any, ...]]]):
        self._query_results = query_results
        self._current_results: list[tuple[Any, ...]] = []
        self._description: list[tuple[str, ...]] | None = None

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        # Determine query type based on content
        if "current_database()" in query:
            # current_database() query
            results = self._query_results.get("current_database", [("test_db",)])
            columns = ["current_database"]
        elif "information_schema.columns" in query:
            results = self._query_results.get("information_schema.columns", [])
            columns = self.COLUMNS_QUERY_COLUMNS
        elif "information_schema.table_constraints" in query:
            results = self._query_results.get("information_schema.table_constraints", [])
            columns = self.CONSTRAINTS_QUERY_COLUMNS
        elif "typcategory = 'A'" in query:
            # Array info query
            results = self._query_results.get("pg_catalog.pg_attribute", [])
            columns = self.ARRAY_QUERY_COLUMNS
        elif "pg_sequence" in query:
            # Serial columns query
            results = self._query_results.get("pg_catalog.pg_depend", [])
            columns = self.SERIAL_QUERY_COLUMNS
        elif "typtype = 'c'" in query:
            # Composite type query
            results = self._query_results.get("pg_catalog.pg_type", [])
            columns = self.COMPOSITE_TYPE_COLUMNS
        else:
            # Unknown query - try to find matching key
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
    udt_name: str,
    character_maximum_length: int | None = None,
    numeric_precision: int | None = None,
    numeric_scale: int | None = None,
    datetime_precision: int | None = None,
    interval_type: str | None = None,
    is_nullable: str = "YES",
    column_default: str | None = None,
    is_identity: str = "NO",
    identity_generation: str | None = None,
    identity_start: int | None = None,
    identity_increment: int | None = None,
    is_generated: str = "NEVER",
    generation_expression: str | None = None,
) -> tuple[Any, ...]:
    """Create a column info row matching information_schema.columns."""
    return (
        column_name,
        ordinal_position,
        data_type,
        udt_name,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        datetime_precision,
        interval_type,
        is_nullable,
        column_default,
        is_identity,
        identity_generation,
        identity_start,
        identity_increment,
        is_generated,
        generation_expression,
    )


# ---- Basic Type Conversion Tests ---------------------------------------------


class TestPostgreSqlLoaderTypeConversion:
    """Test type conversion from PostgreSQL to YadsType."""

    @pytest.mark.parametrize(
        "data_type,udt_name,expected_type",
        [
            # Integers
            ("smallint", "int2", ytypes.Integer(bits=16, signed=True)),
            ("integer", "int4", ytypes.Integer(bits=32, signed=True)),
            ("bigint", "int8", ytypes.Integer(bits=64, signed=True)),
            # Floats
            ("real", "float4", ytypes.Float(bits=32)),
            ("double precision", "float8", ytypes.Float(bits=64)),
            # Strings
            ("text", "text", ytypes.String()),
            ("character varying", "varchar", ytypes.String()),
            # Binary
            ("bytea", "bytea", ytypes.Binary()),
            # Boolean
            ("boolean", "bool", ytypes.Boolean()),
            # Temporal
            ("date", "date", ytypes.Date(bits=32)),
            ("time without time zone", "time", ytypes.Time(unit=ytypes.TimeUnit.US)),
            (
                "timestamp without time zone",
                "timestamp",
                ytypes.TimestampNTZ(unit=ytypes.TimeUnit.US),
            ),
            (
                "timestamp with time zone",
                "timestamptz",
                ytypes.TimestampTZ(unit=ytypes.TimeUnit.US, tz="UTC"),
            ),
            # UUID
            ("uuid", "uuid", ytypes.UUID()),
            # JSON
            ("json", "json", ytypes.JSON()),
            ("jsonb", "jsonb", ytypes.JSON()),
        ],
    )
    def test_basic_type_conversion(
        self, data_type: str, udt_name: str, expected_type: ytypes.YadsType
    ):
        """Test basic type conversions."""
        query_results = {
            "information_schema.columns": [
                make_column_row("test_col", 1, data_type, udt_name)
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

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
                    "character varying",
                    "varchar",
                    character_maximum_length=255,
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.String(length=255)

    def test_numeric_with_precision_scale(self):
        """Test NUMERIC with precision and scale."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "price",
                    1,
                    "numeric",
                    "numeric",
                    numeric_precision=10,
                    numeric_scale=2,
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Decimal(precision=10, scale=2)

    def test_interval_with_fields(self):
        """Test INTERVAL with specific fields."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "duration", 1, "interval", "interval", interval_type="YEAR TO MONTH"
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Interval(
            interval_start=ytypes.IntervalTimeUnit.YEAR,
            interval_end=ytypes.IntervalTimeUnit.MONTH,
        )


# ---- Constraint Tests --------------------------------------------------------


class TestPostgreSqlLoaderConstraints:
    """Test constraint loading from PostgreSQL."""

    def test_not_null_constraint(self):
        """Test NOT NULL constraint detection."""
        query_results = {
            "information_schema.columns": [
                make_column_row("id", 1, "integer", "int4", is_nullable="NO")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert not spec.columns[0].is_nullable
        assert any(isinstance(c, NotNullConstraint) for c in spec.columns[0].constraints)

    def test_identity_column(self):
        """Test IDENTITY column detection."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "id",
                    1,
                    "integer",
                    "int4",
                    is_nullable="NO",
                    is_identity="YES",
                    identity_generation="ALWAYS",
                    identity_start=1,
                    identity_increment=1,
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        identity_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, IdentityConstraint)
        ]
        assert len(identity_constraints) == 1
        assert identity_constraints[0].always is True

    def test_default_literal_value(self):
        """Test literal DEFAULT value."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "status", 1, "text", "text", column_default="'active'::text"
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

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
                make_column_row("count", 1, "integer", "int4", column_default="0")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

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
                    "timestamp with time zone",
                    "timestamptz",
                    column_default="now()",
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            # Should emit a warning about function default
            assert any("now()" in str(warning.message) for warning in w)

        # No DefaultConstraint should be added
        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 0

    def test_primary_key_single_column(self):
        """Test single-column PRIMARY KEY."""
        query_results = {
            "information_schema.columns": [
                make_column_row("id", 1, "integer", "int4", is_nullable="NO")
            ],
            "information_schema.table_constraints": [
                ("pk_test", "PRIMARY KEY", "id", 1, None, None, None)
            ],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        pk_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, PrimaryKeyConstraint)
        ]
        assert len(pk_constraints) == 1


# ---- Unsupported Type Tests --------------------------------------------------


class TestPostgreSqlLoaderUnsupportedTypes:
    """Test handling of unsupported PostgreSQL types."""

    def test_unsupported_type_raises_in_raise_mode(self):
        """Test that unsupported types raise in 'raise' mode."""
        query_results = {
            "information_schema.columns": [make_column_row("price", 1, "money", "money")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        config = SqlLoaderConfig(mode="raise")
        loader = PostgreSqlLoader(conn, config)

        with pytest.raises(UnsupportedFeatureError):
            loader.load("test_table")

    def test_unsupported_type_coerces_with_fallback(self):
        """Test that unsupported types coerce to fallback type."""
        query_results = {
            "information_schema.columns": [make_column_row("price", 1, "money", "money")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        config = SqlLoaderConfig(mode="coerce", fallback_type=ytypes.String())
        loader = PostgreSqlLoader(conn, config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("money" in str(warning.message).lower() for warning in w)

        assert spec.columns[0].type == ytypes.String()


# ---- Array Type Tests --------------------------------------------------------


class TestPostgreSqlLoaderArrayTypes:
    """Test array type handling."""

    def test_simple_array(self):
        """Test simple one-dimensional array."""
        query_results = {
            "information_schema.columns": [make_column_row("tags", 1, "ARRAY", "_text")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [("tags", "text", 1)],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert isinstance(spec.columns[0].type, ytypes.Array)
        assert spec.columns[0].type.element == ytypes.String()

    def test_multidimensional_array_emits_warning(self):
        """Test multi-dimensional array emits warning and uses nested Arrays."""
        query_results = {
            "information_schema.columns": [
                make_column_row("matrix", 1, "ARRAY", "_int4")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [("matrix", "int4", 2)],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any(
                "multi-dimensional" in str(warning.message).lower() for warning in w
            )

        # Should be nested Array
        assert isinstance(spec.columns[0].type, ytypes.Array)
        assert isinstance(spec.columns[0].type.element, ytypes.Array)


# ---- Composite Type Tests ----------------------------------------------------


class TestPostgreSqlLoaderCompositeTypes:
    """Test composite type handling."""

    def test_user_defined_composite_type(self):
        """Test USER-DEFINED composite type converted to Struct."""
        query_results = {
            "information_schema.columns": [
                make_column_row("address", 1, "USER-DEFINED", "address_type")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
            # Composite type query result (typtype = 'c')
            "pg_catalog.pg_type": [
                ("street", 1, "text", False),
                ("city", 2, "text", False),
                ("zip_code", 3, "varchar", False),
            ],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert len(spec.columns) == 1
        col_address = spec.columns[0]
        assert isinstance(col_address.type, ytypes.Struct)
        assert len(col_address.type.fields) == 3

        field_names = [f.name for f in col_address.type.fields]
        assert "street" in field_names
        assert "city" in field_names
        assert "zip_code" in field_names


# ---- Error Handling Tests ----------------------------------------------------


class TestPostgreSqlLoaderErrors:
    """Test error handling."""

    def test_table_not_found_raises_error(self):
        """Test that missing table raises LoaderError."""
        query_results = {
            "information_schema.columns": [],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        with pytest.raises(LoaderError, match="not found"):
            loader.load("nonexistent_table")


# ---- Spec Metadata Tests -----------------------------------------------------


class TestPostgreSqlLoaderSpecMetadata:
    """Test spec metadata generation."""

    def test_default_spec_name(self):
        """Test default spec name from catalog.schema.table."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "integer", "int4")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
            "current_database": [("mydb",)],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("users", schema="public")

        assert spec.name == "mydb.public.users"

    def test_custom_spec_name(self):
        """Test custom spec name."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "integer", "int4")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("users", name="my_catalog.my_schema.users")

        assert spec.name == "my_catalog.my_schema.users"

    def test_spec_version(self):
        """Test spec version assignment."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "integer", "int4")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("users", version=5)

        assert spec.version == 5

    def test_spec_description(self):
        """Test spec description assignment."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "integer", "int4")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("users", description="User accounts table")

        assert spec.description == "User accounts table"


# ---- Config Tests ------------------------------------------------------------


class TestSqlLoaderConfig:
    """Test loader configuration."""

    def test_invalid_fallback_type_raises_error(self):
        """Test that invalid fallback type raises error."""
        with pytest.raises(Exception):  # LoaderConfigError
            SqlLoaderConfig(fallback_type=ytypes.Integer())

    def test_mode_override(self):
        """Test mode override in load() call."""
        query_results = {
            "information_schema.columns": [make_column_row("price", 1, "money", "money")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        # Default mode is coerce
        config = SqlLoaderConfig(fallback_type=ytypes.String())
        loader = PostgreSqlLoader(conn, config)

        # Override to raise mode
        with pytest.raises(UnsupportedFeatureError):
            loader.load("test_table", mode="raise")


# ---- Serial Column Tests -----------------------------------------------------


class TestPostgreSqlLoaderSerialColumns:
    """Test SERIAL/BIGSERIAL column handling."""

    def test_serial_column_detected_as_identity(self):
        """Test that SERIAL columns are detected via sequence ownership."""
        query_results = {
            "information_schema.columns": [
                make_column_row("id", 1, "integer", "int4", is_nullable="NO")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [("id", 1, 1)],  # column_name, start_value, increment
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        identity_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, IdentityConstraint)
        ]
        assert len(identity_constraints) == 1
        assert identity_constraints[0].always is False  # SERIAL allows manual values
        assert identity_constraints[0].start == 1
        assert identity_constraints[0].increment == 1


# ---- Foreign Key Tests -------------------------------------------------------


class TestPostgreSqlLoaderForeignKeys:
    """Test foreign key constraint handling."""

    def test_single_column_foreign_key(self):
        """Test single-column foreign key constraint."""
        from yads.constraints import ForeignKeyConstraint

        query_results = {
            "information_schema.columns": [
                make_column_row("user_id", 1, "integer", "int4")
            ],
            "information_schema.table_constraints": [
                ("fk_user", "FOREIGN KEY", "user_id", 1, "public", "users", "id")
            ],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        fk_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, ForeignKeyConstraint)
        ]
        assert len(fk_constraints) == 1
        assert fk_constraints[0].references.table == "users"
        assert fk_constraints[0].references.columns == ["id"]

    def test_foreign_key_with_non_public_schema(self):
        """Test foreign key referencing non-public schema table."""
        from yads.constraints import ForeignKeyConstraint

        query_results = {
            "information_schema.columns": [
                make_column_row("tenant_id", 1, "integer", "int4")
            ],
            "information_schema.table_constraints": [
                ("fk_tenant", "FOREIGN KEY", "tenant_id", 1, "core", "tenants", "id")
            ],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        fk_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, ForeignKeyConstraint)
        ]
        assert len(fk_constraints) == 1
        assert fk_constraints[0].references.table == "core.tenants"


# ---- Composite Table Constraints Tests ---------------------------------------


class TestPostgreSqlLoaderTableConstraints:
    """Test table-level constraints (composite PK/FK)."""

    def test_composite_primary_key(self):
        """Test composite primary key as table-level constraint."""
        from yads.constraints import PrimaryKeyTableConstraint

        query_results = {
            "information_schema.columns": [
                make_column_row("order_id", 1, "integer", "int4", is_nullable="NO"),
                make_column_row("item_id", 2, "integer", "int4", is_nullable="NO"),
            ],
            "information_schema.table_constraints": [
                ("pk_order_items", "PRIMARY KEY", "order_id", 1, None, None, None),
                ("pk_order_items", "PRIMARY KEY", "item_id", 2, None, None, None),
            ],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

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

    def test_composite_foreign_key(self):
        """Test composite foreign key as table-level constraint."""
        from yads.constraints import ForeignKeyTableConstraint

        query_results = {
            "information_schema.columns": [
                make_column_row("order_id", 1, "integer", "int4"),
                make_column_row("item_id", 2, "integer", "int4"),
            ],
            "information_schema.table_constraints": [
                (
                    "fk_order_items",
                    "FOREIGN KEY",
                    "order_id",
                    1,
                    "public",
                    "orders",
                    "order_id",
                ),
                (
                    "fk_order_items",
                    "FOREIGN KEY",
                    "item_id",
                    2,
                    "public",
                    "orders",
                    "item_id",
                ),
            ],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("order_items")

        # Should have table-level FK
        fk_table_constraints = [
            c for c in spec.table_constraints if isinstance(c, ForeignKeyTableConstraint)
        ]
        assert len(fk_table_constraints) == 1
        assert set(fk_table_constraints[0].columns) == {"order_id", "item_id"}
        assert fk_table_constraints[0].references.table == "orders"

    def test_composite_foreign_key_non_public_schema(self):
        """Test composite foreign key with non-public schema."""
        from yads.constraints import ForeignKeyTableConstraint

        query_results = {
            "information_schema.columns": [
                make_column_row("a_id", 1, "integer", "int4"),
                make_column_row("b_id", 2, "integer", "int4"),
            ],
            "information_schema.table_constraints": [
                ("fk_multi", "FOREIGN KEY", "a_id", 1, "other_schema", "ref_table", "a"),
                ("fk_multi", "FOREIGN KEY", "b_id", 2, "other_schema", "ref_table", "b"),
            ],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        fk_table_constraints = [
            c for c in spec.table_constraints if isinstance(c, ForeignKeyTableConstraint)
        ]
        assert len(fk_table_constraints) == 1
        assert fk_table_constraints[0].references.table == "other_schema.ref_table"


# ---- Generated Column Tests --------------------------------------------------


class TestPostgreSqlLoaderGeneratedColumns:
    """Test generated/computed column handling."""

    def test_generated_column_simple_reference(self):
        """Test generated column with simple column reference."""
        query_results = {
            "information_schema.columns": [
                make_column_row("first_name", 1, "text", "text"),
                make_column_row(
                    "upper_name",
                    2,
                    "text",
                    "text",
                    is_generated="ALWAYS",
                    generation_expression="upper(first_name)",
                ),
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        gen_col = spec.columns[1]
        assert gen_col.generated_as is not None
        assert gen_col.generated_as.column == "first_name"
        assert gen_col.generated_as.transform == "upper"

    def test_generated_column_binary_expression(self):
        """Test generated column with binary expression."""
        query_results = {
            "information_schema.columns": [
                make_column_row("quantity", 1, "integer", "int4"),
                make_column_row("price", 2, "numeric", "numeric"),
                make_column_row(
                    "total",
                    3,
                    "numeric",
                    "numeric",
                    is_generated="ALWAYS",
                    generation_expression="(quantity * price)",
                ),
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        gen_col = spec.columns[2]
        assert gen_col.generated_as is not None
        assert gen_col.generated_as.column == "quantity"
        assert gen_col.generated_as.transform == "expression"
        assert "quantity * price" in gen_col.generated_as.transform_args[0]

    def test_generated_column_unparseable_emits_warning(self):
        """Test unparseable generation expression emits warning."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "weird_col",
                    1,
                    "text",
                    "text",
                    is_generated="ALWAYS",
                    generation_expression="",  # Empty expression
                ),
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        # Empty expression means no generated_as
        assert spec.columns[0].generated_as is None

    def test_generated_column_with_transform_args(self):
        """Test generated column with function having multiple args."""
        query_results = {
            "information_schema.columns": [
                make_column_row("value", 1, "numeric", "numeric"),
                make_column_row(
                    "rounded",
                    2,
                    "numeric",
                    "numeric",
                    is_generated="ALWAYS",
                    generation_expression="round(value, 2)",
                ),
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        gen_col = spec.columns[1]
        assert gen_col.generated_as is not None
        assert gen_col.generated_as.column == "value"
        assert gen_col.generated_as.transform == "round"
        assert gen_col.generated_as.transform_args == ["2"]


# ---- PostGIS Type Tests ------------------------------------------------------


class TestPostgreSqlLoaderPostGISTypes:
    """Test PostGIS geometry/geography type handling."""

    def test_geometry_type(self):
        """Test GEOMETRY type conversion."""
        query_results = {
            "information_schema.columns": [
                make_column_row("location", 1, "USER-DEFINED", "geometry")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
            "pg_catalog.pg_type": [],  # Not a composite type
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Geometry()

    def test_geography_type(self):
        """Test GEOGRAPHY type conversion."""
        query_results = {
            "information_schema.columns": [
                make_column_row("location", 1, "USER-DEFINED", "geography")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
            "pg_catalog.pg_type": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Geography()

    def test_geometry_direct_type(self):
        """Test geometry as direct data_type (not user-defined)."""
        query_results = {
            "information_schema.columns": [
                make_column_row("shape", 1, "geometry", "geometry")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Geometry()

    def test_geography_direct_type(self):
        """Test geography as direct data_type."""
        query_results = {
            "information_schema.columns": [
                make_column_row("shape", 1, "geography", "geography")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Geography()


# ---- Domain Type Tests -------------------------------------------------------


class TestPostgreSqlLoaderDomainTypes:
    """Test domain type handling."""

    def test_domain_type_resolved_to_base(self):
        """Test domain type is resolved to its base type."""

        class DomainMockCursor(MockCursor):
            """Mock cursor that handles domain type queries."""

            DOMAIN_QUERY_COLUMNS = ["base_type", "type_length"]

            def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
                if "typtype = 'd'" in query:
                    # Domain type query
                    results = self._query_results.get("pg_domain", [])
                    self._current_results = results
                    if results:
                        self._description = [(col,) for col in self.DOMAIN_QUERY_COLUMNS]
                    else:
                        self._description = None
                else:
                    super().execute(query, params)

        class DomainMockConnection(MockConnection):
            def cursor(self) -> DomainMockCursor:
                return DomainMockCursor(self._query_results)

        query_results = {
            "information_schema.columns": [
                make_column_row("email", 1, "USER-DEFINED", "email_domain")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
            "pg_catalog.pg_type": [],  # Not a composite type
            "pg_domain": [("varchar", 255)],  # Base type is varchar
        }
        conn = DomainMockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        # Should resolve to String (varchar base type)
        assert spec.columns[0].type == ytypes.String()

    def test_unknown_user_defined_type_coerces(self):
        """Test unknown user-defined type coerces to fallback."""

        class UnknownTypeMockCursor(MockCursor):
            def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
                if "typtype = 'd'" in query:
                    # Domain query returns nothing
                    self._current_results = []
                    self._description = None
                else:
                    super().execute(query, params)

        class UnknownTypeMockConnection(MockConnection):
            def cursor(self) -> UnknownTypeMockCursor:
                return UnknownTypeMockCursor(self._query_results)

        query_results = {
            "information_schema.columns": [
                make_column_row("custom_col", 1, "USER-DEFINED", "my_custom_type")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
            "pg_catalog.pg_type": [],  # Not a composite
        }
        conn = UnknownTypeMockConnection(query_results)
        config = SqlLoaderConfig(mode="coerce", fallback_type=ytypes.String())
        loader = PostgreSqlLoader(conn, config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("my_custom_type" in str(warning.message) for warning in w)

        assert spec.columns[0].type == ytypes.String()


# ---- Additional Type Tests ---------------------------------------------------


class TestPostgreSqlLoaderAdditionalTypes:
    """Test additional PostgreSQL type conversions."""

    def test_time_with_timezone_emits_warning(self):
        """Test TIME WITH TIME ZONE emits warning about lost timezone."""
        query_results = {
            "information_schema.columns": [
                make_column_row("event_time", 1, "time with time zone", "timetz")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("timezone" in str(warning.message).lower() for warning in w)

        assert spec.columns[0].type == ytypes.Time(unit=ytypes.TimeUnit.US)

    def test_char_type(self):
        """Test CHAR/CHARACTER type conversion."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "code", 1, "character", "bpchar", character_maximum_length=5
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.String(length=5)

    def test_name_type(self):
        """Test PostgreSQL NAME type (identifier type)."""
        query_results = {
            "information_schema.columns": [
                make_column_row("identifier", 1, "name", "name")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.String(length=63)

    def test_decimal_without_precision(self):
        """Test DECIMAL without explicit precision/scale."""
        query_results = {
            "information_schema.columns": [
                make_column_row("amount", 1, "numeric", "numeric")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Decimal()

    def test_interval_single_unit(self):
        """Test INTERVAL with single unit (not range)."""
        query_results = {
            "information_schema.columns": [
                make_column_row("years", 1, "interval", "interval", interval_type="YEAR")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Interval(
            interval_start=ytypes.IntervalTimeUnit.YEAR
        )

    def test_interval_without_fields(self):
        """Test INTERVAL without explicit fields (defaults to DAY TO SECOND)."""
        query_results = {
            "information_schema.columns": [
                make_column_row("duration", 1, "interval", "interval")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Interval(
            interval_start=ytypes.IntervalTimeUnit.DAY,
            interval_end=ytypes.IntervalTimeUnit.SECOND,
        )

    def test_interval_invalid_fallback(self):
        """Test INTERVAL with invalid fields falls back to DAY TO SECOND."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "duration", 1, "interval", "interval", interval_type="INVALID"
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Interval(
            interval_start=ytypes.IntervalTimeUnit.DAY,
            interval_end=ytypes.IntervalTimeUnit.SECOND,
        )

    def test_type_udt_fallback(self):
        """Test that udt_name is used as fallback for type conversion."""
        query_results = {
            "information_schema.columns": [
                # data_type is odd, but udt_name is recognizable
                make_column_row("col", 1, "some_alias", "int4")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        # Should fallback to udt_name "int4"
        assert spec.columns[0].type == ytypes.Integer(bits=32, signed=True)


# ---- Default Value Parsing Tests ---------------------------------------------


class TestPostgreSqlLoaderDefaultValues:
    """Test default value parsing."""

    def test_default_null(self):
        """Test NULL default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row("optional", 1, "text", "text", column_default="NULL")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value is None

    def test_default_boolean_true(self):
        """Test boolean TRUE default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row("active", 1, "boolean", "bool", column_default="true")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value is True

    def test_default_boolean_false(self):
        """Test boolean FALSE default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row("active", 1, "boolean", "bool", column_default="false")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value is False

    def test_default_float_value(self):
        """Test floating point default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "rate", 1, "double precision", "float8", column_default="3.14"
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

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
                make_column_row("offset", 1, "integer", "int4", column_default="-10")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == -10

    def test_default_negative_in_parens(self):
        """Test negative number in parentheses default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "offset", 1, "integer", "int4", column_default="(-42)::integer"
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == -42

    def test_default_negative_float_in_parens(self):
        """Test negative float in parentheses default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "temp", 1, "numeric", "numeric", column_default="(-273.15)"
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == -273.15

    def test_default_escaped_string(self):
        """Test string with escaped quotes default value."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "greeting",
                    1,
                    "text",
                    "text",
                    column_default="'Hello ''World'''::text",
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == "Hello 'World'"

    def test_default_complex_expression_warning(self):
        """Test complex expression emits warning."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "computed",
                    1,
                    "integer",
                    "int4",
                    column_default="CASE WHEN true THEN 1 ELSE 0 END",
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any(
                "could not be parsed" in str(warning.message).lower() for warning in w
            )

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 0

    def test_default_current_timestamp_warning(self):
        """Test current_timestamp function emits warning."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "created_at",
                    1,
                    "timestamp with time zone",
                    "timestamptz",
                    column_default="current_timestamp",
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("current_" in str(warning.message) for warning in w)

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 0


# ---- Array Type Edge Cases ---------------------------------------------------


class TestPostgreSqlLoaderArrayEdgeCases:
    """Test edge cases in array type handling."""

    def test_array_fallback_from_udt_name(self):
        """Test array element type parsed from udt_name when not in pg_catalog."""
        query_results = {
            "information_schema.columns": [
                make_column_row("numbers", 1, "ARRAY", "_int4")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],  # Empty - no pg_catalog info
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        assert isinstance(spec.columns[0].type, ytypes.Array)
        assert spec.columns[0].type.element == ytypes.Integer(bits=32, signed=True)

    def test_array_unknown_element_type_coerces(self):
        """Test array with unknown element type coerces to fallback."""
        query_results = {
            "information_schema.columns": [
                make_column_row("weird_array", 1, "ARRAY", "_unknown_type")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [("weird_array", "unknown_type", 1)],
            "pg_catalog.pg_depend": [],
            "pg_catalog.pg_type": [],  # Not a composite type
        }
        conn = MockConnection(query_results)
        config = SqlLoaderConfig(mode="coerce", fallback_type=ytypes.String())
        loader = PostgreSqlLoader(conn, config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("unknown_type" in str(warning.message).lower() for warning in w)

        assert isinstance(spec.columns[0].type, ytypes.Array)
        assert spec.columns[0].type.element == ytypes.String()


# ---- Composite Type Edge Cases -----------------------------------------------


class TestPostgreSqlLoaderCompositeEdgeCases:
    """Test edge cases in composite type handling."""

    def test_composite_with_not_null_field(self):
        """Test composite type with NOT NULL field."""
        query_results = {
            "information_schema.columns": [
                make_column_row("point", 1, "USER-DEFINED", "point_type")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
            "pg_catalog.pg_type": [
                ("x", 1, "float8", True),  # NOT NULL
                ("y", 2, "float8", True),  # NOT NULL
            ],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        struct_type = spec.columns[0].type
        assert isinstance(struct_type, ytypes.Struct)
        assert len(struct_type.fields) == 2

        # Check NOT NULL constraints on struct fields
        for field in struct_type.fields:
            assert any(isinstance(c, NotNullConstraint) for c in field.constraints)

    def test_composite_with_unsupported_field_type_coerces(self):
        """Test composite type with unsupported field type coerces."""
        query_results = {
            "information_schema.columns": [
                make_column_row("data", 1, "USER-DEFINED", "complex_type")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
            "pg_catalog.pg_type": [
                ("name", 1, "text", False),
                ("weird_field", 2, "money", False),  # Unsupported type
            ],
        }
        conn = MockConnection(query_results)
        config = SqlLoaderConfig(mode="coerce", fallback_type=ytypes.String())
        loader = PostgreSqlLoader(conn, config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            # Should emit warning for unsupported field type
            assert any("money" in str(warning.message).lower() for warning in w)

        struct_type = spec.columns[0].type
        assert isinstance(struct_type, ytypes.Struct)
        # The unsupported field should be coerced to String
        weird_field = next(f for f in struct_type.fields if f.name == "weird_field")
        assert weird_field.type == ytypes.String()


# ---- UNIQUE Constraint Warning Tests -----------------------------------------


class TestPostgreSqlLoaderUniqueConstraintWarning:
    """Test UNIQUE constraint warning handling."""

    def test_unique_constraint_emits_warning(self):
        """Test that UNIQUE constraints emit a warning."""
        query_results = {
            "information_schema.columns": [make_column_row("email", 1, "text", "text")],
            "information_schema.table_constraints": [
                ("uq_email", "UNIQUE", "email", 1, None, None, None)
            ],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("unique" in str(warning.message).lower() for warning in w)

        # Spec should still load successfully
        assert len(spec.columns) == 1


# ---- Generation Expression Parsing Tests -------------------------------------


class TestPostgreSqlLoaderGenerationExpressionParsing:
    """Test generation expression parsing edge cases."""

    def test_unparseable_generation_expression_warning(self):
        """Test completely unparseable generation expression emits warning."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "weird_col",
                    1,
                    "text",
                    "text",
                    is_generated="ALWAYS",
                    # This expression can't be parsed - starts with special char
                    generation_expression="@#$%^&*()",
                ),
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            # Should emit warning about unparseable expression
            assert any("could not parse" in str(warning.message).lower() for warning in w)

        # No generated_as should be set
        assert spec.columns[0].generated_as is None


# ---- _safe_int Function Tests ------------------------------------------------


class TestSafeIntFunction:
    """Test the _safe_int helper function."""

    def test_safe_int_with_valid_string(self):
        """Test _safe_int with valid numeric string."""
        # This is tested indirectly via identity columns
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "id",
                    1,
                    "integer",
                    "int4",
                    is_nullable="NO",
                    is_identity="YES",
                    identity_generation="ALWAYS",
                    identity_start=1,
                    identity_increment=1,
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        identity_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, IdentityConstraint)
        ]
        assert len(identity_constraints) == 1
        assert identity_constraints[0].start == 1
        assert identity_constraints[0].increment == 1

    def test_safe_int_with_invalid_value(self):
        """Test _safe_int returns None for invalid values."""
        query_results = {
            "information_schema.columns": [
                make_column_row(
                    "id",
                    1,
                    "integer",
                    "int4",
                    is_nullable="NO",
                    is_identity="YES",
                    identity_generation="BY DEFAULT",
                    identity_start=None,  # None value
                    identity_increment=None,  # None value
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        identity_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, IdentityConstraint)
        ]
        assert len(identity_constraints) == 1
        # start and increment should be None when input is None
        assert identity_constraints[0].start is None
        assert identity_constraints[0].increment is None


# ---- Domain Type Base Type Unsupported Warning Test --------------------------


class TestPostgreSqlLoaderDomainTypeBaseTypeWarning:
    """Test domain type with unsupported base type."""

    def test_domain_with_unsupported_base_type(self):
        """Test domain type with unsupported base type emits warning."""

        class DomainMockCursor(MockCursor):
            """Mock cursor that handles domain type queries."""

            DOMAIN_QUERY_COLUMNS = ["base_type", "type_length"]

            def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
                if "typtype = 'd'" in query:
                    # Domain type query - return domain with unsupported base
                    results = self._query_results.get("pg_domain", [])
                    self._current_results = results
                    if results:
                        self._description = [(col,) for col in self.DOMAIN_QUERY_COLUMNS]
                    else:
                        self._description = None
                else:
                    super().execute(query, params)

        class DomainMockConnection(MockConnection):
            def cursor(self) -> DomainMockCursor:
                return DomainMockCursor(self._query_results)

        query_results = {
            "information_schema.columns": [
                make_column_row("money_col", 1, "USER-DEFINED", "money_domain")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
            "pg_catalog.pg_type": [],  # Not a composite type
            "pg_domain": [("money", 8)],  # Base type is money (unsupported)
        }
        conn = DomainMockConnection(query_results)
        config = SqlLoaderConfig(mode="coerce", fallback_type=ytypes.String())
        loader = PostgreSqlLoader(conn, config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            # Should emit warning about unsupported base type
            assert any("money_domain" in str(warning.message) for warning in w)

        # Should coerce to fallback since domain base type is unsupported
        assert spec.columns[0].type == ytypes.String()


# ---- Empty Default Value Test ------------------------------------------------


class TestPostgreSqlLoaderEmptyDefault:
    """Test empty default value handling."""

    def test_empty_default_returns_none(self):
        """Test that empty default expression returns no constraint."""
        query_results = {
            "information_schema.columns": [
                make_column_row("col", 1, "text", "text", column_default="")
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        spec = loader.load("test_table")

        default_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 0


# ---- Coerce Mode Without Fallback Tests --------------------------------------


class TestPostgreSqlLoaderCoerceWithoutFallback:
    """Test coerce mode behavior when no fallback_type is specified."""

    def test_coerce_mode_without_fallback_raises_error(self):
        """Test that coerce mode without fallback_type raises UnsupportedFeatureError."""
        query_results = {
            "information_schema.columns": [
                make_column_row("weird_col", 1, "money", "money")  # Unsupported type
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        # Coerce mode but NO fallback_type
        config = SqlLoaderConfig(mode="coerce", fallback_type=None)
        loader = PostgreSqlLoader(conn, config)

        with pytest.raises(UnsupportedFeatureError, match="fallback_type"):
            loader.load("test_table")


# ---- Base SqlLoader Field Serialization Tests --------------------------------


class TestSqlLoaderFieldSerialization:
    """Test base SqlLoader field serialization for Struct fields."""

    def test_serialize_field_with_description(self):
        """Test that field description is serialized."""
        from yads.spec import Field

        query_results: dict[str, list[tuple[Any, ...]]] = {
            "information_schema.columns": [],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        # Create a Field with description
        field = Field(
            name="test_field",
            type=ytypes.String(),
            description="A test field description",
        )

        # Call the internal serialization method
        result = loader._serialize_field_definition(field)

        assert result["name"] == "test_field"
        assert result["description"] == "A test field description"

    def test_serialize_field_with_metadata(self):
        """Test that field metadata is serialized."""
        from yads.spec import Field

        query_results: dict[str, list[tuple[Any, ...]]] = {
            "information_schema.columns": [],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        # Create a Field with metadata
        field = Field(
            name="test_field",
            type=ytypes.Integer(bits=32, signed=True),
            metadata={"custom_key": "custom_value", "another": 123},
        )

        # Call the internal serialization method
        result = loader._serialize_field_definition(field)

        assert result["name"] == "test_field"
        assert result["metadata"] == {"custom_key": "custom_value", "another": 123}

    def test_serialize_field_with_all_attributes(self):
        """Test that all field attributes are serialized together."""
        from yads.spec import Field

        query_results: dict[str, list[tuple[Any, ...]]] = {
            "information_schema.columns": [],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSqlLoader(conn)

        # Create a Field with all attributes
        field = Field(
            name="full_field",
            type=ytypes.String(length=100),
            description="A fully specified field",
            metadata={"source": "test"},
            constraints=[NotNullConstraint()],
        )

        result = loader._serialize_field_definition(field)

        assert result["name"] == "full_field"
        assert result["description"] == "A fully specified field"
        assert result["metadata"] == {"source": "test"}
        assert "constraints" in result
