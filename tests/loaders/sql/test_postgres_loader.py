"""Tests for PostgreSQLLoader."""

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
from yads.loaders.sql import PostgreSQLLoader, PostgreSQLLoaderConfig


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
        if "information_schema.columns" in query:
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


class TestPostgreSQLLoaderTypeConversion:
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
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

        spec = loader.load("test_table")

        assert spec.columns[0].type == ytypes.Interval(
            interval_start=ytypes.IntervalTimeUnit.YEAR,
            interval_end=ytypes.IntervalTimeUnit.MONTH,
        )


# ---- Constraint Tests --------------------------------------------------------


class TestPostgreSQLLoaderConstraints:
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
        loader = PostgreSQLLoader(conn)

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
                    identity_start="1",
                    identity_increment="1",
                )
            ],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

        spec = loader.load("test_table")

        pk_constraints = [
            c for c in spec.columns[0].constraints if isinstance(c, PrimaryKeyConstraint)
        ]
        assert len(pk_constraints) == 1


# ---- Unsupported Type Tests --------------------------------------------------


class TestPostgreSQLLoaderUnsupportedTypes:
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
        config = PostgreSQLLoaderConfig(mode="raise")
        loader = PostgreSQLLoader(conn, config)

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
        config = PostgreSQLLoaderConfig(mode="coerce", fallback_type=ytypes.String())
        loader = PostgreSQLLoader(conn, config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load("test_table")

            assert any("money" in str(warning.message).lower() for warning in w)

        assert spec.columns[0].type == ytypes.String()


# ---- Array Type Tests --------------------------------------------------------


class TestPostgreSQLLoaderArrayTypes:
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
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

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


class TestPostgreSQLLoaderCompositeTypes:
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
        loader = PostgreSQLLoader(conn)

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


class TestPostgreSQLLoaderErrors:
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
        loader = PostgreSQLLoader(conn)

        with pytest.raises(LoaderError, match="not found"):
            loader.load("nonexistent_table")


# ---- Spec Metadata Tests -----------------------------------------------------


class TestPostgreSQLLoaderSpecMetadata:
    """Test spec metadata generation."""

    def test_default_spec_name(self):
        """Test default spec name from schema.table."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "integer", "int4")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSQLLoader(conn)

        spec = loader.load("users", schema="public")

        assert spec.name == "public.users"

    def test_custom_spec_name(self):
        """Test custom spec name."""
        query_results = {
            "information_schema.columns": [make_column_row("id", 1, "integer", "int4")],
            "information_schema.table_constraints": [],
            "pg_catalog.pg_attribute": [],
            "pg_catalog.pg_depend": [],
        }
        conn = MockConnection(query_results)
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

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
        loader = PostgreSQLLoader(conn)

        spec = loader.load("users", description="User accounts table")

        assert spec.description == "User accounts table"


# ---- Config Tests ------------------------------------------------------------


class TestPostgreSQLLoaderConfig:
    """Test loader configuration."""

    def test_invalid_fallback_type_raises_error(self):
        """Test that invalid fallback type raises error."""
        with pytest.raises(Exception):  # LoaderConfigError
            PostgreSQLLoaderConfig(fallback_type=ytypes.Integer())

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
        config = PostgreSQLLoaderConfig(fallback_type=ytypes.String())
        loader = PostgreSQLLoader(conn, config)

        # Override to raise mode
        with pytest.raises(UnsupportedFeatureError):
            loader.load("test_table", mode="raise")
