#!/usr/bin/env python3
"""PostgreSQL integration test for PostgreSQLLoader.

This script validates that the PostgreSQLLoader can correctly introspect
PostgreSQL table schemas and convert them to YadsSpec instances.
"""

import sys

import psycopg2
from psycopg2.extensions import connection as PgConnection

from yads import types as ytypes
from yads.constraints import (
    DefaultConstraint,
    ForeignKeyConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
)
from yads.loaders.sql import PostgreSQLLoader


def get_connection() -> PgConnection:
    """Get a connection to the PostgreSQL test database."""
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="yads_test",
        user="yads",
        password="yads",
    )


def setup_test_tables(conn: PgConnection) -> None:
    """Create test tables with various PostgreSQL types and constraints."""
    with conn.cursor() as cur:
        # Drop existing test tables
        cur.execute("DROP TABLE IF EXISTS test_types CASCADE")
        cur.execute("DROP TABLE IF EXISTS test_constraints CASCADE")
        cur.execute("DROP TABLE IF EXISTS test_referenced CASCADE")
        cur.execute("DROP TYPE IF EXISTS address_type CASCADE")

        # Create a composite type for testing
        cur.execute("""
            CREATE TYPE address_type AS (
                street TEXT,
                city TEXT,
                zip_code VARCHAR(10)
            )
        """)

        # Create referenced table for foreign key tests
        cur.execute("""
            CREATE TABLE test_referenced (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)

        # Create table with various types
        cur.execute("""
            CREATE TABLE test_types (
                -- Integer types
                col_smallint SMALLINT,
                col_integer INTEGER,
                col_bigint BIGINT,

                -- Floating point types
                col_real REAL,
                col_double DOUBLE PRECISION,
                col_numeric NUMERIC(10, 2),

                -- String types
                col_varchar VARCHAR(255),
                col_char CHAR(10),
                col_text TEXT,

                -- Binary type
                col_bytea BYTEA,

                -- Boolean type
                col_boolean BOOLEAN,

                -- Date/Time types
                col_date DATE,
                col_time TIME,
                col_time_tz TIME WITH TIME ZONE,
                col_timestamp TIMESTAMP,
                col_timestamp_tz TIMESTAMP WITH TIME ZONE,
                col_interval INTERVAL,
                col_interval_ym INTERVAL YEAR TO MONTH,

                -- UUID type
                col_uuid UUID,

                -- JSON types
                col_json JSON,
                col_jsonb JSONB,

                -- Array types
                col_array_int INTEGER[],
                col_array_text TEXT[],
                col_array_2d INTEGER[][],

                -- Composite type
                col_composite address_type
            )
        """)

        # Create table with constraints
        cur.execute("""
            CREATE TABLE test_constraints (
                -- Primary key (single column)
                id BIGINT PRIMARY KEY,

                -- Identity column
                seq_id INTEGER GENERATED ALWAYS AS IDENTITY,

                -- Serial column (auto-increment)
                auto_id SERIAL,

                -- Not null column
                required_col TEXT NOT NULL,

                -- Default values
                default_string TEXT DEFAULT 'hello',
                default_int INTEGER DEFAULT 42,
                default_bool BOOLEAN DEFAULT true,
                default_null TEXT DEFAULT NULL,

                -- Foreign key
                ref_id BIGINT REFERENCES test_referenced(id),

                -- Generated column
                upper_required TEXT GENERATED ALWAYS AS (upper(required_col)) STORED
            )
        """)

        conn.commit()


def test_basic_type_conversion(conn: PgConnection) -> None:
    """Test basic type conversion from PostgreSQL to YadsTypes."""
    print("\n" + "-" * 60)
    print("Test: Basic Type Conversion")
    print("-" * 60)

    loader = PostgreSQLLoader(conn)
    spec = loader.load("test_types")

    print(f"✓ Loaded spec: {spec.name}")
    print(f"  Columns: {len(spec.columns)}")

    # Define expected types for each column
    expected_types: dict[str, ytypes.YadsType] = {
        # Integers
        "col_smallint": ytypes.Integer(bits=16, signed=True),
        "col_integer": ytypes.Integer(bits=32, signed=True),
        "col_bigint": ytypes.Integer(bits=64, signed=True),
        # Floats
        "col_real": ytypes.Float(bits=32),
        "col_double": ytypes.Float(bits=64),
        "col_numeric": ytypes.Decimal(precision=10, scale=2),
        # Strings
        "col_varchar": ytypes.String(length=255),
        "col_char": ytypes.String(length=10),
        "col_text": ytypes.String(),
        # Binary
        "col_bytea": ytypes.Binary(),
        # Boolean
        "col_boolean": ytypes.Boolean(),
        # Date/Time
        "col_date": ytypes.Date(bits=32),
        "col_time": ytypes.Time(unit=ytypes.TimeUnit.US),
        "col_time_tz": ytypes.Time(unit=ytypes.TimeUnit.US),  # TZ info lost
        "col_timestamp": ytypes.TimestampNTZ(unit=ytypes.TimeUnit.US),
        "col_timestamp_tz": ytypes.TimestampTZ(unit=ytypes.TimeUnit.US, tz="UTC"),
        # UUID
        "col_uuid": ytypes.UUID(),
        # JSON
        "col_json": ytypes.JSON(),
        "col_jsonb": ytypes.JSON(),
    }

    # Check expected types
    columns_by_name = {col.name: col for col in spec.columns}
    errors = []

    for col_name, expected_type in expected_types.items():
        if col_name not in columns_by_name:
            errors.append(f"  ✗ Column '{col_name}' not found in spec")
            continue

        actual_type = columns_by_name[col_name].type
        if actual_type != expected_type:
            errors.append(
                f"  ✗ Column '{col_name}': expected {expected_type}, got {actual_type}"
            )
        else:
            print(f"  ✓ {col_name}: {actual_type}")

    if errors:
        for error in errors:
            print(error)
        raise AssertionError(f"Type conversion errors: {len(errors)}")

    print("✓ All basic types converted correctly")


def test_array_types(conn: PgConnection) -> None:
    """Test array type conversion."""
    print("\n" + "-" * 60)
    print("Test: Array Type Conversion")
    print("-" * 60)

    loader = PostgreSQLLoader(conn)
    spec = loader.load("test_types")

    columns_by_name = {col.name: col for col in spec.columns}

    # Test 1D array
    col_array_int = columns_by_name["col_array_int"]
    assert isinstance(col_array_int.type, ytypes.Array), (
        f"Expected Array, got {type(col_array_int.type)}"
    )
    assert col_array_int.type.element == ytypes.Integer(bits=32, signed=True)
    print(f"  ✓ col_array_int: {col_array_int.type}")

    # Test text array
    col_array_text = columns_by_name["col_array_text"]
    assert isinstance(col_array_text.type, ytypes.Array)
    assert col_array_text.type.element == ytypes.String()
    print(f"  ✓ col_array_text: {col_array_text.type}")

    # Test 2D array (should be nested Array)
    col_array_2d = columns_by_name["col_array_2d"]
    assert isinstance(col_array_2d.type, ytypes.Array)
    assert isinstance(col_array_2d.type.element, ytypes.Array)
    print(f"  ✓ col_array_2d: {col_array_2d.type}")

    print("✓ All array types converted correctly")


def test_composite_types(conn: PgConnection) -> None:
    """Test composite type conversion."""
    print("\n" + "-" * 60)
    print("Test: Composite Type Conversion")
    print("-" * 60)

    loader = PostgreSQLLoader(conn)
    spec = loader.load("test_types")

    columns_by_name = {col.name: col for col in spec.columns}

    col_composite = columns_by_name["col_composite"]
    assert isinstance(col_composite.type, ytypes.Struct), (
        f"Expected Struct, got {type(col_composite.type)}"
    )

    # Check fields
    field_names = [f.name for f in col_composite.type.fields]
    assert "street" in field_names
    assert "city" in field_names
    assert "zip_code" in field_names
    print(f"  ✓ col_composite: Struct with fields {field_names}")

    print("✓ Composite types converted correctly")


def test_interval_types(conn: PgConnection) -> None:
    """Test interval type conversion."""
    print("\n" + "-" * 60)
    print("Test: Interval Type Conversion")
    print("-" * 60)

    loader = PostgreSQLLoader(conn)
    spec = loader.load("test_types")

    columns_by_name = {col.name: col for col in spec.columns}

    # Default interval
    col_interval = columns_by_name["col_interval"]
    assert isinstance(col_interval.type, ytypes.Interval)
    print(f"  ✓ col_interval: {col_interval.type}")

    # Year to Month interval
    col_interval_ym = columns_by_name["col_interval_ym"]
    assert isinstance(col_interval_ym.type, ytypes.Interval)
    assert col_interval_ym.type.interval_start == ytypes.IntervalTimeUnit.YEAR
    assert col_interval_ym.type.interval_end == ytypes.IntervalTimeUnit.MONTH
    print(f"  ✓ col_interval_ym: {col_interval_ym.type}")

    print("✓ Interval types converted correctly")


def test_constraints(conn: PgConnection) -> None:
    """Test constraint loading."""
    print("\n" + "-" * 60)
    print("Test: Constraint Loading")
    print("-" * 60)

    loader = PostgreSQLLoader(conn)
    spec = loader.load("test_constraints")

    columns_by_name = {col.name: col for col in spec.columns}

    # Test Primary Key
    col_id = columns_by_name["id"]
    pk_constraints = [
        c for c in col_id.constraints if isinstance(c, PrimaryKeyConstraint)
    ]
    assert len(pk_constraints) == 1, "Expected PRIMARY KEY constraint on 'id'"
    print("  ✓ Primary key constraint detected")

    # Test Identity column
    col_seq_id = columns_by_name["seq_id"]
    identity_constraints = [
        c for c in col_seq_id.constraints if isinstance(c, IdentityConstraint)
    ]
    assert len(identity_constraints) == 1, "Expected IDENTITY constraint on 'seq_id'"
    assert identity_constraints[0].always is True
    print("  ✓ Identity constraint detected (GENERATED ALWAYS)")

    # Test Serial column (converted to Identity)
    col_auto_id = columns_by_name["auto_id"]
    serial_constraints = [
        c for c in col_auto_id.constraints if isinstance(c, IdentityConstraint)
    ]
    assert len(serial_constraints) == 1, "Expected IDENTITY constraint for SERIAL column"
    print("  ✓ Serial column detected as Identity")

    # Test NOT NULL
    col_required = columns_by_name["required_col"]
    not_null_constraints = [
        c for c in col_required.constraints if isinstance(c, NotNullConstraint)
    ]
    assert len(not_null_constraints) == 1, (
        "Expected NOT NULL constraint on 'required_col'"
    )
    print("  ✓ NOT NULL constraint detected")

    # Test DEFAULT values
    col_default_string = columns_by_name["default_string"]
    default_constraints = [
        c for c in col_default_string.constraints if isinstance(c, DefaultConstraint)
    ]
    assert len(default_constraints) == 1, (
        "Expected DEFAULT constraint on 'default_string'"
    )
    assert default_constraints[0].value == "hello"
    print("  ✓ DEFAULT string value detected")

    col_default_int = columns_by_name["default_int"]
    default_int_constraints = [
        c for c in col_default_int.constraints if isinstance(c, DefaultConstraint)
    ]
    assert len(default_int_constraints) == 1
    assert default_int_constraints[0].value == 42
    print("  ✓ DEFAULT integer value detected")

    col_default_bool = columns_by_name["default_bool"]
    default_bool_constraints = [
        c for c in col_default_bool.constraints if isinstance(c, DefaultConstraint)
    ]
    assert len(default_bool_constraints) == 1
    assert default_bool_constraints[0].value is True
    print("  ✓ DEFAULT boolean value detected")

    # Test Foreign Key
    col_ref_id = columns_by_name["ref_id"]
    fk_constraints = [
        c for c in col_ref_id.constraints if isinstance(c, ForeignKeyConstraint)
    ]
    assert len(fk_constraints) == 1, "Expected FOREIGN KEY constraint on 'ref_id'"
    assert fk_constraints[0].references.table == "test_referenced"
    print("  ✓ Foreign key constraint detected")

    print("✓ All constraints loaded correctly")


def test_generated_columns(conn: PgConnection) -> None:
    """Test generated column handling."""
    print("\n" + "-" * 60)
    print("Test: Generated Columns")
    print("-" * 60)

    loader = PostgreSQLLoader(conn)
    spec = loader.load("test_constraints")

    columns_by_name = {col.name: col for col in spec.columns}

    col_upper = columns_by_name["upper_required"]
    assert col_upper.generated_as is not None, (
        "Expected generated_as for 'upper_required'"
    )
    assert col_upper.generated_as.column == "required_col"
    assert col_upper.generated_as.transform == "upper"
    print(f"  ✓ Generated column detected: {col_upper.generated_as}")

    print("✓ Generated columns handled correctly")


def test_spec_metadata(conn: PgConnection) -> None:
    """Test spec metadata generation."""
    print("\n" + "-" * 60)
    print("Test: Spec Metadata")
    print("-" * 60)

    loader = PostgreSQLLoader(conn)

    # Test default name (fully qualified: catalog.schema.table)
    spec = loader.load("test_types")
    assert spec.name == "yads_test.public.test_types"
    print(f"  ✓ Default name: {spec.name}")

    # Test custom name
    spec = loader.load("test_types", name="custom.schema.test")
    assert spec.name == "custom.schema.test"
    print(f"  ✓ Custom name: {spec.name}")

    # Test version
    spec = loader.load("test_types", version=5)
    assert spec.version == 5
    print(f"  ✓ Version: {spec.version}")

    # Test description
    spec = loader.load("test_types", description="Test table for types")
    assert spec.description == "Test table for types"
    print(f"  ✓ Description: {spec.description}")

    print("✓ Spec metadata generated correctly")


def main() -> int:
    """Run all PostgreSQL integration tests."""
    print("=" * 60)
    print("PostgreSQL Loader Integration Test")
    print("=" * 60)

    print("\n▶ Connecting to PostgreSQL...")
    try:
        conn = get_connection()
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return 1

    try:
        print("\n▶ Setting up test tables...")
        setup_test_tables(conn)
        print("✓ Test tables created")

        # Run all tests
        test_basic_type_conversion(conn)
        test_array_types(conn)
        test_composite_types(conn)
        test_interval_types(conn)
        test_constraints(conn)
        test_generated_columns(conn)
        test_spec_metadata(conn)

        print("\n" + "=" * 60)
        print("✓ All PostgreSQL integration tests PASSED")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n✗ Test FAILED: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
