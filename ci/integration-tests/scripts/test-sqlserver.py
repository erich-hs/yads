#!/usr/bin/env python3
"""SQL Server integration test for SqlServerLoader.

This script validates that the SqlServerLoader can correctly introspect
SQL Server table schemas and convert them to YadsSpec instances.
"""

import sys

import pyodbc

from yads import types as ytypes
from yads.constraints import (
    DefaultConstraint,
    ForeignKeyConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
)
from yads.loaders.sql import SqlServerLoader


def get_connection() -> pyodbc.Connection:
    """Get a connection to the SQL Server test database."""
    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=yads_test;"
        "UID=sa;"
        "PWD=YadsTest123!;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connection_string)


def setup_test_tables(conn: pyodbc.Connection) -> None:
    """Create test tables with various SQL Server types and constraints."""
    cursor = conn.cursor()

    # Drop existing test tables
    cursor.execute("""
        IF OBJECT_ID('dbo.test_constraints', 'U') IS NOT NULL
            DROP TABLE dbo.test_constraints
    """)
    cursor.execute("""
        IF OBJECT_ID('dbo.test_types', 'U') IS NOT NULL
            DROP TABLE dbo.test_types
    """)
    cursor.execute("""
        IF OBJECT_ID('dbo.test_referenced', 'U') IS NOT NULL
            DROP TABLE dbo.test_referenced
    """)
    conn.commit()

    # Create referenced table for foreign key tests
    cursor.execute("""
        CREATE TABLE dbo.test_referenced (
            id BIGINT PRIMARY KEY,
            name NVARCHAR(100) NOT NULL
        )
    """)

    # Create table with various types
    cursor.execute("""
        CREATE TABLE dbo.test_types (
            -- Integer types
            col_tinyint TINYINT,
            col_smallint SMALLINT,
            col_int INT,
            col_bigint BIGINT,

            -- Floating point types
            col_real REAL,
            col_float FLOAT,
            col_decimal DECIMAL(10, 2),

            -- String types - ANSI
            col_char CHAR(10),
            col_varchar VARCHAR(255),
            col_varchar_max VARCHAR(MAX),
            col_text TEXT,

            -- String types - Unicode
            col_nchar NCHAR(10),
            col_nvarchar NVARCHAR(255),
            col_nvarchar_max NVARCHAR(MAX),
            col_ntext NTEXT,

            -- Binary types
            col_binary BINARY(16),
            col_varbinary VARBINARY(256),
            col_varbinary_max VARBINARY(MAX),
            col_image IMAGE,

            -- Boolean type
            col_bit BIT,

            -- Date/Time types
            col_date DATE,
            col_time TIME,
            col_smalldatetime SMALLDATETIME,
            col_datetime DATETIME,
            col_datetime2 DATETIME2,
            col_datetimeoffset DATETIMEOFFSET,

            -- UUID type
            col_uniqueidentifier UNIQUEIDENTIFIER,

            -- Spatial types
            col_geometry GEOMETRY,
            col_geography GEOGRAPHY
        )
    """)

    # Create table with constraints
    cursor.execute("""
        CREATE TABLE dbo.test_constraints (
            -- Primary key (single column)
            id BIGINT PRIMARY KEY,

            -- Identity column
            seq_id INT IDENTITY(1,1),

            -- Not null column
            required_col NVARCHAR(100) NOT NULL,

            -- Default values
            default_string NVARCHAR(50) DEFAULT 'hello',
            default_int INT DEFAULT 42,
            default_float FLOAT DEFAULT 3.14,
            default_null NVARCHAR(50) DEFAULT NULL,

            -- Foreign key
            ref_id BIGINT REFERENCES dbo.test_referenced(id),

            -- Computed column
            upper_required AS UPPER(required_col) PERSISTED
        )
    """)

    conn.commit()
    cursor.close()


def test_basic_type_conversion(conn: pyodbc.Connection) -> None:
    """Test basic type conversion from SQL Server to YadsTypes."""
    print("\n" + "-" * 60)
    print("Test: Basic Type Conversion")
    print("-" * 60)

    loader = SqlServerLoader(conn)
    spec = loader.load("test_types")

    print(f"  Loaded spec: {spec.name}")
    print(f"  Columns: {len(spec.columns)}")

    # Define expected types for each column
    expected_types: dict[str, ytypes.YadsType] = {
        # Integers
        "col_tinyint": ytypes.Integer(bits=8, signed=False),
        "col_smallint": ytypes.Integer(bits=16, signed=True),
        "col_int": ytypes.Integer(bits=32, signed=True),
        "col_bigint": ytypes.Integer(bits=64, signed=True),
        # Floats
        "col_real": ytypes.Float(bits=32),
        "col_float": ytypes.Float(bits=64),
        "col_decimal": ytypes.Decimal(precision=10, scale=2),
        # Strings - ANSI
        "col_char": ytypes.String(length=10),
        "col_varchar": ytypes.String(length=255),
        "col_varchar_max": ytypes.String(),
        "col_text": ytypes.String(),
        # Strings - Unicode
        "col_nchar": ytypes.String(length=10),
        "col_nvarchar": ytypes.String(length=255),
        "col_nvarchar_max": ytypes.String(),
        "col_ntext": ytypes.String(),
        # Binary
        "col_binary": ytypes.Binary(length=16),
        "col_varbinary": ytypes.Binary(length=256),
        "col_varbinary_max": ytypes.Binary(),
        "col_image": ytypes.Binary(),
        # Boolean
        "col_bit": ytypes.Boolean(),
        # Date/Time
        "col_date": ytypes.Date(bits=32),
        "col_time": ytypes.Time(unit=ytypes.TimeUnit.US),
        "col_smalldatetime": ytypes.TimestampNTZ(unit=ytypes.TimeUnit.S),
        "col_datetime": ytypes.TimestampNTZ(unit=ytypes.TimeUnit.MS),
        "col_datetime2": ytypes.TimestampNTZ(unit=ytypes.TimeUnit.US),
        "col_datetimeoffset": ytypes.TimestampTZ(unit=ytypes.TimeUnit.US, tz="UTC"),
        # UUID
        "col_uniqueidentifier": ytypes.UUID(),
        # Spatial
        "col_geometry": ytypes.Geometry(),
        "col_geography": ytypes.Geography(),
    }

    # Check expected types
    columns_by_name = {col.name: col for col in spec.columns}
    errors = []

    for col_name, expected_type in expected_types.items():
        if col_name not in columns_by_name:
            errors.append(f"  X Column '{col_name}' not found in spec")
            continue

        actual_type = columns_by_name[col_name].type
        if actual_type != expected_type:
            errors.append(
                f"  X Column '{col_name}': expected {expected_type}, got {actual_type}"
            )
        else:
            print(f"  + {col_name}: {actual_type}")

    if errors:
        for error in errors:
            print(error)
        raise AssertionError(f"Type conversion errors: {len(errors)}")

    print("+ All basic types converted correctly")


def test_constraints(conn: pyodbc.Connection) -> None:
    """Test constraint loading."""
    print("\n" + "-" * 60)
    print("Test: Constraint Loading")
    print("-" * 60)

    loader = SqlServerLoader(conn)
    spec = loader.load("test_constraints")

    columns_by_name = {col.name: col for col in spec.columns}

    # Test Primary Key
    col_id = columns_by_name["id"]
    pk_constraints = [
        c for c in col_id.constraints if isinstance(c, PrimaryKeyConstraint)
    ]
    assert len(pk_constraints) == 1, "Expected PRIMARY KEY constraint on 'id'"
    print("  + Primary key constraint detected")

    # Test Identity column
    col_seq_id = columns_by_name["seq_id"]
    identity_constraints = [
        c for c in col_seq_id.constraints if isinstance(c, IdentityConstraint)
    ]
    assert len(identity_constraints) == 1, "Expected IDENTITY constraint on 'seq_id'"
    assert identity_constraints[0].start == 1
    assert identity_constraints[0].increment == 1
    print("  + Identity constraint detected (seed=1, increment=1)")

    # Test NOT NULL
    col_required = columns_by_name["required_col"]
    not_null_constraints = [
        c for c in col_required.constraints if isinstance(c, NotNullConstraint)
    ]
    assert len(not_null_constraints) == 1, (
        "Expected NOT NULL constraint on 'required_col'"
    )
    print("  + NOT NULL constraint detected")

    # Test DEFAULT values
    col_default_string = columns_by_name["default_string"]
    default_constraints = [
        c for c in col_default_string.constraints if isinstance(c, DefaultConstraint)
    ]
    assert len(default_constraints) == 1, (
        "Expected DEFAULT constraint on 'default_string'"
    )
    assert default_constraints[0].value == "hello"
    print("  + DEFAULT string value detected")

    col_default_int = columns_by_name["default_int"]
    default_int_constraints = [
        c for c in col_default_int.constraints if isinstance(c, DefaultConstraint)
    ]
    assert len(default_int_constraints) == 1
    assert default_int_constraints[0].value == 42
    print("  + DEFAULT integer value detected")

    col_default_float = columns_by_name["default_float"]
    default_float_constraints = [
        c for c in col_default_float.constraints if isinstance(c, DefaultConstraint)
    ]
    assert len(default_float_constraints) == 1
    assert default_float_constraints[0].value == 3.14
    print("  + DEFAULT float value detected")

    # Test Foreign Key
    col_ref_id = columns_by_name["ref_id"]
    fk_constraints = [
        c for c in col_ref_id.constraints if isinstance(c, ForeignKeyConstraint)
    ]
    assert len(fk_constraints) == 1, "Expected FOREIGN KEY constraint on 'ref_id'"
    assert fk_constraints[0].references.table == "test_referenced"
    print("  + Foreign key constraint detected")

    print("+ All constraints loaded correctly")


def test_computed_columns(conn: pyodbc.Connection) -> None:
    """Test computed column handling."""
    print("\n" + "-" * 60)
    print("Test: Computed Columns")
    print("-" * 60)

    loader = SqlServerLoader(conn)
    spec = loader.load("test_constraints")

    columns_by_name = {col.name: col for col in spec.columns}

    col_upper = columns_by_name["upper_required"]
    assert col_upper.generated_as is not None, (
        "Expected generated_as for 'upper_required'"
    )
    assert col_upper.generated_as.column == "required_col"
    assert col_upper.generated_as.transform == "upper"
    print(f"  + Computed column detected: {col_upper.generated_as}")

    print("+ Computed columns handled correctly")


def test_spec_metadata(conn: pyodbc.Connection) -> None:
    """Test spec metadata generation."""
    print("\n" + "-" * 60)
    print("Test: Spec Metadata")
    print("-" * 60)

    loader = SqlServerLoader(conn)

    # Test default name (fully qualified: catalog.schema.table)
    spec = loader.load("test_types")
    assert spec.name == "yads_test.dbo.test_types"
    print(f"  + Default name: {spec.name}")

    # Test custom name
    spec = loader.load("test_types", name="custom.schema.test")
    assert spec.name == "custom.schema.test"
    print(f"  + Custom name: {spec.name}")

    # Test version
    spec = loader.load("test_types", version=5)
    assert spec.version == 5
    print(f"  + Version: {spec.version}")

    # Test description
    spec = loader.load("test_types", description="Test table for types")
    assert spec.description == "Test table for types"
    print(f"  + Description: {spec.description}")

    print("+ Spec metadata generated correctly")


def main() -> int:
    """Run all SQL Server integration tests."""
    print("=" * 60)
    print("SQL Server Loader Integration Test")
    print("=" * 60)

    print("\n> Connecting to SQL Server...")
    try:
        conn = get_connection()
        print("+ Connected to SQL Server")
    except Exception as e:
        print(f"X Failed to connect: {e}")
        return 1

    try:
        print("\n> Setting up test tables...")
        setup_test_tables(conn)
        print("+ Test tables created")

        # Run all tests
        test_basic_type_conversion(conn)
        test_constraints(conn)
        test_computed_columns(conn)
        test_spec_metadata(conn)

        print("\n" + "=" * 60)
        print("+ All SQL Server integration tests PASSED")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\nX Test FAILED: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
