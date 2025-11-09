#!/usr/bin/env python3
"""DuckDB integration test for SQL converters.

This script validates that the DuckdbSQLConverter can generate valid DDL
that executes successfully in a DuckDB environment.
"""

import sys
from pathlib import Path

import duckdb
import yads
from yads.converters.sql import DuckdbSQLConverter


def test_duckdb_sql_converter():
    """Test DuckDB SQL converter with full_spec fixture."""
    print("=" * 60)
    print("DuckDB SQL Converter Integration Test")
    print("=" * 60)

    fixture_path = Path(__file__).parents[3] / "tests/fixtures/spec/valid/full_spec.yaml"
    print(f"\n▶ Loading spec from: {fixture_path}")

    spec = yads.from_yaml(str(fixture_path))
    print(f"✓ Loaded spec: {spec.name} (version {spec.version})")

    print("\n▶ Generating DDL with DuckdbSQLConverter...")
    from yads.converters.sql.ast_converter import SQLGlotConverterConfig

    ast_config = SQLGlotConverterConfig(ignore_catalog=True, ignore_database=True)
    converter = DuckdbSQLConverter(mode="coerce", ast_config=ast_config)
    ddl = converter.convert(spec, pretty=True)

    print("\n--- Generated DDL ---")
    print(ddl)
    print("--- End DDL ---\n")

    print("▶ Creating DuckDB in-memory connection...")
    conn = duckdb.connect(":memory:")

    print("▶ Installing DuckDB spatial extension...")
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        print("✓ Spatial extension loaded")
    except Exception as e:
        print(f"⚠ Warning: Could not load spatial extension: {e}")

    # Create referenced table for foreign key constraint
    print("▶ Creating referenced table for foreign key...")
    try:
        conn.execute("CREATE TABLE other_table (id BIGINT PRIMARY KEY)")
        print("✓ Referenced table created")
    except Exception as e:
        print(f"⚠ Warning: Could not create referenced table: {e}")

    print("▶ Executing DDL...")
    try:
        conn.execute(ddl)
        print("✓ DDL executed successfully!")
    except Exception as e:
        print(f"✗ DDL execution failed: {e}")
        raise
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("✓ DuckDB integration test PASSED")
    print("=" * 60)


if __name__ == "__main__":
    try:
        test_duckdb_sql_converter()
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Test FAILED: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)
