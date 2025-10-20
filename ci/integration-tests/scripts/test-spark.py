#!/usr/bin/env python3
"""Spark integration test for SQL and PySpark converters.

This script validates that:
1. SparkSQLConverter can generate valid DDL for Spark + Iceberg
2. PySparkConverter can generate valid StructType schemas
"""

import sys
from pathlib import Path

import yads
from yads.converters.sql import SparkSQLConverter
from yads.converters import PySparkConverter


def test_spark_sql_converter():
    """Test Spark SQL converter with iceberg_spec fixture."""
    print("=" * 60)
    print("Spark SQL Converter Integration Test")
    print("=" * 60)

    from pyspark.sql import SparkSession

    fixture_path = (
        Path(__file__).parents[3] / "tests/fixtures/spec/valid/iceberg_spec.yaml"
    )
    print(f"\n▶ Loading spec from: {fixture_path}")

    spec = yads.from_yaml(str(fixture_path))
    print(f"✓ Loaded spec: {spec.name} (version {spec.version})")

    print("\n▶ Generating DDL with SparkSQLConverter...")
    from yads.converters.sql.ast_converter import SQLGlotConverterConfig

    ast_config = SQLGlotConverterConfig(ignore_catalog=True, ignore_database=True)
    converter = SparkSQLConverter(mode="coerce", ast_config=ast_config)
    ddl = converter.convert(spec, pretty=True)

    print("\n--- Generated DDL ---")
    print(ddl)
    print("--- End DDL ---\n")

    print("▶ Creating SparkSession with Iceberg Hadoop catalog...")
    spark = (
        SparkSession.builder.appName("YadsIntegrationTest")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/iceberg-warehouse")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )

    print("▶ Executing DDL...")
    try:
        spark.sql(ddl)
        print("✓ DDL executed successfully!")
    except Exception as e:
        print(f"✗ DDL execution failed: {e}")
        raise
    finally:
        spark.stop()

    print("\n" + "=" * 60)
    print("✓ Spark SQL integration test PASSED")
    print("=" * 60)


def test_pyspark_converter():
    """Test PySpark converter with iceberg_spec fixture."""
    print("\n" + "=" * 60)
    print("PySpark Converter Integration Test")
    print("=" * 60)

    from pyspark.sql import SparkSession

    fixture_path = (
        Path(__file__).parents[3] / "tests/fixtures/spec/valid/iceberg_spec.yaml"
    )
    print(f"\n▶ Loading spec from: {fixture_path}")

    spec = yads.from_yaml(str(fixture_path))
    print(f"✓ Loaded spec: {spec.name} (version {spec.version})")

    print("\n▶ Generating schema with PySparkConverter...")
    from yads.converters import PySparkConverterConfig

    config = PySparkConverterConfig(mode="coerce")
    converter = PySparkConverter(config=config)
    schema = converter.convert(spec)

    print("\n--- Generated Schema ---")
    print(schema.simpleString())
    print("--- End Schema ---\n")

    print("▶ Creating SparkSession...")
    spark = (
        SparkSession.builder.appName("YadsSchemaTest").master("local[*]").getOrCreate()
    )

    print("▶ Creating DataFrame with generated schema...")
    try:
        df = spark.createDataFrame([], schema)
        print("✓ DataFrame created successfully!")
        print(f"  Schema columns: {df.columns}")
    except Exception as e:
        print(f"✗ DataFrame creation failed: {e}")
        raise
    finally:
        spark.stop()

    print("\n" + "=" * 60)
    print("✓ PySpark converter integration test PASSED")
    print("=" * 60)


if __name__ == "__main__":
    exit_code = 0

    try:
        test_spark_sql_converter()
    except Exception as e:
        print(f"\n✗ Spark SQL test FAILED: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        exit_code = 1

    try:
        test_pyspark_converter()
    except Exception as e:
        print(f"\n✗ PySpark converter test FAILED: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        exit_code = 1

    sys.exit(exit_code)
