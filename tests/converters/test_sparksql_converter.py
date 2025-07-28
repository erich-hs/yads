import pytest
from typing import Any
import warnings

from yads.converters.sql import SparkSQLConverter
from yads.loader import from_dict
from yads.spec import SchemaSpec

# (yads_type_in_spec, yads_params_in_spec, expected_spark_sql_type)
SPARK_TYPE_CASES = [
    ("tinyint", None, "TINYINT"),
    ("smallint", None, "SMALLINT"),
    ("integer", None, "INT"),
    ("bigint", None, "BIGINT"),
    ("long", None, "BIGINT"),
    ("float", None, "DOUBLE"),
    ("double", None, "DOUBLE"),
    ("decimal", {"precision": 10, "scale": 2}, "DECIMAL(10, 2)"),
    ("string", None, "STRING"),
    ("boolean", None, "BOOLEAN"),
    ("date", None, "DATE"),
    ("timestamp", None, "TIMESTAMP"),
    ("timestamp_tz", None, "TIMESTAMP"),  # Spark's TIMESTAMP is with local time zone
    ("integer", {"bits": 8}, "TINYINT"),
    ("tinyint", None, "TINYINT"),
    ("integer", {"bits": 16}, "SMALLINT"),
    ("smallint", None, "SMALLINT"),
    ("integer", {"bits": 32}, "INT"),
    ("int32", None, "INT"),
    ("integer", {"bits": 64}, "BIGINT"),
    ("bigint", None, "BIGINT"),
    ("long", None, "BIGINT"),
    ("float", {"bits": 32}, "FLOAT"),
    ("float32", None, "FLOAT"),
    ("float", {"bits": 64}, "DOUBLE"),
    ("double", None, "DOUBLE"),
    ("float64", None, "DOUBLE"),
    ("boolean", None, "BOOLEAN"),
    ("bool", None, "BOOLEAN"),
    ("binary", None, "BINARY"),
    ("array", {"element": {"type": "integer"}}, "ARRAY<INT>"),
    (
        "struct",
        {"fields": [{"name": "c1", "type": "integer"}]},
        "STRUCT<c1: INT>",
    ),
    (
        "map",
        {"key": {"type": "string"}, "value": {"type": "integer"}},
        "MAP<STRING, INT>",
    ),
]


@pytest.mark.parametrize("yads_type, yads_params, spark_type", SPARK_TYPE_CASES)
def test_spark_data_type_conversion(
    yads_type: str, yads_params: dict | None, spark_type: str
):
    """
    Tests the end-to-end conversion of a yads type to a Spark SQL DDL type.
    """
    column_def: dict[str, Any] = {"name": "my_col", "type": yads_type}
    if yads_params:
        if yads_type in ("array", "struct", "map"):
            column_def.update(yads_params)
        else:
            column_def["params"] = yads_params

    spec_dict = {
        "name": "test_db.test_table",
        "version": "1.0.0",
        "columns": [column_def],
    }

    spec = from_dict(spec_dict)

    converter = SparkSQLConverter(pretty=True)
    # Default mode is strict, these types should pass
    sql = converter.convert(spec)

    expected_sql = f"CREATE TABLE test_db.test_table (\n  my_col {spark_type}\n)"
    assert " ".join(sql.strip().split()) == " ".join(expected_sql.strip().split())


def _get_spec_with_fixed_length_string() -> SchemaSpec:
    """Helper function to create a spec with a fixed-length string column."""
    spec_dict = {
        "name": "test_db.test_table",
        "version": "1.0.0",
        "columns": [
            {"name": "my_col", "type": "string", "params": {"length": 50}},
        ],
    }
    return from_dict(spec_dict)


def test_fixed_length_string_strict_mode():
    """Tests that a fixed-length string raises a ValueError in strict mode."""
    spec = _get_spec_with_fixed_length_string()
    converter = SparkSQLConverter(pretty=True)
    with pytest.raises(ValueError) as excinfo:
        converter.convert(spec, mode="strict")
    assert "Fixed-length strings are not supported" in str(excinfo.value)


def test_fixed_length_string_warn_mode():
    """Tests that a fixed-length string issues a warning and converts in warn mode."""
    spec = _get_spec_with_fixed_length_string()
    converter = SparkSQLConverter(pretty=True)
    with pytest.warns(UserWarning) as record:
        sql = converter.convert(spec, mode="warn")

    # Check that a warning was issued
    assert len(record) == 1
    assert "Fixed-length strings are not supported" in str(record[0].message)
    assert "Set mode to 'fix' to automatically adjust the AST." in str(
        record[0].message
    )

    expected_sql = "CREATE TABLE test_db.test_table (\n  my_col VARCHAR(50)\n)"
    assert " ".join(sql.strip().split()) == " ".join(expected_sql.strip().split())


def test_fixed_length_string_fix_mode():
    """Tests that a fixed-length string converts without warning or adjustment in ignore mode."""
    spec = _get_spec_with_fixed_length_string()
    converter = SparkSQLConverter(pretty=True)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        sql = converter.convert(spec, mode="fix")
        assert len(w) == 1, "A warning should be issued in fix mode"
        assert "Fixed-length strings are not supported" in str(w[0].message)
        assert "The length parameter will be removed." in str(w[0].message)

    expected_sql = "CREATE TABLE test_db.test_table (\n  my_col STRING\n)"
    assert " ".join(sql.strip().split()) == " ".join(expected_sql.strip().split())
