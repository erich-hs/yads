import pytest
from yads.converters.sql import SparkSQLConverter
from yads.exceptions import ValidationRuleError
from yads.loader import from_string
from yads.spec import SchemaSpec


@pytest.fixture
def spec_with_fixed_length_string() -> SchemaSpec:
    """Returns a spec with a fixed-length string column."""
    yaml_string = """
    name: my_db.my_table
    version: 1
    columns:
      - name: col1
        type: string
        params:
          length: 10
    """
    return from_string(yaml_string)


class TestSparkSQLConverter:
    def test_convert_raise_mode_raises_error(
        self, spec_with_fixed_length_string: SchemaSpec
    ):
        """
        Tests that the SparkSQLConverter raises a ValueError in 'raise' mode
        when encountering a fixed-length string.
        """
        converter = SparkSQLConverter()
        with pytest.raises(
            ValidationRuleError, match="Fixed-length strings are not supported"
        ):
            converter.convert(spec_with_fixed_length_string, mode="raise")

    def test_convert_warn_mode_removes_length(
        self, spec_with_fixed_length_string: SchemaSpec
    ):
        """
        Tests that the SparkSQLConverter removes the length from a fixed-length
        string in 'warn' mode.
        """
        converter = SparkSQLConverter(pretty=True)
        with pytest.warns(UserWarning, match="Fixed-length strings are not supported"):
            generated_ddl = converter.convert(
                spec_with_fixed_length_string, mode="warn"
            )
        expected_ddl = """CREATE TABLE my_db.my_table (
  col1 STRING
)"""
        assert generated_ddl.strip() == expected_ddl

    def test_convert_ignore_mode_does_nothing(
        self, spec_with_fixed_length_string: SchemaSpec
    ):
        """
        Tests that the SparkSQLConverter keeps the length of a fixed-length string
        in 'ignore' mode and does nothing.
        """
        converter = SparkSQLConverter(pretty=True)
        generated_ddl = converter.convert(spec_with_fixed_length_string, mode="ignore")
        expected_ddl = """CREATE TABLE my_db.my_table (
  col1 VARCHAR(10)
)"""
        assert generated_ddl.strip() == expected_ddl
