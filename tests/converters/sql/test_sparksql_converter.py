import pytest
from yads.converters.sql import SparkSQLConverter
from yads.exceptions import AstValidationError
from yads.loaders import from_string
from yads.spec import YadsSpec


@pytest.fixture
def spec_with_json_type() -> YadsSpec:
    """Returns a spec with a JSON column."""
    yaml_string = """
    name: my_db.my_table
    version: 1
    columns:
      - name: col1
        type: json
    """
    return from_string(yaml_string)


class TestSparkSQLConverter:
    def test_convert_raise_mode_raises_error(self, spec_with_json_type: YadsSpec):
        """
        Tests that the SparkSQLConverter raises a ValueError in 'raise' mode
        when encountering a JSON column.
        """
        converter = SparkSQLConverter()
        with pytest.raises(
            AstValidationError,
            match="Data type 'JSON' is not supported for column 'col1'.",
        ):
            converter.convert(spec_with_json_type, mode="raise")

    def test_convert_warn_mode_updates_type(self, spec_with_json_type: YadsSpec):
        """
        Tests that the SparkSQLConverter updates the type of a JSON column to
        STRING in 'warn' mode.
        """
        converter = SparkSQLConverter(pretty=True)
        with pytest.warns(
            UserWarning, match="Data type 'JSON' is not supported for column 'col1'."
        ):
            generated_ddl = converter.convert(spec_with_json_type, mode="warn")
        expected_ddl = """CREATE TABLE my_db.my_table (
  col1 STRING
)"""
        assert generated_ddl.strip() == expected_ddl

    def test_convert_ignore_mode_does_nothing(self, spec_with_json_type: YadsSpec):
        """
        Tests that the SparkSQLConverter keeps the JSON column in 'ignore' mode
        and does nothing.
        """
        converter = SparkSQLConverter(pretty=True)
        generated_ddl = converter.convert(spec_with_json_type, mode="ignore")
        expected_ddl = """CREATE TABLE my_db.my_table (
  col1 JSON
)"""
        assert generated_ddl.strip() == expected_ddl
