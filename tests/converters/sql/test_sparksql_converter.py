import pytest

from yads.converters.sql import SparkSQLConverter
from yads.exceptions import AstValidationError
from yads.loaders import from_yaml_string
from yads.spec import YadsSpec


# ==========================================================
# SparkSQLConverter tests
# Scope: mode behavior for unsupported JSON → raise/warn/ignore
# ==========================================================


# %% Fixtures
@pytest.fixture
def spec_with_json_type() -> YadsSpec:
    yaml_string = """
    name: my_db.my_table
    version: 1
    columns:
      - name: col1
        type: json
    """
    return from_yaml_string(yaml_string)


# %% Mode behavior
class TestSparkSQLConverter:
    def test_convert_raise_mode_raises_error(self, spec_with_json_type: YadsSpec):
        converter = SparkSQLConverter()
        with pytest.raises(
            AstValidationError,
            match="Data type 'JSON' is not supported for column 'col1'.",
        ):
            converter.convert(spec_with_json_type, mode="raise")

    def test_convert_warn_mode_updates_type(self, spec_with_json_type: YadsSpec):
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
        converter = SparkSQLConverter(pretty=True)
        generated_ddl = converter.convert(spec_with_json_type, mode="ignore")
        expected_ddl = """CREATE TABLE my_db.my_table (
  col1 JSON
)"""
        assert generated_ddl.strip() == expected_ddl
