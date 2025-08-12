import pytest
import warnings

from yads.converters.sql import SparkSQLConverter
from yads.exceptions import AstValidationError
from yads.loaders import from_yaml_string, from_yaml_path
from yads.converters.sql.validators.ast_validator import ValidationWarning


# ==========================================================
# SparkSQLConverter tests
# Scope: verifies Spark dialect and built-in validation rules
# ==========================================================


# %% Dialect behavior
class TestSparkSQLConverterDialect:
    def test_convert_full_spec_matches_spark_fixture(self):
        spec = from_yaml_path("tests/fixtures/spec/valid/full_spec.yaml")
        converter = SparkSQLConverter(pretty=True)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ddl = converter.convert(spec)

        # Expect 3 warnings for JSON/GEOMETRY/GEOGRAPHY
        assert len(w) == 3
        assert all(issubclass(wi.category, ValidationWarning) for wi in w)
        messages = "\n".join(str(wi.message) for wi in w)
        assert "Data type 'JSON' is not supported for column 'c_json'." in messages
        assert (
            "Data type 'GEOMETRY' is not supported for column 'c_geometry'." in messages
        )
        assert (
            "Data type 'GEOGRAPHY' is not supported for column 'c_geography'." in messages
        )
        assert "The data type will be replaced with 'TEXT'." in messages

        with open("tests/fixtures/sql/spark/full_spec.sql", "r") as f:
            expected_sql = f.read().strip()

        assert ddl.strip() == expected_sql


# %% Validation rules wiring (JSON/GEOMETRY/GEOGRAPHY)
class TestSparkSQLConverterValidation:
    @pytest.mark.parametrize(
        "yads_type, original_type_sql",
        [
            ("json", "JSON"),
            ("geometry", "GEOMETRY"),
            ("geography", "GEOGRAPHY"),
        ],
    )
    def test_warn_mode_replaces_to_string_and_warns(
        self, yads_type: str, original_type_sql: str
    ):
        yaml_string = f"""
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: {yads_type}
        """
        spec = from_yaml_string(yaml_string)

        converter = SparkSQLConverter(pretty=True)
        with pytest.warns(
            UserWarning,
            match=f"Data type '{original_type_sql}' is not supported for column 'col1'.",
        ):
            ddl = converter.convert(spec, mode="warn")

        expected_ddl = """CREATE TABLE my_db.my_table (
  col1 STRING
)"""
        assert ddl.strip() == expected_ddl

    @pytest.mark.parametrize(
        "yads_type, original_type_sql",
        [
            ("json", "JSON"),
            ("geometry", "GEOMETRY"),
            ("geography", "GEOGRAPHY"),
        ],
    )
    def test_ignore_mode_keeps_original_type(
        self, yads_type: str, original_type_sql: str
    ):
        yaml_string = f"""
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: {yads_type}
        """
        spec = from_yaml_string(yaml_string)

        converter = SparkSQLConverter(pretty=True)
        ddl = converter.convert(spec, mode="ignore")

        expected_ddl = f"""CREATE TABLE my_db.my_table (
  col1 {original_type_sql}
)"""
        assert ddl.strip() == expected_ddl

    @pytest.mark.parametrize(
        "yads_type, original_type_sql",
        [
            ("json", "JSON"),
            ("geometry", "GEOMETRY"),
            ("geography", "GEOGRAPHY"),
        ],
    )
    def test_raise_mode_raises_ast_validation_error(
        self, yads_type: str, original_type_sql: str
    ):
        yaml_string = f"""
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: {yads_type}
        """
        spec = from_yaml_string(yaml_string)

        converter = SparkSQLConverter(pretty=True)
        with pytest.raises(
            AstValidationError,
            match=f"Data type '{original_type_sql}' is not supported for column 'col1'.",
        ):
            converter.convert(spec, mode="raise")
