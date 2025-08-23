import pytest
import warnings

from yads.converters.sql import DuckdbSQLConverter
from yads.exceptions import AstValidationError
from yads.loaders import from_yaml_string, from_yaml_path
from yads.converters.sql.validators.ast_validator import ValidationWarning


# ==========================================================
# DuckdbSQLConverter tests
# Scope: verifies DuckDB dialect and built-in validation rules
# ==========================================================


# %% Dialect behavior
class TestDuckdbSQLConverterDialect:
    def test_convert_full_spec_matches_duckdb_fixture(self):
        spec = from_yaml_path("tests/fixtures/spec/valid/full_spec.yaml")
        converter = DuckdbSQLConverter(pretty=True)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ddl = converter.convert(spec)

        assert len(w) == 8
        assert all(issubclass(wi.category, ValidationWarning) for wi in w)
        messages = "\n".join(str(wi.message) for wi in w)
        assert (
            "Data type 'TIMESTAMPLTZ' is not supported for column 'c_timestamp_ltz'."
            in messages
        )
        assert "Data type 'VOID' is not supported for column 'c_void'." in messages
        assert (
            "Data type 'GEOGRAPHY' is not supported for column 'c_geography'." in messages
        )
        assert (
            "Parameterized 'GEOMETRY' is not supported for column 'c_geometry'."
            in messages
        )
        assert "Data type 'VARIANT' is not supported for column 'c_variant'." in messages
        assert (
            "GENERATED ALWAYS AS IDENTITY is not supported for column 'c_int32_identity'."
            in messages
        )
        assert "NULLS FIRST is not supported in PRIMARY KEY constraints." in messages
        assert "The data type will be replaced with 'TIMESTAMPTZ'." in messages
        assert "The data type will be replaced with 'TEXT'." in messages
        assert "The parameters will be removed." in messages
        assert "The NULLS FIRST attribute will be removed." in messages

        with open("tests/fixtures/sql/duckdb/full_spec.sql", "r") as f:
            expected_sql = f.read().strip()

        assert ddl.strip() == expected_sql


# %% Validation rules wiring
class TestDuckdbSQLConverterValidation:
    @pytest.mark.parametrize(
        "yads_type, original_type_sql, expected_sql",
        [
            ("timestampltz", "TIMESTAMPLTZ", "TIMESTAMPTZ"),
            ("void", "VOID", "TEXT"),
            ("geography", "GEOGRAPHY", "TEXT"),
            ("variant", "VARIANT", "TEXT"),
        ],
    )
    def test_coerce_mode_replaces_to_duckdb_supported_and_warns(
        self, yads_type: str, original_type_sql: str, expected_sql: str
    ):
        yaml_string = f"""
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: {yads_type}
        """
        spec = from_yaml_string(yaml_string)

        converter = DuckdbSQLConverter(pretty=True)
        with pytest.warns(
            UserWarning,
            match=f"Data type '{original_type_sql}' is not supported for column 'col1'.",
        ):
            ddl = converter.convert(spec, mode="coerce")

        expected_ddl = f"""CREATE TABLE my_db.my_table (
  col1 {expected_sql}
)"""
        assert ddl.strip() == expected_ddl

    def test_coerce_mode_removes_geometry_parameters_and_warns(self):
        yaml_string = """
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: geometry
            params:
              srid: 4326
        """
        spec = from_yaml_string(yaml_string)

        converter = DuckdbSQLConverter(pretty=True)
        with pytest.warns(
            UserWarning,
            match="Parameterized 'GEOMETRY' is not supported for column 'col1'.",
        ):
            ddl = converter.convert(spec, mode="coerce")

        expected_ddl = """CREATE TABLE my_db.my_table (
  col1 GEOMETRY
)"""
        assert ddl.strip() == expected_ddl

    @pytest.mark.parametrize(
        "yads_type, original_type_sql",
        [
            ("timestampltz", "TIMESTAMPLTZ"),
            ("void", "VOID"),
            ("geography", "GEOGRAPHY"),
            ("variant", "VARIANT"),
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

        converter = DuckdbSQLConverter(pretty=True)
        with pytest.raises(
            AstValidationError,
            match=f"Data type '{original_type_sql}' is not supported for column 'col1'.",
        ):
            converter.convert(spec, mode="raise")

    def test_raise_mode_raises_for_parameterized_geometry(self):
        yaml_string = """
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: geometry
            params:
              srid: 4326
        """
        spec = from_yaml_string(yaml_string)

        converter = DuckdbSQLConverter(pretty=True)
        with pytest.raises(
            AstValidationError,
            match="Parameterized 'GEOMETRY' is not supported for column 'col1'.",
        ):
            converter.convert(spec, mode="raise")
