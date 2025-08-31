import pytest
from sqlglot.expressions import DataType
from yads.converters.sql import SQLConverter, AstValidator, DisallowType
from yads.converters import SQLConverterConfig
from yads.exceptions import AstValidationError
from yads.loaders import from_yaml_string


class TestSQLConverterModeHierarchy:
    def test_converter_instance_mode_raise_raises(self):
        yaml_string = """
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: geography
        """
        spec = from_yaml_string(yaml_string)

        validator = AstValidator(
            rules=[DisallowType(disallow_type=DataType.Type.GEOGRAPHY)]
        )
        config = SQLConverterConfig(
            dialect="spark", ast_validator=validator, mode="raise"
        )
        converter = SQLConverter(config)

        with pytest.raises(
            AstValidationError,
            match="Data type 'GEOGRAPHY' is not supported for column 'col1'.",
        ):
            converter.convert(spec)

    def test_converter_instance_mode_coerce_warns(self):
        yaml_string = """
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: geography
        """
        spec = from_yaml_string(yaml_string)

        validator = AstValidator(
            rules=[DisallowType(disallow_type=DataType.Type.GEOGRAPHY)]
        )
        config = SQLConverterConfig(
            dialect="spark", ast_validator=validator, mode="coerce"
        )
        converter = SQLConverter(config)

        with pytest.warns(
            UserWarning,
            match="Data type 'GEOGRAPHY' is not supported for column 'col1'.",
        ):
            converter.convert(spec)

    def test_call_override_to_coerce_does_not_persist(self):
        yaml_string = """
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: geography
        """
        spec = from_yaml_string(yaml_string)

        validator = AstValidator(
            rules=[DisallowType(disallow_type=DataType.Type.GEOGRAPHY)]
        )
        config = SQLConverterConfig(
            dialect="spark", ast_validator=validator, mode="raise"
        )
        converter = SQLConverter(config)

        with pytest.warns(
            UserWarning,
            match="Data type 'GEOGRAPHY' is not supported for column 'col1'.",
        ):
            ddl = converter.convert(spec, mode="coerce")
        assert "CREATE TABLE" in ddl

        # Instance mode should remain 'raise'
        with pytest.raises(
            AstValidationError,
            match="Data type 'GEOGRAPHY' is not supported for column 'col1'.",
        ):
            converter.convert(spec)

    def test_call_override_to_raise_does_not_persist(self):
        yaml_string = """
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: geography
        """
        spec = from_yaml_string(yaml_string)

        validator = AstValidator(
            rules=[DisallowType(disallow_type=DataType.Type.GEOGRAPHY)]
        )
        config = SQLConverterConfig(
            dialect="spark", ast_validator=validator, mode="coerce"
        )
        converter = SQLConverter(config)

        # Default coerce -> warns, not raises
        with pytest.warns(
            UserWarning,
            match="Data type 'GEOGRAPHY' is not supported for column 'col1'.",
        ):
            ddl1 = converter.convert(spec)
        assert "CREATE TABLE" in ddl1

        # Override to raise
        with pytest.raises(
            AstValidationError,
            match="Data type 'GEOGRAPHY' is not supported for column 'col1'.",
        ):
            converter.convert(spec, mode="raise")

        # Back to default coerce -> warns again
        with pytest.warns(
            UserWarning,
            match="Data type 'GEOGRAPHY' is not supported for column 'col1'.",
        ):
            ddl2 = converter.convert(spec)
        assert "CREATE TABLE" in ddl2
