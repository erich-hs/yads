import pytest
import yaml
from pathlib import Path

from yads.constraints import NotNullConstraint, PrimaryKeyTableConstraint
from yads.loader import from_dict, from_string, from_yaml
from yads.spec import SchemaSpec
from yads.constraints import DefaultConstraint, ForeignKeyTableConstraint
from yads.types import (
    String,
    Integer,
    Float,
    Boolean,
    Decimal,
    Date,
    Timestamp,
    Binary,
    UUID,
    Interval,
    IntervalTimeUnit,
    Array,
    Struct,
    Map,
)

# Define paths to the fixture directories
VALID_SPEC_DIR = Path(__file__).parent / "fixtures" / "spec" / "valid"
INVALID_SPEC_DIR = Path(__file__).parent / "fixtures" / "spec" / "invalid"

# Get all valid spec files
valid_spec_files = list(VALID_SPEC_DIR.glob("*.yaml"))


@pytest.fixture(params=valid_spec_files, ids=[f.name for f in valid_spec_files])
def valid_spec_path(request):
    return request.param


@pytest.fixture
def valid_spec_content(valid_spec_path):
    return valid_spec_path.read_text()


@pytest.fixture
def valid_spec_dict(valid_spec_content):
    return yaml.safe_load(valid_spec_content)


class TestFromDict:
    def test_with_valid_spec(self, valid_spec_dict):
        spec = from_dict(valid_spec_dict)
        assert isinstance(spec, SchemaSpec)
        assert spec.name == valid_spec_dict["name"]


class TestFromString:
    def test_with_valid_spec(self, valid_spec_content, valid_spec_dict):
        spec = from_string(valid_spec_content)
        assert isinstance(spec, SchemaSpec)
        assert spec.name == valid_spec_dict["name"]


class TestFromYaml:
    def test_with_valid_spec(self, valid_spec_path, valid_spec_dict):
        spec = from_yaml(str(valid_spec_path))
        assert isinstance(spec, SchemaSpec)
        assert spec.name == valid_spec_dict["name"]


class TestFullSpec:
    """Tests the loading of a comprehensive spec with all features."""

    @pytest.fixture(scope="class")
    def spec(self) -> SchemaSpec:
        """Fixture to load the full_spec.yaml file."""
        return from_yaml(str(VALID_SPEC_DIR / "full_spec.yaml"))

    def test_top_level_attributes(self, spec: SchemaSpec):
        assert spec.name == "catalog.db.full_schema"
        assert spec.version == "2.1.0"
        assert spec.description == "A full schema with all features."
        assert spec.metadata == {"owner": "data-team", "sensitive": False}
        assert spec.external is True

    def test_storage_attributes(self, spec: SchemaSpec):
        assert spec.storage is not None
        assert spec.storage.location == "/data/full.schema"
        assert spec.storage.format == "parquet"
        assert spec.storage.tbl_properties == {"write_compression": "snappy"}

    def test_partitioning(self, spec: SchemaSpec):
        assert len(spec.partitioned_by) == 2
        assert spec.partitioned_by[0].column == "c_string_len"
        assert spec.partitioned_by[1].column == "c_date"
        assert spec.partitioned_by[1].transform == "month"

    def test_columns(self, spec: SchemaSpec):
        assert len(spec.columns) == 20

    def test_column_constraints(self, spec: SchemaSpec):
        # Test not_null constraint
        c_with_not_null = {
            c.name
            for c in spec.columns
            if any(isinstance(cons, NotNullConstraint) for cons in c.constraints)
        }
        assert len(c_with_not_null) == 2
        assert "c_uuid" in c_with_not_null
        assert "c_date" in c_with_not_null

        # Test default constraint
        c_with_default = {
            c.name: next(
                (cons for cons in c.constraints if isinstance(cons, DefaultConstraint)),
                None,
            )
            for c in spec.columns
            if any(isinstance(cons, DefaultConstraint) for cons in c.constraints)
        }
        assert len(c_with_default) == 1
        assert "c_string" in c_with_default
        assert c_with_default["c_string"] is not None
        assert c_with_default["c_string"].value == "default_string"

    def test_table_constraints(self, spec: SchemaSpec):
        assert len(spec.table_constraints) == 2

        pk_constraint = next(
            (
                c
                for c in spec.table_constraints
                if isinstance(c, PrimaryKeyTableConstraint)
            ),
            None,
        )
        assert pk_constraint is not None
        assert pk_constraint.name == "pk_full_schema"
        assert pk_constraint.columns == ["c_uuid", "c_date"]

        fk_constraint = next(
            (
                c
                for c in spec.table_constraints
                if isinstance(c, ForeignKeyTableConstraint)
            ),
            None,
        )
        assert fk_constraint is not None
        assert fk_constraint.name == "fk_other_table"
        assert fk_constraint.columns == ["c_int64"]
        assert fk_constraint.references.table == "other_table"
        assert fk_constraint.references.columns == ["id"]

    def test_get_column(self, spec: SchemaSpec):
        column = next((c for c in spec.columns if c.name == "c_uuid"), None)
        assert column is not None
        assert column.name == "c_uuid"
        assert str(column.type) == "uuid"
        assert column.description == "Primary key part 1"
        assert not column.metadata


@pytest.mark.parametrize(
    "spec_path, error_msg",
    [
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_name.yaml",
            "'name' is a required field",
        ),
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_version.yaml",
            "'version' is a required field",
        ),
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_columns.yaml",
            "'columns' is a required field",
        ),
        (
            INVALID_SPEC_DIR / "missing_column_field" / "missing_name.yaml",
            "'name' is a required field in a column definition",
        ),
        (
            INVALID_SPEC_DIR / "missing_column_field" / "missing_type.yaml",
            "'type' is a required field in a column definition",
        ),
        (INVALID_SPEC_DIR / "unknown_type.yaml", "Unknown type: 'invalid_type'"),
        (
            INVALID_SPEC_DIR / "invalid_type_def.yaml",
            "The 'type' of a field must be a string",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "array_missing_element.yaml",
            "Array type definition must include 'element'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "struct_missing_fields.yaml",
            "Struct type definition must include 'fields'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "map_missing_key.yaml",
            "Map type definition must include 'key' and 'value'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "map_missing_value.yaml",
            "Map type definition must include 'key' and 'value'",
        ),
        (
            INVALID_SPEC_DIR / "unknown_constraint.yaml",
            "Unknown column constraint: invalid_constraint",
        ),
        (
            INVALID_SPEC_DIR / "generated_as_undefined_column.yaml",
            "Source column 'non_existent_col' for generated column 'col2' not found in schema.",
        ),
        (
            INVALID_SPEC_DIR / "partitioned_by_undefined_column.yaml",
            "Partition column 'non_existent_col' must be defined as a column in the schema.",
        ),
        (
            INVALID_SPEC_DIR / "identity_with_increment_zero.yaml",
            "Identity 'increment' cannot be zero.",
        ),
        (
            INVALID_SPEC_DIR / "invalid_interval" / "missing_start.yaml",
            "Interval type definition must include 'interval_start'",
        ),
    ],
)
def test_from_string_with_invalid_spec_raises_error(spec_path, error_msg):
    with open(spec_path) as f:
        content = f.read()
    with pytest.raises(ValueError, match=error_msg):
        from_string(content)


def test_invalid_yaml_content_raises_error():
    """Test that non-dictionary YAML content raises a ``TypeError``."""
    content = "- item1\n- item2"  # A list, not a dictionary
    with pytest.raises(
        TypeError, match="Loaded YAML content did not parse to a dictionary"
    ):
        from_string(content)


class TestTypeLoading:
    """Tests that the loader correctly parses YAML type definitions into Type objects."""

    def _create_minimal_spec_with_type(self, type_def: dict) -> dict:
        """Helper to create a minimal spec with a single column of the given type."""
        return {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [{"name": "test_column", **type_def}],
        }

    @pytest.mark.parametrize(
        "type_def, expected_type, expected_str",
        [
            # String types
            ({"type": "string"}, String(), "string"),
            ({"type": "text"}, String(), "string"),
            ({"type": "varchar"}, String(), "string"),
            ({"type": "char"}, String(), "string"),
            (
                {"type": "string", "params": {"length": 255}},
                String(length=255),
                "string(255)",
            ),
            # Integer types
            ({"type": "integer"}, Integer(bits=32), "integer(bits=32)"),
            ({"type": "int8"}, Integer(bits=8), "integer(bits=8)"),
            ({"type": "tinyint"}, Integer(bits=8), "integer(bits=8)"),
            ({"type": "byte"}, Integer(bits=8), "integer(bits=8)"),
            ({"type": "int16"}, Integer(bits=16), "integer(bits=16)"),
            ({"type": "smallint"}, Integer(bits=16), "integer(bits=16)"),
            ({"type": "short"}, Integer(bits=16), "integer(bits=16)"),
            ({"type": "int32"}, Integer(bits=32), "integer(bits=32)"),
            ({"type": "int"}, Integer(bits=32), "integer(bits=32)"),
            ({"type": "int64"}, Integer(bits=64), "integer(bits=64)"),
            ({"type": "bigint"}, Integer(bits=64), "integer(bits=64)"),
            ({"type": "long"}, Integer(bits=64), "integer(bits=64)"),
            # Float types
            ({"type": "float"}, Float(bits=32), "float(bits=32)"),
            ({"type": "float32"}, Float(bits=32), "float(bits=32)"),
            ({"type": "float64"}, Float(bits=64), "float(bits=64)"),
            ({"type": "double"}, Float(bits=64), "float(bits=64)"),
            # Boolean types
            ({"type": "boolean"}, Boolean(), "boolean"),
            ({"type": "bool"}, Boolean(), "boolean"),
            # Decimal types
            ({"type": "decimal"}, Decimal(), "decimal"),
            ({"type": "numeric"}, Decimal(), "decimal"),
            (
                {"type": "decimal", "params": {"precision": 10, "scale": 2}},
                Decimal(precision=10, scale=2),
                "decimal(10, 2)",
            ),
            # Temporal types
            ({"type": "date"}, Date(), "date"),
            ({"type": "timestamp"}, Timestamp(), "timestamp"),
            ({"type": "datetime"}, Timestamp(), "timestamp"),
            # ({"type": "timestamp_tz"}, TimestampTZ(), "timestamptz"),  # Not supported yet
            # Binary types
            ({"type": "binary"}, Binary(), "binary"),
            ({"type": "blob"}, Binary(), "binary"),
            ({"type": "bytes"}, Binary(), "binary"),
            # Other types
            ({"type": "uuid"}, UUID(), "uuid"),
            # ({"type": "json"}, JSON(), "json"),  # Not supported yet
            # Interval types
            (
                {"type": "interval", "params": {"interval_start": "YEAR"}},
                Interval(interval_start=IntervalTimeUnit.YEAR),
                "interval(YEAR)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "MONTH"}},
                Interval(interval_start=IntervalTimeUnit.MONTH),
                "interval(MONTH)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "DAY"}},
                Interval(interval_start=IntervalTimeUnit.DAY),
                "interval(DAY)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "HOUR"}},
                Interval(interval_start=IntervalTimeUnit.HOUR),
                "interval(HOUR)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "MINUTE"}},
                Interval(interval_start=IntervalTimeUnit.MINUTE),
                "interval(MINUTE)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "SECOND"}},
                Interval(interval_start=IntervalTimeUnit.SECOND),
                "interval(SECOND)",
            ),
            (
                {
                    "type": "interval",
                    "params": {"interval_start": "YEAR", "interval_end": "MONTH"},
                },
                Interval(
                    interval_start=IntervalTimeUnit.YEAR,
                    interval_end=IntervalTimeUnit.MONTH,
                ),
                "interval(YEAR to MONTH)",
            ),
            (
                {
                    "type": "interval",
                    "params": {"interval_start": "DAY", "interval_end": "SECOND"},
                },
                Interval(
                    interval_start=IntervalTimeUnit.DAY,
                    interval_end=IntervalTimeUnit.SECOND,
                ),
                "interval(DAY to SECOND)",
            ),
        ],
    )
    def test_simple_type_loading(self, type_def, expected_type, expected_str):
        """Test loading of simple (non-complex) types from YAML."""
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert column.type == expected_type
        assert str(column.type) == expected_str

    @pytest.mark.parametrize(
        "type_def, expected_element_type",
        [
            # Array types
            ({"type": "array", "element": {"type": "string"}}, String()),
            ({"type": "list", "element": {"type": "int"}}, Integer(bits=32)),
            (
                {
                    "type": "array",
                    "element": {
                        "type": "decimal",
                        "params": {"precision": 10, "scale": 2},
                    },
                },
                Decimal(precision=10, scale=2),
            ),
            # Nested arrays
            (
                {
                    "type": "array",
                    "element": {"type": "array", "element": {"type": "boolean"}},
                },
                Array(element=Boolean()),
            ),
        ],
    )
    def test_array_type_loading(self, type_def, expected_element_type):
        """Test loading of array types from YAML."""
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert isinstance(column.type, Array)
        assert column.type.element == expected_element_type

    @pytest.mark.parametrize(
        "type_def, expected_key_type, expected_value_type",
        [
            # Map types
            (
                {"type": "map", "key": {"type": "string"}, "value": {"type": "int"}},
                String(),
                Integer(bits=32),
            ),
            (
                {
                    "type": "dictionary",
                    "key": {"type": "uuid"},
                    "value": {"type": "double"},
                },
                UUID(),
                Float(bits=64),
            ),
            (
                {
                    "type": "map",
                    "key": {"type": "int"},
                    "value": {"type": "array", "element": {"type": "string"}},
                },
                Integer(bits=32),
                Array(element=String()),
            ),
        ],
    )
    def test_map_type_loading(self, type_def, expected_key_type, expected_value_type):
        """Test loading of map types from YAML."""
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert isinstance(column.type, Map)
        assert column.type.key == expected_key_type
        assert column.type.value == expected_value_type

    def test_struct_type_loading(self):
        """Test loading of struct types from YAML."""
        type_def = {
            "type": "struct",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"},
                {"name": "field3", "type": "boolean"},
            ],
        }
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert isinstance(column.type, Struct)
        assert len(column.type.fields) == 3

        # Check individual fields
        field1, field2, field3 = column.type.fields
        assert field1.name == "field1"
        assert field1.type == String()
        assert field2.name == "field2"
        assert field2.type == Integer(bits=32)
        assert field3.name == "field3"
        assert field3.type == Boolean()

    def test_nested_struct_type_loading(self):
        """Test loading of nested struct types from YAML."""
        type_def = {
            "type": "struct",
            "fields": [
                {"name": "simple_field", "type": "string"},
                {
                    "name": "nested_struct",
                    "type": "struct",
                    "fields": [{"name": "inner_field", "type": "int"}],
                },
            ],
        }
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert isinstance(column.type, Struct)
        assert len(column.type.fields) == 2

        simple_field, nested_field = column.type.fields
        assert simple_field.name == "simple_field"
        assert simple_field.type == String()

        assert nested_field.name == "nested_struct"
        assert isinstance(nested_field.type, Struct)
        assert len(nested_field.type.fields) == 1
        assert nested_field.type.fields[0].name == "inner_field"
        assert nested_field.type.fields[0].type == Integer(bits=32)
