import pytest
import yaml
from pathlib import Path

from yads.constraints import NotNullConstraint, PrimaryKeyTableConstraint
from yads.loader import from_dict, from_string, from_yaml
from yads.spec import SchemaSpec
from yads.types import Decimal

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

    @pytest.fixture
    def spec(self) -> SchemaSpec:
        """Fixture to load the full_spec.yaml file."""
        spec_path = VALID_SPEC_DIR / "full_spec.yaml"
        return from_yaml(str(spec_path))

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
        assert spec.partitioned_by[0].column == "user_id"
        assert spec.partitioned_by[0].transform is None
        assert spec.partitioned_by[1].column == "score"
        assert spec.partitioned_by[1].transform == "identity"

    def test_columns(self, spec: SchemaSpec):
        assert len(spec.columns) == 2
        user_id_col, score_col = spec.columns

        assert user_id_col.name == "user_id"
        assert str(user_id_col.type) == "uuid"
        assert user_id_col.description == "User identifier"
        assert any(isinstance(c, NotNullConstraint) for c in user_id_col.constraints)
        assert user_id_col.metadata == {"pii": True}

        assert score_col.name == "score"
        assert isinstance(score_col.type, Decimal)
        assert score_col.type.precision == 5
        assert score_col.type.scale == 2

    def test_table_constraints(self, spec: SchemaSpec):
        assert len(spec.table_constraints) == 1
        pk = spec.table_constraints[0]
        assert isinstance(pk, PrimaryKeyTableConstraint)
        assert pk.name == "pk_full_schema"
        assert pk.columns == ["user_id"]


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
            "The 'increment' for an identity constraint cannot be 0",
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
